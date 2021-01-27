import os
import logging
import requests
import time
from lithops.constants import COMPUTE_CLI_MSG
from lithops.util.ibm_token_manager import IBMTokenManager

from ibm_vpc import VpcV1
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
import namegenerator

logger = logging.getLogger(__name__)


class IBMVPCInstanceClient:

    def __init__(self, ibm_vpc_config, public_vsi=False):
        logger.debug("Creating IBM VPC client")
        self.name = 'ibm_vpc'
        self.config = ibm_vpc_config
        self.public_vsi = public_vsi

        self.endpoint = self.config['endpoint']
        self.region = self.endpoint.split('//')[1].split('.')[0]
        # optional, create VM will update new instance id
        self.instance_id = self.config.get('instance_id', None)
        # optional, create VM will update new virtual ip address
        self.ip_address = self.config.get('ip_address', None)
        self.instance_data = None

        self.vm_create_timeout = self.config.get('vm_create_timeout', 120)
        self.ssh_credentials = {'username': self.config.get('ssh_user', 'root'),
                                'password': self.config.get('ssh_password', None),
                                'key_filename': self.config.get('ssh_key_filename', None)}

        if public_vsi and self.ip_address:
            from lithops.util.ssh_client import SSHClient
            self.ssh_client = SSHClient(self.ip_address, self.ssh_credentials)

        iam_api_key = self.config.get('iam_api_key')
        self.custom_image = self.config.get('custom_lithops_image')

        authenticator = IAMAuthenticator(iam_api_key)
        self.service = VpcV1('2020-06-02', authenticator=authenticator)
        self.service.set_service_url(self.config['endpoint'] + '/v1')

        user_agent_string = 'ibm_vpc_' + ' {}'.format(self.config['user_agent'])
        logger.debug("Set user agent to {}".format(user_agent_string))
        self.service._set_user_agent_header(user_agent_string)

        msg = COMPUTE_CLI_MSG.format('IBM VPC')
        logger.info("{} - Region: {} - Host: {}".format(msg, self.region, self.ip_address))

    def get_ssh_credentials(self):
        return self.ssh_credentials

    def is_custom_image(self):
        return self.custom_image

    def get_ssh_client(self):
        return self.ssh_client

    def get_ip_address(self):
        return self.ip_address

    def get_instance_id(self):
        return self.instance_id

    def set_instance_id(self, instance_id):
        self.instance_id = instance_id

    def set_ip_address(self, ip_address):
        self.ip_address = ip_address

    def _generate_name(self, r_type, job_key, call_id):
        if job_key is not None and call_id is not None:
            red_call_id = call_id
            if red_call_id.find('/') > 0:
                red_call_id = red_call_id[red_call_id.find('/') + 1:]
            return "lithops-{}-{}-{}".format(job_key, red_call_id, r_type).replace('/', '-').replace(':', '-').replace('.', '').lower()
        return "lithops-{}-{}".format(namegenerator.gen(), r_type)

    def execution_wrapper(self, func, method, job_key=None, call_id=None, instance_id=None, ip_address=None):
        retry_attempt = 0
        while (int(retry_attempt) < 15):
            try:
                logger.debug("Execution {} for {} {} {} {}. Retry attempt {}".format(method, job_key, call_id, instance_id, ip_address, retry_attempt))
                response = func()
                return response
            except Exception as e:
                logger.debug("Execution {} for {} {} {} {} failed. Retry attempt {}".format(method, job_key, call_id, instance_id, ip_address, retry_attempt))
                logger.debug(e)
                retry_attempt = int(retry_attempt) + 1
                time.sleep(1)
                if int(retry_attempt) == 15:
                    raise e

    def _create_instance(self, job_key, call_id):
        logger.debug("__create_instance {} {} - start".format(job_key, call_id))

        security_group_identity_model = {'id': self.config['security_group_id']}
        subnet_identity_model = {'id': self.config['subnet_id']}
        primary_network_interface = {
            'name': 'eth0',
            'subnet': subnet_identity_model,
            'security_groups': [security_group_identity_model]
        }

        boot_volume_profile = {
            'capacity': 100,
            'name': self._generate_name('volume', job_key, call_id),
            'profile': {'name': self.config['volume_tier_name']}}

        boot_volume_attachment = {
            'delete_volume_on_instance_delete': True,
            'name': self._generate_name('boot', job_key, call_id),
            'volume': boot_volume_profile
        }

        key_identity_model = {'id': self.config['key_id']}
        instance_prototype_model = {
            'keys': [key_identity_model],
            'name': self._generate_name('instance', job_key, call_id)
        }

        instance_prototype_model['profile'] = {'name': self.config['profile_name']}
        instance_prototype_model['resource_group'] = {'id': self.config['resource_group_id']}
        instance_prototype_model['vpc'] = {'id': self.config['vpc_id']}
        instance_prototype_model['image'] = {'id': self.config['image_id']}
        instance_prototype_model['zone'] = {'name': self.config['zone_name']}
        instance_prototype_model['boot_volume_attachment'] = boot_volume_attachment
        instance_prototype_model['primary_network_interface'] = primary_network_interface

        logger.debug("Creating instance for {} {}".format(job_key, call_id))
        response = self.execution_wrapper(lambda: self.service.create_instance(instance_prototype_model),'creating instance', job_key = job_key, call_id = call_id)

        return response.result

    def _create_and_attach_floating_ip(self, instance, job_key, call_id):

        floating_ip = None

        try:
            floating_ip_list = self.service.list_floating_ips().get_result()
            for vip in floating_ip_list['floating_ips']:
                if vip['name'] == self.floating_ip_name:
                    floating_ip = vip
        except Exception as e:
            logger.warn('Failed to get the floating ip: {}'.format(str(e)))
            raise e

        try:
            if floating_ip is None:
                # allocate new floating ip
                floating_ip_prototype_model = {}
                floating_ip_prototype_model['name'] = self.floating_ip_name
                floating_ip_prototype_model['zone'] = {'name': self.config['zone_name']}
                floating_ip_prototype_model['resource_group'] = {'id': self.config['resource_group_id']}
                response = self.service.create_floating_ip(floating_ip_prototype_model)
                floating_ip = response.result
        except Exception as e:
            logger.warn('Failed to create floating ip: {}'.format(str(e)))
            raise e

        # we need to check if floating ip is not attached already. if not, attach it to instance
        primary_ni = instance['primary_network_interface']
        if ('target' in floating_ip and floating_ip['target']['primary_ipv4_address'] == primary_ni['primary_ipv4_address'] and
            floating_ip['target']['id'] == primary_ni['id']):
            # floating ip already atteched. do nothing
            logger.debug('Floating ip {} already attached to eth0 {}'.format(floating_ip['address'],floating_ip['target']['id']))
            return floating_ip['address']

        # attach floating ip
        try:
            response = self.service.add_instance_network_interface_floating_ip(
                instance['id'], instance['network_interfaces'][0]['id'], floating_ip['id'])
        except Exception as e:
            logger.warn('Failed to attach floating ip {} to {} : '.format(str(e)))
            raise e

        return floating_ip['address']

    def _wait_instance_running(self, instance_id):
        """
        Waits until the VM instance is running
        """
        logger.debug('Waiting VM instance {} {} to become running'.format(self.name, self.ip_address))

        start = time.time()
        while(time.time() - start < self.vm_create_timeout):
            instance = self.service.get_instance(instance_id).result
            if instance['status'] == 'running':
                return True
            time.sleep(1)

        self.stop()
        raise Exception('VM create failed, check logs and configurations')

    def _get_instance_id_and_status(self, name):
        #check if VSI exists and return it's id with status
        all_instances = self.service.list_instances().get_result()['instances']
        for instance in all_instances:
            if instance['name'] == name:
                logger.debug('{}  exists'.format(instance['name']))
                return instance['id'], instance['status']

    def _delete_instance(self):
        # delete floating ip
        response = self.service.list_instance_network_interfaces(self.instance_id)
        for nic in response.result['network_interfaces']:
            if 'floating_ips' in nic:
                for fip in nic['floating_ips']:
                    self.service.delete_floating_ip(fip['id'])
                    logger.debug("floating ip {} been deleted".format(fip['address']))

        # delete vm instance
        resp = self.service.delete_instance(self.instance_id)
        logger.debug("instance {} been deleted".format(self.instance_id))

    def start(self):
        logger.info("Starting VM instance id {} with IP {}".format(self.instance_id, self.ip_address))
        resp = self.execution_wrapper(lambda: self.service.create_instance_action(self.instance_id, 'start'),'start vm', instance_id = self.instance_id, ip_address = self.ip_address)

        logger.debug("VM instance {} started successfully".format(self.instance_id))

    def is_ready(self):
        resp = self.service.get_instance(self.instance_id).get_result()
        if resp['status'] == 'running':
            return True
        return False

    def create(self, job_key=None, call_id=None, check_if_vsi_exists=False):

        vsi_exists = False
        if self.instance_id:
            # user provided details of an already created instance
            vsi_exists = True

        if job_key and call_id:
            logger.debug("Creating VM instance {} {}".format(job_key, call_id))

        if check_if_vsi_exists:
            try:
                resp = self.service.list_instances()
                all_instances = resp.get_result()['instances']
            except Exception as e:
                logger.warn(e)
                raise e

            for instance in all_instances:
                if instance['name'] == self._generate_name('instance', job_key, call_id):
                    logger.debug('{} Already exists. Need to find floating ip attached'.format(instance['name']))
                    vsi_exists = True
                    self.instance_id = instance['id']

        if not vsi_exists:
            try:
                instance = self._create_instance(job_key, call_id)
                self.instance_id = instance['id']
                self.config['instance_id'] = self.instance_id
                logger.debug("VM {} created successfully ".format(instance['name']))
            except Exception as e:
                logger.error("There was an error trying to create the VM for {} {}".format(job_key, call_id))
                raise e

        try:
            if self.public_vsi:
                if not self.ip_address:
                    floating_ip = self._create_and_attach_floating_ip(instance, job_key, call_id)
                    logger.debug("VM {} updated successfully with floating IP {}".format(instance['name'], floating_ip))
                    self.config['ip_address'] = floating_ip
                    self.ip_address = floating_ip
                if not self.ssh_client:
                    from lithops.util.ssh_client import SSHClient
                    self.ssh_client = SSHClient(self.ip_address, self.ssh_credentials)
        except Exception as e:
            logger.error("There was an error trying to to bind floating ip to vm {}".format(self.instance_id))
            self._delete_instance()
            raise e

        return self.instance_id, self.ip_address

    def stop(self):
        if self.config['delete_on_dismantle']:
            logger.info("Deleting VM instance {}".format(self.ip_address))
            try:
                self._delete_instance()
                logger.debug("VM instance {} deleted successfully".format(self.ip_address))
            except Exception as e:
                logger.warn("VSI {} Delete error {}" .format(self.ip_address, e))
        else:
            logger.info("Stopping VM instance {}".format(self.ip_address))
            resp = self.execution_wrapper(lambda: self.service.create_instance_action(self.instance_id, 'stop'), 'stop vm', ip_address = self.ip_address)
            logger.debug("VM instance {} stopped successfully".format(self.instance_id))

            logger.debug("VM instance stopped successfully")

    def get_runtime_key(self, runtime_name):
        runtime_key = runtime_name.strip("/")

        return runtime_key
