#
# iluvatar.py
#
import os
import json
import logging
import subprocess
import tempfile
import base64
from lithops import utils
from lithops.version import __version__
from lithops.constants import COMPUTE_CLI_MSG
from . import config
import pickle
import re
logger = logging.getLogger(__name__)


class IluvatarBackend:
    """
    Minimal "serverless" backend for Iluvatar that expects a single, generic Docker
    image with entry_point.py. All user code is pickled by Lithops and loaded at runtime.

    - 'deploy_runtime()' calls `iluvatar_worker_cli register` once if needed.
    - 'invoke()' calls `iluvatar_worker_cli invoke ...`.
    """

    def __init__(self, iluvatar_config, internal_storage):
        logger.info("Creating Iluvatar client (Lithops)")

        self.name = 'iluvatar'
        self.type = utils.BackendType.FAAS.value 
        self.il_config = iluvatar_config
        self.internal_storage = internal_storage
        self.is_lithops_worker = utils.is_lithops_worker()

        self.worker_url = self.il_config['worker_url']
        self.runtime = self.il_config.get('runtime', self._get_default_runtime_image_name())     # e.g. "docker.io/myuser/lithops-iluvatar:latest"
        self.runtime_memory = self.il_config['runtime_memory']
        self.runtime_timeout = self.il_config['runtime_timeout']
        self.max_workers = self.il_config['max_workers']
        self.function_version = self.il_config.get('function_version', '1')
        self.function_name = f"lithops_{self.runtime}_{self.runtime_memory}MB_iluvatar_action_{self.function_version}"
        self.docker_image_name = self.il_config.get('docker_image_name',  None)


        # TODO, during deploy build and push the image to the registy, use lithops lib from directory
        self.docker_user = self.il_config.get('docker_user')
        self.docker_password = self.il_config.get('docker_password')
        self.docker_server = self.il_config.get('docker_server', "docker.io")

        msg = COMPUTE_CLI_MSG.format('Iluvatar')
        logger.info(f"{msg} - Worker URL: {self.worker_url}")

    def is_function_registered(self):
        """
        Check if the function is already registered with Iluvatar.
        """
        logger.debug(f"Checking if function {self.function_name} is already registered.")
        pass

    def _build_default_runtime(self, runtime_name):
        """
        Build the default runtime image.
        """
        logger.debug(f"Building default runtime: {runtime_name}")
        dockerfile = "Dockerfile.default-iluvatar-runtime"
        with open(dockerfile, 'w') as f:
            f.write(f"FROM python:{utils.CURRENT_PY_VERSION}-slim-bullseye\n")
            f.write(config.DEFAULT_DOCKERFILE)
        try:
            self.build_runtime(runtime_name, dockerfile)
        finally:
            os.remove(dockerfile)

    def _format_image_name(self, runtime_name):
        """
        Formats Docker image name from runtime name
        """
        if 'docker.io' not in runtime_name:
            return f'docker.io/{self.docker_user}/{runtime_name}'
        else:
            return runtime_name

    def build_runtime(self, runtime_name, dockerfile=None, extra_args=[]):
        """
        Build the Docker image for the function.
        """
        logger.info(f"Building runtime: {runtime_name}")
        docker_path = utils.get_docker_path()
        image_name = self._format_image_name(runtime_name)

        if dockerfile:
            assert os.path.exists(dockerfile), f"Dockerfile not found: {dockerfile}"
            cmd = f'{docker_path} build --platform=linux/amd64 -t {image_name} -f {dockerfile} . '
        else:
            cmd = f'{docker_path} build --platform=linux/amd64 -t {image_name} . '
        cmd = cmd + ' '.join(extra_args)

        try:
            entry_point = os.path.join(os.path.dirname(__file__), 'entry_point.py')
            utils.create_handler_zip(config.FH_ZIP_LOCATION, entry_point, 'main.py')

            utils.run_command(cmd)
        finally:
            os.remove(config.FH_ZIP_LOCATION)

        logger.debug("Logging in to Docker registry")

        if self.docker_user and self.docker_password:
            cmd = f'{docker_path} login -u {self.docker_user} --password-stdin {self.docker_server}'
            utils.run_command(cmd, input=self.docker_password)
        
        logger.debug(f"Pushing image to Docker registry {image_name}")
        cmd = f'{docker_path} push {image_name}'
        utils.run_command(cmd)

        logger.debug("Building done. Image ready to be used.")
        import time
        time.sleep(3)

    def deploy_runtime(self, runtime_name, memory, timeout):
        """
        Registers the function image with Iluvatar if not done yet.
        Then returns runtime metadata.
        """
        #TO DO: add such functionality in iluvatar to list/check registered functions
        # if self.is_function_registered():
        #     logger.debug(f"Function {self.function_name} already registered.")
        #     return self._generate_runtime_meta(docker_image_name, memory)
        logger.debug(f"Deploying runtime: {runtime_name}")
        if self.docker_image_name == None:
            self._build_default_runtime(runtime_name)
        logger.debug("build ????")
        # format image name using the runtime_name
        docker_image_name = self._format_image_name(runtime_name)

        logger.info(f"Registering Iluvatar function: name={self.function_name}, version={self.function_version} "
                    f"image={runtime_name}, mem={memory}, timeout={timeout}")

        # TODO as of now using CLI to invoke/register, but parsing response is messy and error prone, 
        # one option is to  make and use RPC client 
        logger.debug("image name is " + docker_image_name)
        cli_cmd = [
            "./iluvatar_worker_cli",
            "--host", '127.0.0.1',
            "--port", '8031',
            "register",
            "--name", self.function_name,
            "--version", self.function_version,
            "--memory", str(memory),
            "--cpu", "1", 
            "--image", docker_image_name,
            "--isolation", "docker"        
        ]
        try:
            completed_proc = subprocess.run(cli_cmd, capture_output=True, text=True, check=True)
            logger.debug(f"Register output:\n{completed_proc.stdout.strip()}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to register function: {e.stderr}")
            raise e

        return self._generate_runtime_meta(runtime_name, memory)

    def delete_runtime(self, runtime_name, memory, version=__version__):
        """
        As of now, there is no 'unregister' command in Iluvatar.
        Once it is added, finish this method.
        """
        pass

    def _get_default_runtime_image_name(self):
        """
        Generates the default runtime image name
        """
        return utils.get_default_container_name(
            self.name, self.il_config, 'lithops-iluvatar-default'
        )

    def list_runtimes(self, docker_image_name='all'):
        """
        List registered runtimes. This is not yet implemented in Iluvatar.
        """
        pass

    def encode_iluvatar_body(self, payload: dict) -> str:
        """
        Encode payload for Iluvatar to a base64 string.
        """
        return base64.b64encode(pickle.dumps(payload)).decode('utf-8')
    
    def invoke(self, docker_image_name, runtime_memory, payload):
        """
        Actually invoke the function via `iluvatar_worker_cli invoke`.
        TODO: we want either iluvatar cli to accept json string or we need to make a RPC client
        encode the payload to a string and pass it to the CLI as an argument.
        """
        # logger.debug("Mocking invoking for now")
        # return "mock_activation_id"
        try:
            # Step 1: Prepare the payload
            encoded_payload = self.encode_iluvatar_body(payload)
            strpayload = str(encoded_payload)
            cli_cmd = [
                "./iluvatar_worker_cli",
                "--host", '127.0.0.1',
                "--port", '8031',
                "invoke",
                "--name", str(self.function_name),
                "--version", str(1),
                "-a",
                f'data={strpayload}'
            ]
            stdout_str = ""
            completed_proc = subprocess.run(cli_cmd, capture_output=True, text=True, check=True)
            stdout_str = completed_proc.stdout.strip()
            logger.debug(f"Invoke output:\n{stdout_str}")
            
            match = re.search(r'(\{.*\})', stdout_str, re.DOTALL)
            if not match:
                raise ValueError("No JSON object found in output!")

            json_str = match.group(1) 

            outer_obj = json.loads(json_str)

            activation_id = None
            if "json_result" in outer_obj:
                inner_obj = json.loads(outer_obj["json_result"])
                activation_id = inner_obj.get("activationId")
                logger.info("inner object is: " + str(inner_obj))

            logger.info(f"Activation ID: {activation_id}")
            return activation_id

        except subprocess.CalledProcessError as e:
            logger.error(f"Error invoking function: {e.stderr}")
            raise e

    def get_runtime_key(self, docker_image_name, runtime_memory, version=__version__):
        """
        Build a unique runtime key for internal caching.
        """
        name_part = f"{docker_image_name}_{runtime_memory}MB_{version}"
        runtime_key = os.path.join(self.name, version, self.worker_url, name_part)
        return runtime_key

    def get_runtime_info(self):
        """
        The Invoker calls this to know default memory, timeout, etc.
        """
        return {
            'runtime_name': self.runtime,
            'runtime_memory': self.runtime_memory,
            'function_name': self.function_name,
            'runtime_timeout': self.runtime_timeout,
            'max_workers': self.max_workers
        }

    def _generate_runtime_meta(self, runtime_name, memory):
        """
        TODO, check why this is needed, and what is the purpose of this method
        """
        logger.debug(f"Generating runtime metadata for {runtime_name}")
        try:
            runtime_meta = {
                'runtime_name': runtime_name,
                'runtime_memory': memory,
                'runtime_timeout': self.runtime_timeout,
                'lithops_version': __version__,
                'storage_config': self.internal_storage.storage.config,
                'python_version': utils.version_str(utils.sys.version_info),
                'preinstalls': []
            }
            return runtime_meta
        except Exception as e:
            raise Exception(f"Unable to get Iluvatar runtime metadata: {e}")
