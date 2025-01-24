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
        self.runtime_image = self.il_config['runtime']      # e.g. "docker.io/myuser/lithops-iluvatar:latest"
        self.runtime_memory = self.il_config['runtime_memory']
        self.runtime_timeout = self.il_config['runtime_timeout']
        self.max_workers = self.il_config['max_workers']
        self.function_version = self.il_config.get('function_version', '1')
        self.function_name = f"lithops_{self.runtime_memory}MB_iluvatar_action_{self.function_version}"


        # TODO, during deploy build and push the image to the registy, use lithops lib from directory
        self.docker_user = self.il_config.get('docker_user')
        self.docker_password = self.il_config.get('docker_password')

        msg = COMPUTE_CLI_MSG.format('Iluvatar')
        logger.info(f"{msg} - Worker URL: {self.worker_url}")

    def is_function_registered(self):
        """
        Check if the function is already registered with Iluvatar.
        """
        logger.debug(f"Checking if function {self.function_name} is already registered.")
        pass


    def build_runtime(self, docker_image_name, dockerfile=None, extra_args=[]):
        """
        Build the Docker image for the function.
        """
        logger.info(f"Building runtime: {docker_image_name}")
        pass

    def deploy_runtime(self, docker_image_name, memory, timeout):
        """
        Registers the function image with Iluvatar if not done yet.
        Then returns runtime metadata.
        """
        #TO DO: add such functionality in iluvatar to list/check registered functions
        # if self.is_function_registered():
        #     logger.debug(f"Function {self.function_name} already registered.")
        #     return self._generate_runtime_meta(docker_image_name, memory)
            

        logger.info(f"Registering Iluvatar function: name={self.function_name}, version={self.function_version} "
                    f"image={docker_image_name}, mem={memory}, timeout={timeout}")

        # TODO as of now using CLI to invoke/register, but parsing response is messy and error prone, 
        # one option is to  make and use RPC client 

        cli_cmd = [
            "./iluvatar_worker_cli",
            "--host", '127.0.0.1',
            "--port", '8031',
            "register",
            "--name", self.function_name,
            "--version", self.function_version,
            "--memory", str(memory),
            "--cpu", "3",     # if you want a custom CPU from config, add it
            "--image", docker_image_name,
            "--isolation", "docker"        
        ]

        try:
            completed_proc = subprocess.run(cli_cmd, capture_output=True, text=True, check=True)
            logger.debug(f"Register output:\n{completed_proc.stdout.strip()}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to register function: {e.stderr}")
            raise e

        return self._generate_runtime_meta(docker_image_name, memory)

    def delete_runtime(self, docker_image_name, memory, version=__version__):
        """
        As of now, there is no 'unregister' command in Iluvatar.
        Once it is added, finish this method.
        """
        pass


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
        tmp_path = None
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
            'runtime_name': self.runtime_image,
            'runtime_memory': self.runtime_memory,
            'function_name': self.function_name,
            'runtime_timeout': self.runtime_timeout,
        }

    def _generate_runtime_meta(self, docker_image_name, memory):
        """
        TODO, check why this is needed, and what is the purpose of this method
        """
        logger.debug(f"Generating runtime metadata for {docker_image_name}")
        try:
            runtime_meta = {
                'runtime_name': docker_image_name,
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
