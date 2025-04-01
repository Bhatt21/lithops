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
import shutil
import tempfile
import time
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
        self.iluvatar_gcp_credential_path = self.il_config.get('iluvatar_gcp_credential_path', None)


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
        Build the Docker image for the function using the ilubuild CLI.

        Steps:
        1. Create a temporary directory.
        2. Copy requirements.txt from the current working directory (where the script using Lithops is running)
            into the temporary directory.
        3. Copy entry_point.py (from os.path.dirname(__file__)) to a file named main.py in the temporary directory.
        4. Copy the credential JSON file (if provided) from self.iluvatar_credential_path into the temporary directory.
        5. Call the ilubuild CLI with the temporary directory as the function directory.
        """
        logger.info(f"Building runtime using ilubuild: {runtime_name}")
        
        image_name = self._format_image_name(runtime_name)
        
        # Create a temporary directory to serve as the function directory.
        with tempfile.TemporaryDirectory() as tmpdir:
            logger.debug(f"Created temporary directory: {tmpdir}")

            # Step 1: Copy requirements.txt from the current working directory, if it exists.
            req_src = os.path.join(os.getcwd(), "requirements.txt")
            req_dst = os.path.join(tmpdir, "requirements.txt")
            if os.path.exists(req_src):
                shutil.copy(req_src, req_dst)
                logger.debug(f"Copied requirements.txt from {req_src} to {req_dst}")
            else:
                logger.debug("No requirements.txt found in the current working directory.")

            # Step 2: Copy entry_point.py from the directory where this file resides to main.py in the temp dir.
            src_dir = os.path.dirname(__file__)
            entry_point_src = os.path.join(src_dir, "entry_point.py")
            main_py_dst = os.path.join(tmpdir, "main.py")
            if not os.path.exists(entry_point_src):
                raise FileNotFoundError(f"entry_point.py not found in {src_dir}")
            shutil.copy(entry_point_src, main_py_dst)
            logger.debug(f"Copied {entry_point_src} to {main_py_dst}")

            # Step 3: If iluvatar credential path is provided, copy that file to the temp dir.
            cred_path = self.il_config.get('iluvatar_credential_path', None)
            if cred_path:
                if not os.path.exists(cred_path):
                    raise FileNotFoundError(f"Credential file not found: {cred_path}")
                cred_dst = os.path.join(tmpdir, os.path.basename(cred_path))
                shutil.copy(cred_path, cred_dst)
                logger.debug(f"Copied credential file from {cred_path} to {cred_dst}")
            else:
                logger.debug("No iluvatar credential file provided in configuration.")

            # Step 4: Build the ilubuild CLI command.
            cmd = [
                "py2lambda",
                "--function-dir", tmpdir,
                "--runtime", "python",
                "--tag", image_name,
            ]
            
            # Append Docker registry credentials if provided.
            if self.docker_user:
                cmd.extend(["--docker-user", self.docker_user])
            if self.docker_password:
                cmd.extend(["--docker-pass", self.docker_password])
            
            # Append any extra arguments if provided.
            if extra_args:
                cmd.extend(extra_args)
            
            logger.info(f"Building image: {image_name}")
            logger.debug(f"Executing ilubuild command: {' '.join(cmd)}")
            # Execute the ilubuild command.
            utils.run_command(" ".join(cmd))
            logger.debug("Runtime build completed using ilubuild.")
            time.sleep(3)
            return image_name


    def deploy_runtime(self, runtime_name, memory, timeout):
        """
        Registers the function image with Iluvatar if not done yet.
        Then returns runtime metadata.
        """
        logger.debug(f"Deploying runtime: {runtime_name}")
        logger.info("Building Image")
        docker_image_name = self.build_runtime(runtime_name)

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
            "--isolation", "DOCKER"        
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
