#
# config.py
#
import os

# To be used to build images in coming changes
AVAILABLE_PY_RUNTIMES = {
    # If you have different base images for different Python versions, you can define them here.
    '3.7': 'iluvatar.default.py37',
    '3.8': 'iluvatar.default.py38',
    '3.9': 'iluvatar.default.py39',
    # etc.
}

# Figure out use of max_workers
DEFAULT_CONFIG_KEYS = {
    'runtime_timeout': 300,   
    'runtime_memory': 256,     
    'max_workers': 10,
    'docker_server': 'docker.io', 
    'worker_processes': 1,
    'invoke_pool_threads': 100,
}

DEFAULT_DOCKERFILE = """
WORKDIR /app
COPY requirements.txt reqs.txt

RUN apt-get update && apt-get install -y \
        zip \
        && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --upgrade pip && python3 -m pip install flask gunicorn && python3 -m pip install -r reqs.txt && python3 -m pip cache purge

# find alternatives to this, how to avoid copying server.py from Iluvatar
COPY server.py .
COPY *.json ./

COPY lithops_iluvatar.zip .
RUN unzip lithops_iluvatar.zip && rm lithops_iluvatar.zip

ENTRYPOINT [ "gunicorn", "-w", "1", "server:app" ]
"""

# At minimum, we need to know how to contact Iluvatar's worker, e.g. "localhost:8079"
REQ_PARAMS = ('worker_url',)
FH_ZIP_LOCATION = os.path.join(os.getcwd(), 'lithops_iluvatar.zip')

def load_config(config_data):
    """
    This function ensures the 'iluvatar' config section is present and sets default values.
    Example of config:

    lithops:
      backend: iluvatar
      storage: ...
      ...

    iluvatar:
      worker_url: "localhost:8079"
      runtime: "docker.io/myuser/lithops-iluvatar:latest"
      docker_user: "..."
      docker_password: "..."
      runtime_memory: 512
      runtime_timeout: 300
      max_workers: 50
      ...
    """
    if 'iluvatar' not in config_data:
        raise Exception("'iluvatar' section is mandatory in the Lithops configuration")

    iluv_cfg = config_data['iluvatar']

    # Check required params
    for param in REQ_PARAMS:
        if param not in iluv_cfg:
            raise Exception(f"'{param}' is mandatory under 'iluvatar' section of the configuration")

    # Set defaults if missing
    for key, val in DEFAULT_CONFIG_KEYS.items():
        iluv_cfg.setdefault(key, val)

    # Possibly set a default runtime image from AVAILABLE_PY_RUNTIMES if none specified
    if 'runtime' not in iluv_cfg:
        iluv_cfg['runtime'] = AVAILABLE_PY_RUNTIMES.get('3.8', 'iluvatar.default.py38')
