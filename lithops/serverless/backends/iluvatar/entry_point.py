import os
import sys
import logging
from lithops.version import __version__
from lithops.utils import setup_lithops_logger
from lithops.worker import function_handler, function_invoker
from lithops.worker.utils import get_runtime_metadata
from lithops.constants import LOGGER_FORMAT_SHORT
import ast
import uuid
logger = logging.getLogger('lithops.worker')
import json
import re
import traceback
import base64
import pickle


def decode_iluvatar_body(s: str) -> dict:
    """
    Decode the body of an Iluvatar request.
    """
    return pickle.loads(base64.b64decode(s.encode('utf-8')))


def main(args):
    """
    This is the Iluvatar function entry point.
    Iluvatar will call `python entry_point.py` with some JSON `args`.
    """
    try:
        # ger data from args
        data = args.get('data')
        # decode the data
        args_dict = decode_iluvatar_body(data)

        args_dict['data_byte_strs'] = [repr(element) for element in args_dict['data_byte_strs']]

        # check why this is needed
        os.environ['__LITHOPS_BACKEND'] = 'Iluvatar'

        # activation id to save results
        activation_id = uuid.uuid4().hex
        os.environ['__LITHOPS_ACTIVATION_ID'] = str(activation_id)

        # Configure logging
        log_level = args.get('log_level', 'INFO')
        setup_lithops_logger(log_level, LOGGER_FORMAT_SHORT, sys.stdout)

        # Normal invocation: run the user’s function
        logger.info(f"Lithops v{__version__} - Starting Iluvatar execution")
        function_handler(args_dict)

        return {"Execution": "Finished", "activationId":str(activation_id)}
    except (ValueError, SyntaxError) as e:
        return {"Error": "Could not convert args to dictionary", "error":str(e)}
    except Exception as e:
        exception_trace = traceback.format_exc()
        return {"Error": "dam error occurred", "error":str(exception_trace)}
