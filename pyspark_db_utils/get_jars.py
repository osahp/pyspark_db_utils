from typing import List
import os

DEFAULT_JARS_PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'jars')


def get_jars() -> List[str]:
    """ get list of jars paths
        if os.environ variable JARS_PATH is set, find all in thise path
        elsewhere find all jars inside `jars` directory
    """
    jars_path = os.environ.get('JARS_PATH', DEFAULT_JARS_PATH)
    jars = [os.path.join(jars_path, jar)
            for jar in os.listdir(jars_path)]
    return jars
