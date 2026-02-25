import importlib.util
import os

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))


def load_project_module(module_name, file_name):
    module_path = os.path.join(PROJECT_ROOT, file_name)
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Impossibile caricare il modulo {module_name} da {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
