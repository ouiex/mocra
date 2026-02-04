import importlib
import inspect
import sys
import os
import logging
from typing import Dict, Type
from pathlib import Path

from common.interfaces.module import BaseModule
from engine.components.js_module import JSModule
from common.config import settings

logger = logging.getLogger(__name__)

class ModuleManager:
    def __init__(self, modules_dir: str = "modules"):
        self.modules_dir = modules_dir
        self._modules: Dict[str, BaseModule] = {}

    @property
    def modules(self) -> Dict[str, BaseModule]:
        return self._modules

    async def load_modules(self):
        """
        Dynamically load all modules from the modules directory.
        """
        base_path = Path(os.getcwd()) / self.modules_dir
        if not base_path.exists():
            logger.warning(f"Modules directory not found: {base_path}")
            return

        sys.path.append(str(base_path))

        # Load Python Modules
        for file_path in base_path.glob("*.py"):
            if file_path.name.startswith("__"):
                continue

            module_name = file_path.stem
            try:
                # Import the module
                spec = importlib.util.spec_from_file_location(module_name, file_path)
                if spec and spec.loader:
                    py_mod = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(py_mod)
                    
                    # Scan for BaseModule subclasses
                    for name, obj in inspect.getmembers(py_mod):
                        if (inspect.isclass(obj) and 
                            issubclass(obj, BaseModule) and 
                            obj is not BaseModule):
                            
                            instance = obj()
                            self._modules[instance.name] = instance
                            logger.info(f"Loaded module: {instance.name} (v{instance.version})")
            except Exception as e:
                logger.error(f"Failed to load module {module_name}: {e}")

        # Load JS Modules
        for file_path in base_path.glob("*.js"):
            try:
                js_mod = JSModule(str(file_path))
                await js_mod.load()
                if js_mod.name != "Unknown":
                    self._modules[js_mod.name] = js_mod
                    logger.info(f"Loaded JS module: {js_mod.name} (v{js_mod.version})")
            except Exception as e:
                logger.error(f"Failed to load JS module {file_path.name}: {e}")

    def get_module(self, module_name: str) -> BaseModule:
        return self._modules.get(module_name)

    def list_modules(self) -> list[str]:
        return list(self._modules.keys())
