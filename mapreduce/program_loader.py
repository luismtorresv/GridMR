import importlib.util
import inspect
import os
import tempfile
import urllib.request
from typing import Type
from urllib.parse import urlparse

from .framework import Mapper, Reducer


class ProgramLoader:
    """
    Handles loading of mapper and reducer programs from URLs.
    Supports file://, http://, https://, and nfs:// URLs.
    """

    def __init__(self, nfs_mount: str = "/mnt/gridmr"):
        self.nfs_mount = nfs_mount
        self.loaded_modules = {}  # Cache for loaded modules

    def load_program_from_url(self, program_url: str, program_type: str) -> Type:
        """
        Load a mapper or reducer class from a URL.

        Args:
            program_url: URL to the Python file containing the program
            program_type: "mapper" or "reducer"

        Returns:
            The loaded Mapper or Reducer class
        """
        print(f"🔗 Loading {program_type} from URL: {program_url}")

        # Check cache first
        cache_key = f"{program_url}_{program_type}"
        if cache_key in self.loaded_modules:
            print(f"   Using cached {program_type}")
            return self.loaded_modules[cache_key]

        # Download/locate the program file
        local_file_path = self._get_local_file_path(program_url)

        # Load the Python module
        module = self._load_python_module(local_file_path, program_url)

        # Find the appropriate class in the module
        program_class = self._find_program_class(module, program_type)

        # Cache the result
        self.loaded_modules[cache_key] = program_class

        print(f"✅ Successfully loaded {program_type} class: {program_class.__name__}")
        return program_class

    def _get_local_file_path(self, program_url: str) -> str:
        """Convert URL to local file path, downloading if necessary."""
        parsed = urlparse(program_url)

        if parsed.scheme == "file":
            # Local file URL
            return parsed.path

        elif parsed.scheme == "nfs":
            # NFS URL: nfs://shared/gridmr/programs/wordcount.py
            # Convert to local mount path
            nfs_path = parsed.path  # /shared/gridmr/programs/wordcount.py
            local_path = nfs_path.replace("/shared/gridmr", self.nfs_mount)
            return local_path

        elif parsed.scheme in ["http", "https"]:
            # Remote HTTP URL - download to temporary file
            return self._download_program(program_url)

        else:
            # Assume it's a local file path without scheme
            return program_url

    def _download_program(self, url: str) -> str:
        """Download program from HTTP URL to temporary file."""
        print(f"📥 Downloading program from: {url}")

        # Create temporary file
        with tempfile.NamedTemporaryFile(
            mode="w+b", suffix=".py", delete=False
        ) as tmp_file:
            # Download the file
            with urllib.request.urlopen(url) as response:
                tmp_file.write(response.read())

            print(f"   Downloaded to: {tmp_file.name}")
            return tmp_file.name

    def _load_python_module(self, file_path: str, original_url: str):
        """Load a Python module from file path."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(
                f"Program file not found: {file_path} (from URL: {original_url})"
            )

        # Generate a unique module name
        module_name = f"gridmr_program_{abs(hash(original_url))}"

        # Load the module
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Could not load module from: {file_path}")

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        return module

    def _find_program_class(self, module, program_type: str) -> Type:
        """Find the appropriate Mapper or Reducer class in the loaded module."""
        target_base_class = Mapper if program_type == "mapper" else Reducer

        # Look for classes that inherit from the target base class
        found_classes = []
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if (
                issubclass(obj, target_base_class)
                and obj != target_base_class  # Exclude the base class itself
                and obj.__module__ == module.__name__
            ):  # Only classes defined in this module
                found_classes.append(obj)

        if not found_classes:
            raise ImportError(
                f"No {target_base_class.__name__} class found in module. "
                f"Make sure your file contains a class that inherits from {target_base_class.__name__}."
            )

        if len(found_classes) > 1:
            print(
                f"⚠️  Multiple {target_base_class.__name__} classes found: {[c.__name__ for c in found_classes]}"
            )
            print(f"   Using the first one: {found_classes[0].__name__}")

        return found_classes[0]

    def cleanup(self):
        """Clean up any temporary files."""
        # In a full implementation, we'd track and clean up temporary files
        pass
