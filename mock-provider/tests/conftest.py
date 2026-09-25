import importlib
from types import ModuleType

import pytest

import app.main as main_module


@pytest.fixture
def provider_module() -> ModuleType:
    return importlib.reload(main_module)
