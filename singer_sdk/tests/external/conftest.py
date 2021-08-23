import json
from pathlib import Path
from typing import Optional

import pytest


@pytest.fixture
def gitlab_config() -> Optional[dict]:
    """Create a tap-gitlab config object."""

    config: Optional[dict] = None
    path = Path("singer_sdk/tests/external/.secrets/gitlab-config.json")

    if path.exists():
        config = json.loads(path.read_text())

    return config
