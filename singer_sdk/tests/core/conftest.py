import pytest

@pytest.fixture
def csv_config(outdir: str) -> dict:
    return {"target_folder": outdir}
