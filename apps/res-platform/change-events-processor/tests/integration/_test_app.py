from pathlib import Path
from src import processor
import json
import pytest


TEST_DIR = Path(__file__).resolve().parent / ".."


def _test_platform_job_event():
    e = json.load(open(TEST_DIR / "resources" / "sample_job_update.json"))
    processor.handle_event(e)


def _test_platform_job_invalid_schema():
    with pytest.raises(Exception) as ex:
        processor.handle_event({"uh oh": 3})
