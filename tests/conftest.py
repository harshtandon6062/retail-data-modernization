import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from common import get_spark
    # hive=False: tests use Spark's in-memory catalog, never the pipeline's metastore
    s = get_spark("tests", hive=False)
    yield s
    s.stop()
