import importlib.util
import sys
import types
from pathlib import Path


def load_streaming_module():
    pyspark_module = types.ModuleType("pyspark")
    pyspark_sql_module = types.ModuleType("pyspark.sql")
    pyspark_sql_module.SparkSession = object
    pyspark_sql_module.functions = types.SimpleNamespace()
    pyspark_sql_module.types = types.SimpleNamespace()
    pyspark_module.sql = pyspark_sql_module

    sys.modules.setdefault("pyspark", pyspark_module)
    sys.modules.setdefault("pyspark.sql", pyspark_sql_module)
    sys.modules.setdefault("pyspark.sql.functions", pyspark_sql_module.functions)
    sys.modules.setdefault("pyspark.sql.types", pyspark_sql_module.types)

    module_path = Path(__file__).resolve().parents[1] / "code" / "streaming_serving_mlflow_hdfs_kafka_optimize.py"
    spec = importlib.util.spec_from_file_location("streaming_module", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_get_registry_model_version_selects_latest():
    module = load_streaming_module()

    class Version:
        def __init__(self, version):
            self.version = version

    class FakeClient:
        def get_latest_versions(self, model_name):
            assert model_name == "sentiment-model"
            return [Version("1"), Version("10"), Version("2")]

    version, uri = module.get_registry_model_version(FakeClient(), "sentiment-model", "Production")

    assert version == "10"
    assert uri == "models:/sentiment-model/10"


def test_model_cache_set_and_get():
    module = load_streaming_module()
    cache = module.ModelCache()

    cache.set_model("model", "3", "models:/sentiment-model/3")
    model, version, uri = cache.get_model()

    assert model == "model"
    assert version == "3"
    assert uri == "models:/sentiment-model/3"
