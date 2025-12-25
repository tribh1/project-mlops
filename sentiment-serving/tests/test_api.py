import importlib
import pathlib
import sys
import types
import unittest

from fastapi.testclient import TestClient


APP_DIR = pathlib.Path(__file__).resolve().parents[1] / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))


class DummyPredictor:
    def __init__(self):
        self.models = {
            "logistic_regression": object(),
            "random_forest": object(),
        }

    def predict(self, text, model):
        return {
            "text": text,
            "sentiment": "Positive",
            "confidence": 0.95,
            "model_used": model,
        }


class TestAPI(unittest.TestCase):
    def setUp(self):
        dummy_module = types.ModuleType("models.predictor")
        dummy_module.SentimentPredictor = DummyPredictor
        sys.modules["models.predictor"] = dummy_module
        api = importlib.import_module("models.api")
        self.api_module = importlib.reload(api)
        self.client = TestClient(self.api_module.app)

    def tearDown(self):
        sys.modules.pop("models.predictor", None)

    def test_api_info(self):
        response = self.client.get("/api")

        self.assertEqual(response.status_code, 200)
        self.assertIn("Sentiment Analysis API is running", response.json()["message"])

    def test_predict_endpoint(self):
        response = self.client.post(
            "/predict",
            json={"text": "Amazing!", "model": "logistic_regression"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["sentiment"], "Positive")
        self.assertEqual(response.json()["model_used"], "logistic_regression")

    def test_models_endpoint(self):
        response = self.client.get("/models")

        self.assertEqual(response.status_code, 200)
        self.assertIn("logistic_regression", response.json()["available_models"])

    def test_health_endpoint(self):
        response = self.client.get("/health")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "healthy")
        self.assertEqual(response.json()["models_loaded"], 2)
