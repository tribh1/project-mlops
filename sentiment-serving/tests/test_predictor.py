import importlib
import pathlib
import sys
import types
import unittest
from unittest import mock


APP_DIR = pathlib.Path(__file__).resolve().parents[1] / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))


class DummyVectorizer:
    def __init__(self):
        self.seen = None

    def transform(self, texts):
        self.seen = texts
        return ["vector"]


class DummyModel:
    def __init__(self, prediction):
        self.prediction = prediction

    def predict(self, _):
        return [self.prediction]

    def predict_proba(self, _):
        if self.prediction == 1:
            return [[0.2, 0.8]]
        return [[0.9, 0.1]]


class TestSentimentPredictor(unittest.TestCase):
    def _import_predictor(self):
        nltk_module = types.ModuleType("nltk")
        corpus_module = types.ModuleType("nltk.corpus")
        stopwords_module = types.ModuleType("nltk.corpus.stopwords")
        stem_module = types.ModuleType("nltk.stem")

        def download(_):
            return True

        def words(_):
            return {"the"}

        class DummyLemmatizer:
            def lemmatize(self, word):
                return word

        nltk_module.download = download
        stopwords_module.words = words
        stem_module.WordNetLemmatizer = DummyLemmatizer
        corpus_module.stopwords = stopwords_module
        nltk_module.corpus = corpus_module
        nltk_module.stem = stem_module

        sys.modules["nltk"] = nltk_module
        sys.modules["nltk.corpus"] = corpus_module
        sys.modules["nltk.corpus.stopwords"] = stopwords_module
        sys.modules["nltk.stem"] = stem_module

        preprocessor = importlib.import_module("models.preprocessor")
        importlib.reload(preprocessor)
        predictor = importlib.import_module("models.predictor")
        return importlib.reload(predictor)

    def test_predict_returns_expected_payload(self):
        predictor_module = self._import_predictor()
        vectorizer = DummyVectorizer()
        model_lr = DummyModel(prediction=1)
        model_rf = DummyModel(prediction=0)
        model_gb = DummyModel(prediction=1)

        with mock.patch("models.predictor.joblib.load") as mock_load, \
            mock.patch("models.predictor.clean_text", return_value="cleaned text"):
            mock_load.side_effect = [vectorizer, model_lr, model_rf, model_gb]
            predictor = predictor_module.SentimentPredictor(models_dir="models")

            result = predictor.predict("Great product!", "logistic_regression")

        self.assertEqual(vectorizer.seen, ["cleaned text"])
        self.assertEqual(result["sentiment"], "Positive")
        self.assertEqual(result["confidence"], 0.8)
        self.assertEqual(result["model_used"], "logistic_regression")

    def test_predict_raises_for_unknown_model(self):
        predictor_module = self._import_predictor()
        vectorizer = DummyVectorizer()
        model_lr = DummyModel(prediction=1)
        model_rf = DummyModel(prediction=0)
        model_gb = DummyModel(prediction=1)

        with mock.patch("models.predictor.joblib.load") as mock_load, \
            mock.patch("models.predictor.clean_text", return_value="cleaned text"):
            mock_load.side_effect = [vectorizer, model_lr, model_rf, model_gb]
            predictor = predictor_module.SentimentPredictor(models_dir="models")

            with self.assertRaises(ValueError):
                predictor.predict("Great product!", "unknown_model")
