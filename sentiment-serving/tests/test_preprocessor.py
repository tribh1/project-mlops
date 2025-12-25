import importlib
import pathlib
import sys
import types
import unittest


APP_DIR = pathlib.Path(__file__).resolve().parents[1] / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))


class TestPreprocessor(unittest.TestCase):
    def test_clean_text_removes_noise_and_stopwords(self):
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

        result = preprocessor.clean_text("Hello, THE world!!!")

        self.assertEqual(result, "hello world")
