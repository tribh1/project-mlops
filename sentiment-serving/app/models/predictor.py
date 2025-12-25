# app/models/predictor.py
import joblib
import os
from .preprocessor import clean_text

class SentimentPredictor:
    def __init__(self, models_dir=None):
        if models_dir is None:
            # Đi từ file predictor.py ra ngoài 2 cấp để đến thư mục gốc
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            models_dir = os.path.join(base_dir, 'models')
        # Tải TF-IDF Vectorizer
        self.vectorizer = joblib.load(os.path.join(models_dir, 'tfidf_vectorizer.pkl'))

        # Tải các model
        self.models = {
            "logistic_regression": joblib.load(os.path.join(models_dir, 'logistic_regression.pkl')),
            "random_forest": joblib.load(os.path.join(models_dir, 'random_forest.pkl')),
            "gradient_boosting": joblib.load(os.path.join(models_dir, 'gradient_boosting.pkl'))
        }

    def predict(self, text: str, model_name: str = "logistic_regression"):
        """Dự đoán sentiment cho một văn bản với model được chọn."""
        # Làm sạch và vector hóa văn bản
        cleaned_text = clean_text(text)
        text_vectorized = self.vectorizer.transform([cleaned_text])

        # Lấy model và dự đoán
        model = self.models.get(model_name)
        if not model:
            raise ValueError(f"Model '{model_name}' not found. Available: {list(self.models.keys())}")

        prediction = model.predict(text_vectorized)[0]
        probability = model.predict_proba(text_vectorized)[0]

        # Ánh xạ kết quả
        sentiment = "Positive" if prediction == 1 else "Negative"
        confidence = float(probability[prediction])

        return {
            "text": text,
            "sentiment": sentiment,
            "confidence": confidence,
            "model_used": model_name
        }