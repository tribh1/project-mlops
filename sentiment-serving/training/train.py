# training/train.py
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import accuracy_score, f1_score, classification_report
import mlflow
import mlflow.sklearn
import joblib
from data_preparation import load_and_clean_data

def train_models():
    # 1. Tải và chuẩn bị dữ liệu
    print("Loading data...")
    df = load_and_clean_data('./sentiment140.csv')
    # Lấy mẫu nhỏ để chạy nhanh (có thể bỏ dòng này để train full)
    df = df.sample(frac=0.1, random_state=42)

    X = df['cleaned_text']
    y = df['target']

    # 2. Chia tập train/test và vector hóa văn bản
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    vectorizer = TfidfVectorizer(max_features=5000)
    X_train_tfidf = vectorizer.fit_transform(X_train)
    X_test_tfidf = vectorizer.transform(X_test)

    # Lưu vectorizer để dùng sau
    joblib.dump(vectorizer, './models/tfidf_vectorizer.pkl')

    # 3. Định nghĩa các model cần train
    models = {
        "Logistic Regression": LogisticRegression(max_iter=1000, random_state=42),
        "Random Forest": RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1),
        "Gradient Boosting": GradientBoostingClassifier(n_estimators=100, random_state=42)
    }

    # 4. Huấn luyện và log lên MLflow
    mlflow.set_tracking_uri("file:../mlruns")  # Lưu local
    mlflow.set_experiment("Sentiment140_Comparison")

    for model_name, model in models.items():
        with mlflow.start_run(run_name=model_name):
            print(f"Training {model_name}...")
            model.fit(X_train_tfidf, y_train)

            # Dự đoán và đánh giá
            y_pred = model.predict(X_test_tfidf)
            accuracy = accuracy_score(y_test, y_pred)
            f1 = f1_score(y_test, y_pred, average='weighted')

            # Log tham số và metrics lên MLflow
            mlflow.log_param("model_type", model_name)
            mlflow.log_metric("accuracy", accuracy)
            mlflow.log_metric("f1_score", f1)

            # Log model
            #mlflow.sklearn.log_model(model, artifact_path=f"{model_name.lower().replace(' ', '_')}_model")
            mlflow.sklearn.log_model(model, name=f"{model_name.lower().replace(' ', '_')}_model")
            # In kết quả
            print(f"{model_name} - Accuracy: {accuracy:.4f}, F1-Score: {f1:.4f}")
            print(classification_report(y_test, y_pred, target_names=['Negative', 'Positive']))

            # Lưu model ra file .pkl để dùng trong API
            joblib.dump(model, f'./models/{model_name.lower().replace(" ", "_")}.pkl')

    print("Training completed! Models saved to '../models/'")

if __name__ == "__main__":
    train_models()