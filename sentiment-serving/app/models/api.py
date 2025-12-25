# app/api.py
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
import os
import sys

# Thêm đường dẫn để Python có thể tìm thấy module models
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

# Lấy đường dẫn tuyệt đối của file hiện tại
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
static_dir = os.path.join(BASE_DIR, "static")

# Import SentimentPredictor sau khi đã thêm đường dẫn
from models.predictor import SentimentPredictor

app = FastAPI(title="Sentiment Analysis API", version="1.0")

# Phục vụ file tĩnh (giao diện web)
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# Khởi tạo predictor
predictor = SentimentPredictor()

# Định nghĩa kiểu dữ liệu đầu vào
class TextInput(BaseModel):
    text: str
    model: str = "logistic_regression"  # Mặc định dùng Logistic Regression

@app.get("/")
def read_root():
    # Trả về giao diện web khi truy cập trang chủ
    return FileResponse(static_dir + "/index.html")

@app.get("/api")
def api_info():
    return {"message": "Sentiment Analysis API is running. Use POST /predict to analyze text."}

@app.post("/predict")
def predict_sentiment(data: TextInput):
    try:
        result = predictor.predict(data.text, data.model)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/models")
def list_models():
    return {"available_models": list(predictor.models.keys())}

# Endpoint để kiểm tra health
@app.get("/health")
def health_check():
    return {"status": "healthy", "models_loaded": len(predictor.models)}