# app/api.py
from fastapi import FastAPI, HTTPException, Response, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
import os
import sys
import time
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

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

# Prometheus metrics
REQUEST_COUNT = Counter(
    "sentiment_serving_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "http_status"],
)
REQUEST_LATENCY = Histogram(
    "sentiment_serving_request_latency_seconds",
    "Request latency in seconds",
    ["endpoint"],
)
IN_FLIGHT_REQUESTS = Gauge(
    "sentiment_serving_in_flight_requests",
    "Number of in-flight requests",
)

# Khởi tạo predictor
predictor = SentimentPredictor()

# Định nghĩa kiểu dữ liệu đầu vào
class TextInput(BaseModel):
    text: str
    model: str = "logistic_regression"  # Mặc định dùng Logistic Regression

@app.middleware("http")
async def monitor_requests(request: Request, call_next):
    endpoint = request.url.path
    start_time = time.perf_counter()
    IN_FLIGHT_REQUESTS.inc()
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    except Exception:
        status_code = 500
        raise
    finally:
        elapsed = time.perf_counter() - start_time
        REQUEST_COUNT.labels(request.method, endpoint, status_code).inc()
        REQUEST_LATENCY.labels(endpoint).observe(elapsed)
        IN_FLIGHT_REQUESTS.dec()

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

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
