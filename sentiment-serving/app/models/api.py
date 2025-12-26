# app/api.py
from fastapi import FastAPI, HTTPException, Response, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
import os
import sys
import time
from threading import Lock

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

# Lightweight Prometheus-style metrics (no external dependency)
METRICS_LOCK = Lock()
REQUEST_COUNT = {}
REQUEST_LATENCY = {}
IN_FLIGHT_REQUESTS = 0
LATENCY_BUCKETS = [0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]

CONTENT_TYPE_LATEST = "text/plain; version=0.0.4; charset=utf-8"

def _labels_to_key(labels):
    return tuple(sorted(labels.items()))

def _format_labels(labels):
    if not labels:
        return ""
    pairs = [f'{key}="{value}"' for key, value in labels.items()]
    return "{" + ",".join(pairs) + "}"

def _observe_latency(endpoint, elapsed):
    key = _labels_to_key({"endpoint": endpoint})
    if key not in REQUEST_LATENCY:
        REQUEST_LATENCY[key] = {
            "count": 0,
            "sum": 0.0,
            "buckets": {bucket: 0 for bucket in LATENCY_BUCKETS},
        }
    entry = REQUEST_LATENCY[key]
    entry["count"] += 1
    entry["sum"] += elapsed
    for bucket in LATENCY_BUCKETS:
        if elapsed <= bucket:
            entry["buckets"][bucket] += 1

def _inc_request_count(method, endpoint, status_code):
    key = _labels_to_key(
        {"method": method, "endpoint": endpoint, "http_status": status_code}
    )
    REQUEST_COUNT[key] = REQUEST_COUNT.get(key, 0) + 1

def _render_metrics():
    lines = []
    lines.append("# HELP sentiment_serving_requests_total Total HTTP requests")
    lines.append("# TYPE sentiment_serving_requests_total counter")
    for key, value in REQUEST_COUNT.items():
        labels = dict(key)
        lines.append(
            f"sentiment_serving_requests_total{_format_labels(labels)} {value}"
        )

    lines.append("# HELP sentiment_serving_request_latency_seconds Request latency in seconds")
    lines.append("# TYPE sentiment_serving_request_latency_seconds histogram")
    for key, entry in REQUEST_LATENCY.items():
        labels = dict(key)
        cumulative = 0
        for bucket in LATENCY_BUCKETS:
            cumulative += entry["buckets"][bucket]
            bucket_labels = {**labels, "le": str(bucket)}
            lines.append(
                "sentiment_serving_request_latency_seconds_bucket"
                f"{_format_labels(bucket_labels)} {cumulative}"
            )
        bucket_labels = {**labels, "le": "+Inf"}
        lines.append(
            "sentiment_serving_request_latency_seconds_bucket"
            f"{_format_labels(bucket_labels)} {entry['count']}"
        )
        lines.append(
            "sentiment_serving_request_latency_seconds_count"
            f"{_format_labels(labels)} {entry['count']}"
        )
        lines.append(
            "sentiment_serving_request_latency_seconds_sum"
            f"{_format_labels(labels)} {entry['sum']}"
        )

    lines.append("# HELP sentiment_serving_in_flight_requests Number of in-flight requests")
    lines.append("# TYPE sentiment_serving_in_flight_requests gauge")
    lines.append(f"sentiment_serving_in_flight_requests {IN_FLIGHT_REQUESTS}")
    return "\n".join(lines) + "\n"

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
    with METRICS_LOCK:
        global IN_FLIGHT_REQUESTS
        IN_FLIGHT_REQUESTS += 1
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    except Exception:
        status_code = 500
        raise
    finally:
        elapsed = time.perf_counter() - start_time
        with METRICS_LOCK:
            _inc_request_count(request.method, endpoint, status_code)
            _observe_latency(endpoint, elapsed)
            IN_FLIGHT_REQUESTS -= 1

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
    with METRICS_LOCK:
        payload = _render_metrics()
    return Response(payload, media_type=CONTENT_TYPE_LATEST)
