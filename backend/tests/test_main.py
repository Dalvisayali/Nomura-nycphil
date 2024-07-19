# tests/test_main.py
import os

import sys

# Add the parent directory of main.py to the Python path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

def test_docs_endpoint():
    response = client.get("/docs")
    assert response.status_code == 200
    # assert response.json() == {"message": "API documentation"}

# def test_health_endpoint():
#     response = client.get("/health")
#     assert response.status_code == 200
#     assert response.json() == {"status": "ok"}
