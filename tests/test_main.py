from fastapi.testclient import TestClient
from app.main import app, get_session

# Функция, которая создает временную сессию БД для тестов

client = TestClient(app)

def test_create_and_redirect_url():
    # 1. Создание ссылки
    response = client.post(
        "/shorten",
        json={"target_url": "https://www.google.com"}
    )
    assert response.status_code == 200
    data = response.json()
    assert "short_code" in data
    
    short_code = data["short_code"]

    # 2. Проверка перенаправления
    redirect_response = client.get(f"/{short_code}", follow_redirects=False)
    assert redirect_response.status_code == 307
    assert redirect_response.headers["location"] == "https://www.google.com"