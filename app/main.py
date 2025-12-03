# app/main.py

import os
import secrets
import time
from typing import Optional, List
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, status, Depends
from fastapi.responses import RedirectResponse
from sqlmodel import SQLModel, Session, Field, create_engine, select
from sqlalchemy.exc import OperationalError
import redis.asyncio as redis

# --- 1. Настройки и Конфигурация --------------------------------
# Берем данные из .env файла через os.environ
DB_USER = os.environ.get("POSTGRES_USER")
DB_PASS = os.environ.get("POSTGRES_PASSWORD")
DB_HOST = os.environ.get("DB_HOST") 
DB_NAME = os.environ.get("POSTGRES_DB")

REDIS_HOST = os.environ.get("REDIS_HOST") 
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

# Строка подключения к PostgreSQL
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"

# --- 2. Модели Данных (SQLModel/Pydantic) --------------------------------

class URLBase(SQLModel):
    """Базовая модель для валидации входящих данных."""
    target_url: str = Field(index=True)
    
class URLCreate(URLBase):
    """Модель для создания новой ссылки."""
    short_code: Optional[str] = None

class URL(URLBase, table=True):
    """Модель для базы данных (SQLModel)."""
    id: Optional[int] = Field(default=None, primary_key=True)
    short_code: str = Field(index=True, unique=True, nullable=False)
    is_active: bool = Field(default=True)
    clicks: int = Field(default=0)
    
# --- 3. Инициализация Fast API и Ресурсов --------------------------------

engine = create_engine(DATABASE_URL)

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)

redis_client: redis.Redis = None

# --- 4. Зависимости (Dependency Injection) --------------------------------

def get_session():
    with Session(engine) as session:
        yield session

# --- 5. Жизненный цикл приложения (Lifespan) --------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Обработчик событий жизненного цикла.
    """
    print("Ожидание доступности БД и создание таблиц...")
    # Простейший ретрай, чтобы подождать, пока PostgreSQL полностью поднимется
    max_attempts = 10
    for attempt in range(1, max_attempts + 1):
        try:
            create_db_and_tables()
            print("Таблицы БД успешно созданы/проверены.")
            break
        except OperationalError as e:
            if attempt == max_attempts:
                print(f"Не удалось подключиться к БД после {max_attempts} попыток: {e}")
                raise
            wait_seconds = 2
            print(f"БД ещё не готова (попытка {attempt}/{max_attempts}): {e}. "
                  f"Повтор через {wait_seconds} сек...")
            time.sleep(wait_seconds)
    
    global redis_client
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    
    try:
        await redis_client.ping()
        print(f"Подключение к Redis успешно по адресу: {REDIS_HOST}:{REDIS_PORT}")
    except Exception as e:
        print(f"Ошибка подключения к Redis: {e}")
        
    yield 

    if redis_client:
        await redis_client.close()

# Инициализация приложения
app = FastAPI(title="FastAPI URL Shortener", lifespan=lifespan)

# --- 6. Хелперы и Вспомогательные функции --------------------------------

def generate_short_code(length: int = 7) -> str:
    """Генерирует случайный буквенно-цифровой код."""
    alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return "".join(secrets.choice(alphabet) for _ in range(length))

def get_db_url_by_code(*, code: str, session: Session) -> Optional[URL]:
    """Ищет URL в БД по короткому коду."""
    statement = select(URL).where(URL.short_code == code)
    return session.exec(statement).first()

# --- 7. Эндпоинты API (ПОРЯДОК ИСПРАВЛЕН!) --------------------------------

@app.post("/shorten", response_model=URL)
def create_short_url(url: URLCreate, session: Session = Depends(get_session)):
    """
    Создает новую короткую ссылку.
    """
    if url.short_code:
        if get_db_url_by_code(code=url.short_code, session=session):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Код '{url.short_code}' уже занят."
            )
        short_code = url.short_code
    else:
        while True:
            short_code = generate_short_code()
            if not get_db_url_by_code(code=short_code, session=session):
                break

    db_url = URL(target_url=url.target_url, short_code=short_code)
    session.add(db_url)
    session.commit()
    session.refresh(db_url)
    
    if redis_client:
        redis_client.set(short_code, db_url.target_url)

    return db_url

# --- 7А. БОЛЕЕ СПЕЦИФИЧНЫЙ РОУТ ПЕРВЫМ (Исправляет 405 ошибку) --------------------------------
@app.get("/stats/{short_code}")
def get_url_stats(short_code: str, session: Session = Depends(get_session)):
    """Получить статистику по короткой ссылке (количество кликов, целевой URL)."""
    db_url = get_db_url_by_code(code=short_code, session=session)
    if not db_url:
        raise HTTPException(status_code=404, detail="Ссылка не найдена")
    
    return {
        "short_code": db_url.short_code,
        "target_url": db_url.target_url,
        "clicks": db_url.clicks,
        "is_active": db_url.is_active
    }


# --- 7Б. ОБЩИЙ РОУТ ПОСЛЕДНИМ (Теперь он не конфликтует) --------------------------------
@app.get("/{short_code}")
async def redirect_to_target_url(short_code: str, request: Request, session: Session = Depends(get_session)):
    """
    Перенаправляет пользователя по короткой ссылке.
    """
    target_url = None

    if redis_client:
        target_url = await redis_client.get(short_code)
    
    if not target_url:
        db_url = get_db_url_by_code(code=short_code, session=session)

        if not db_url or not db_url.is_active:
            raise HTTPException(status_code=404, detail="Ссылка не найдена или неактивна")
        
        target_url = db_url.target_url
        
        if redis_client:
            await redis_client.set(short_code, target_url)

        db_url.clicks += 1
        session.add(db_url)
        session.commit()
    
    return RedirectResponse(url=target_url, status_code=status.HTTP_307_TEMPORARY_REDIRECT)

# --- 8. Тестовое окружение для Pytest (Необходимо для запуска тестов) --------------------------------         

from sqlmodel import create_engine as create_test_engine, SQLModel as TestSQLModel
testing_engine = create_test_engine("sqlite:///./test.db")

def create_test_db_and_tables():
    TestSQLModel.metadata.create_all(testing_engine)

def override_get_session():
    with Session(testing_engine) as session:
        yield session