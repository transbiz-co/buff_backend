FROM python:3.12-slim

WORKDIR /app

# 安裝 Poetry
RUN pip install poetry

# 複製 poetry 配置文件
COPY pyproject.toml ./

# 生成 lock 文件並安裝依賴
RUN poetry config virtualenvs.create false \
    && poetry lock \
    && poetry install --no-interaction --no-ansi --no-root

# 複製專案文件
COPY . .

EXPOSE 8000

CMD ["poetry", "run", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"] 