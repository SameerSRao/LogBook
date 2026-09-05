FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml .
RUN pip install -e ".[test]"
COPY src/ src/
CMD ["uvicorn", "ingest_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
