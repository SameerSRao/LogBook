FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml .
COPY src/ src/
RUN pip install -e ".[test]"
CMD ["python", "-m", "worker.main"]
