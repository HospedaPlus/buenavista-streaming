FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install the project dependencies needed by the runtime.
COPY pyproject.toml README.md ./
COPY src ./src
COPY catalog ./catalog
RUN python -m pip install --no-cache-dir .

# Run as a non-root user in the application container.
RUN useradd --create-home --uid 10001 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000
CMD ["uvicorn", "src.api:app", "--host", "0.0.0.0", "--port", "8000"]
