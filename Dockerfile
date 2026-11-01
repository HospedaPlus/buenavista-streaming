# Imagen única para el productor F2 y el consumidor Kafka -> Neon.
# La misma imagen sirve en local (docker compose) y en AWS (ECS Fargate).
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
COPY catalog ./catalog
RUN pip install --no-cache-dir . \
    && useradd --create-home --uid 10001 app

USER app

# Por defecto arranca el consumidor que guarda en Neon; el compose lo cambia para el productor.
CMD ["python", "-m", "src.neon_consumer"]
