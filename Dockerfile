FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
RUN python -m pip install --no-cache-dir .

RUN useradd --system --uid 10001 --create-home appuser
USER 10001:10001

ENTRYPOINT ["python"]
CMD ["-m", "src.producer", "--continuous"]
