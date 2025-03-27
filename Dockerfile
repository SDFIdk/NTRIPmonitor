FROM python:3.11.9-alpine3.20

WORKDIR /app
COPY requirements.txt ./

# Updates to newest packages,
RUN apk update \
    && apk upgrade --no-cache --available \
    && apk add --no-cache \
    && pip install --no-cache-dir -r requirements.txt \
    && mkdir ./config

COPY ./src/ ./
COPY ./src/settings.py ./settings.py
COPY ./src/ingestion.py ./ingestion.py

CMD ["python3", "ingestion.py"]
