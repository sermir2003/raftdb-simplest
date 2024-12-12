FROM python:3.11-slim

WORKDIR /app

COPY node /app/node
COPY requirements.txt /app

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python3", "-m", "node"]
