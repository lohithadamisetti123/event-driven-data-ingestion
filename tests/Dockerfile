FROM python:3.11-slim

WORKDIR /tests

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY integration_test.py .

CMD ["pytest", "-q"]
