FROM python:3.11-slim-bookworm

# headless jre because I won't need any GUI components
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless procps && \
    rm -rf /var/lib/apt/lists/*

RUN java -version

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# --no-cache-dir to not bloat my docker image with installation cache

COPY src/ .

CMD ["python", "spark_process.py"]