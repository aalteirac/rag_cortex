FROM --platform=linux/amd64 python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY / .

RUN pip3 install -r requirements.txt

EXPOSE 8600

HEALTHCHECK CMD curl --fail http://localhost:8600/_stcore/health

ENTRYPOINT ["streamlit", "run", "index.py", "--server.port=8600"]