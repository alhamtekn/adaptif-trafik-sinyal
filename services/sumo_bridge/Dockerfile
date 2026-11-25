FROM python:3.11-slim
# SUMO ikili dosyalarının ihtiyaç duyduğu sistem kütüphaneleri (slim imajda yok)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libexpat1 libatomic1 libgl1 libx11-6 libxext6 libxrender1 \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY bridge.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "bridge.py"]
