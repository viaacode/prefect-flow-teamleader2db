FROM python:3.10-slim
RUN pip install --no-cache-dir fastapi==0.108.0 uvicorn==0.25.0 python-multipart==0.0.6
COPY app.py /app/app.py
WORKDIR /app
EXPOSE 8080
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
