# Use an official Python runtime as the base image
FROM python:3.8
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app/
CMD ["python", "data_ingestion_script.py"]
