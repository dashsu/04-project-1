# dockerfile
FROM python:3.9

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the rest of the project
COPY . /app

# Run your pipeline module
CMD ["python", "-m", "marketstack_etl.trade_pipeline"]


