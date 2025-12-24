FROM python:3.10-slim

WORKDIR /app

# Install system dependencies if needed (e.g. for pdf parsing libraries)
# RUN apt-get update && apt-get install -y ...

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create data directories
RUN mkdir -p data/invoices data/bonnen

# Expose the port
EXPOSE 5050

# Run the application
CMD ["python", "app.py"]
