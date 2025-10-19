# Use official Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the app code
COPY . .

# Set environment variables
ENV PORT=8080
ENV PYTHONUNBUFFERED=1

# Expose port 8080 for Cloud Run
EXPOSE 8080

# Run the app using gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "main:app"]
