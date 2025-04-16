FROM python:3.10-slim

# Install uv
RUN pip install uv

WORKDIR /app

# Copy pyproject files first for better caching
COPY pyproject.toml .
COPY uv.lock .
RUN uv pip install -r uv.lock

# Copy the rest of your app
COPY . .

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]