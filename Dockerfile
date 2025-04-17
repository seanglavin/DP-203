FROM python:3.13-slim

# Install uv by copying the binary
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy pyproject files first for better caching
COPY pyproject.toml uv.lock ./

# Install dependencies using uv pip install into the system Python
RUN uv pip install --system . --no-cache

# Copy the rest of the app
COPY . .

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]