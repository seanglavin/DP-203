from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.endpoints import sports

# Create database tables
# models.Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Sports Data ETL API",
    description="API for fetching sports data and processing it through an ETL pipeline to Azure",
    version="0.1.0",
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
# app.include_router(sports.router, prefix="/api/sports", tags=["sports"])

@app.get("/")
async def root():
    return {"message": "Welcome to the Sports Data ETL API"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}