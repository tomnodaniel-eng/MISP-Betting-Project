from fastapi import FastAPI
import os

app = FastAPI(title="MISP Betting API")

@app.get("/")
def read_root():
    return {"message": "MISP Betting API is running on Render!"}

@app.get("/health")
def health_check():
    db_url = os.getenv('DATABASE_URL')
    return {
        "status": "healthy", 
        "database_connected": bool(db_url),
        "environment": "production"
    }

@app.get("/add/{a}/{b}")
def api_add(a: int, b: int):
    result = a + b
    return {"operation": "add", "result": result}

@app.get("/multiply/{a}/{b}")
def api_multiply(a: int, b: int):
    result = a * b
    return {"operation": "multiply", "result": result}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)