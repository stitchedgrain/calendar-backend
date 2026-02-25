from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {"status": "server is alive"}

@app.get("/health")
def health():
    return {"ok": True}
