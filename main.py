from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {"status": "server is alive"}

@app.get("/health")
def health():
    return {"ok": True}
@app.get("/privacy")
def privacy():
    return {
        "policy": "This application accesses your calendar only to read availability and create bookings requested by you. We do not sell or share your data."
    }

@app.get("/terms")
def terms():
    return {
        "terms": "By using this service you authorize the application to read calendar availability and create events on your behalf."
    }
