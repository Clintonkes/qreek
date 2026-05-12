from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from database.session import init_db
from routers import web_auth, web_rates, web_wallet, web_pools, web_alerts, web_ws, web_payroll, web_payment_links, web_monnify
import os


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Asynchronous context manager for the FastAPI application lifespan.
    Handles the initialization of the database on startup.
    """
    await init_db()
    yield


app = FastAPI(title="Qreek Web API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://qreekfinance.org",
        "https://www.qreekfinance.org",
        "http://localhost:5173",
        "http://localhost:3000",
        os.getenv("FRONTEND_URL", ""),
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(web_auth.router)
app.include_router(web_rates.router)
app.include_router(web_wallet.router)
app.include_router(web_pools.router)
app.include_router(web_alerts.router)
app.include_router(web_payroll.router)
app.include_router(web_payment_links.router)
app.include_router(web_monnify.router)
app.include_router(web_ws.router)


@app.get("/")
async def root():
    """
    Root endpoint for the API.
    Returns a welcome message, version, and link to documentation.
    """
    return {"message": "Qreek Web API", "version": "1.0.0", "docs": "/docs"}


@app.get("/health")
async def health():
    """
    Health check endpoint to verify the service is running.
    """
    return {"status": "live", "service": "qreek-web"}
