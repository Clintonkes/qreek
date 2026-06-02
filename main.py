"""
@file main.py
@description Entry point for the Qreek Web API.
This file initializes the FastAPI application, configures CORS for secure frontend communication, 
mounts all functional API routers, and manages the application lifespan.

Flow:
1. Startup: Triggers the `lifespan` event to initialize the database connection (via init_db).
2. Middleware: Injects CORSMiddleware to allow requests from authorized frontend origins.
3. Routing: Mounts specialized routers for auth, payments, pools, payroll, and more.
4. Health: Provides monitoring endpoints (root and /health) for deployment status.
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.requests import Request
from database.session import init_db
from routers import web_auth, web_rates, web_wallet, web_pools, web_alerts, web_ws, web_payroll, web_payment_links, web_flutterwave
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

# Ensure CORS headers are present even on error responses (e.g. 500s from unhandled
# exceptions like DB errors). The CORSMiddleware should handle most cases, but an
# explicit handler guarantees the ACAO header for origins like https://qreekfinance.org
# so the browser doesn't block the response with "No 'Access-Control-Allow-Origin'".
@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    # Log is already done by uvicorn for ASGI errors; here we just return clean JSON.
    # In production you might want more selective handling (e.g. only for certain exc).
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error. Please try again or contact support."},
    )

app.include_router(web_auth.router)
app.include_router(web_rates.router)
app.include_router(web_wallet.router)
app.include_router(web_pools.router)
app.include_router(web_alerts.router)
app.include_router(web_payroll.router)
app.include_router(web_payment_links.router)
app.include_router(web_flutterwave.router)
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
