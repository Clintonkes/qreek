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
import json
import logging
import time
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.requests import Request
from database.session import init_db
from routers import web_auth, web_rates, web_wallet, web_pools, web_family, web_alerts, web_ws, web_payroll, web_payment_links, web_flutterwave
import os

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("qreek.api")

ALLOWED_ORIGINS = [
    "https://qreekfinance.org",
    "https://www.qreekfinance.org",
    "http://localhost:5173",
    "http://localhost:3000",
]


def _cors_headers(origin: str | None) -> dict[str, str]:
    if not origin or origin not in ALLOWED_ORIGINS:
        return {}
    return {
        "Access-Control-Allow-Origin": origin,
        "Access-Control-Allow-Credentials": "true",
        "Vary": "Origin",
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Asynchronous context manager for the FastAPI application lifespan.
    Handles the initialization of the database on startup.
    """
    await init_db()
    yield


app = FastAPI(title="Qreek Web API", version="1.0.0", lifespan=lifespan)


@app.middleware("http")
async def railway_request_logger(request: Request, call_next):
    started = time.perf_counter()
    request_id = request.headers.get("x-request-id") or request.headers.get("railway-request-id")
    log_base = {
        "event": "http_request",
        "request_id": request_id,
        "method": request.method,
        "path": request.url.path,
        "query": str(request.url.query)[:500],
        "client_ip": request.headers.get("x-forwarded-for", request.client.host if request.client else None),
        "user_agent": request.headers.get("user-agent"),
    }
    try:
        response = await call_next(request)
    except Exception:
        elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
        logger.exception(json.dumps({**log_base, "status_code": 500, "duration_ms": elapsed_ms}))
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error. Please try again or contact support."},
            headers=_cors_headers(request.headers.get("origin")),
        )

    elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
    log_line = json.dumps({**log_base, "status_code": response.status_code, "duration_ms": elapsed_ms})
    if response.status_code >= 500:
        logger.error(log_line)
    elif response.status_code >= 400:
        logger.warning(log_line)
    else:
        logger.info(log_line)
    return response

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS + ([os.getenv("FRONTEND_URL", "")] if os.getenv("FRONTEND_URL", "") else []),
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
    logger.exception(
        "Unhandled API exception path=%s method=%s",
        request.url.path,
        request.method,
        exc_info=exc,
    )
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error. Please try again or contact support."},
        headers=_cors_headers(request.headers.get("origin")),
    )

app.include_router(web_auth.router)
app.include_router(web_rates.router)
app.include_router(web_wallet.router)
app.include_router(web_pools.router)
app.include_router(web_family.router)
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
