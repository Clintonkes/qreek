import hashlib
import os
import secrets
import uuid
from datetime import datetime, timedelta

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import WebSession
from database.session import get_db

SECRET = os.getenv("JWT_SECRET", "qreek-change-this-in-production-use-openssl-rand-hex-32")
ALGO = "HS256"
ACCESS_TOKEN_MINUTES = int(os.getenv("ACCESS_TOKEN_MINUTES", "30"))
REFRESH_TOKEN_DAYS = int(os.getenv("REFRESH_TOKEN_DAYS", "30"))
SESSION_IDLE_MINUTES = int(os.getenv("SESSION_IDLE_MINUTES", "30"))
bearer = HTTPBearer()


def _utcnow() -> datetime:
    return datetime.utcnow()


def _client_ip(request: Request | None) -> str | None:
    if not request or not request.client:
        return None
    return request.headers.get("x-forwarded-for", request.client.host).split(",")[0].strip()


def hash_refresh_token(refresh_token: str) -> str:
    return hashlib.sha256(refresh_token.encode("utf-8")).hexdigest()


def _session_expired(session: WebSession, now: datetime | None = None) -> bool:
    now = now or _utcnow()
    idle_cutoff = now - timedelta(minutes=SESSION_IDLE_MINUTES)
    return bool(
        session.is_revoked
        or session.expires_at <= now
        or session.last_activity_at <= idle_cutoff
    )


def _create_access_token(phone: str, session_id: str, access_jti: str) -> str:
    now = _utcnow()
    payload = {
        "typ": "access",
        "phone": phone,
        "sid": session_id,
        "jti": access_jti,
        "iat": now,
        "exp": now + timedelta(minutes=ACCESS_TOKEN_MINUTES),
    }
    return jwt.encode(payload, SECRET, algorithm=ALGO)


def _new_refresh_token() -> str:
    return secrets.token_urlsafe(48)


async def issue_session_tokens(db: AsyncSession, phone: str, request: Request | None = None) -> dict:
    refresh_token = _new_refresh_token()
    access_jti = uuid.uuid4().hex
    now = _utcnow()
    session = WebSession(
        user_phone=phone,
        refresh_token_hash=hash_refresh_token(refresh_token),
        current_access_jti=access_jti,
        user_agent=request.headers.get("user-agent")[:255] if request else None,
        ip_address=_client_ip(request),
        last_activity_at=now,
        expires_at=now + timedelta(days=REFRESH_TOKEN_DAYS),
    )
    db.add(session)
    await db.flush()

    return {
        "token": _create_access_token(phone, session.id, access_jti),
        "refresh_token": refresh_token,
        "token_type": "bearer",
        "expires_in": ACCESS_TOKEN_MINUTES * 60,
        "session_id": session.id,
    }


async def refresh_session_tokens(db: AsyncSession, refresh_token: str, request: Request | None = None) -> dict:
    token_hash = hash_refresh_token(refresh_token)
    result = await db.execute(select(WebSession).where(WebSession.refresh_token_hash == token_hash))
    session = result.scalar_one_or_none()
    now = _utcnow()

    if not session or _session_expired(session, now):
        if session and not session.is_revoked:
            session.is_revoked = True
            session.revoked_at = now
            await db.commit()
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Session expired. Please log in again.")

    next_refresh_token = _new_refresh_token()
    next_access_jti = uuid.uuid4().hex
    session.refresh_token_hash = hash_refresh_token(next_refresh_token)
    session.current_access_jti = next_access_jti
    session.last_activity_at = now
    session.user_agent = request.headers.get("user-agent")[:255] if request else session.user_agent
    session.ip_address = _client_ip(request) or session.ip_address
    await db.commit()

    return {
        "token": _create_access_token(session.user_phone, session.id, next_access_jti),
        "refresh_token": next_refresh_token,
        "token_type": "bearer",
        "expires_in": ACCESS_TOKEN_MINUTES * 60,
        "session_id": session.id,
    }


async def revoke_session(db: AsyncSession, session_id: str, phone: str | None = None) -> None:
    query = select(WebSession).where(WebSession.id == session_id)
    if phone:
        query = query.where(WebSession.user_phone == phone)
    result = await db.execute(query)
    session = result.scalar_one_or_none()
    if session and not session.is_revoked:
        session.is_revoked = True
        session.revoked_at = _utcnow()
        await db.commit()


async def revoke_all_sessions(db: AsyncSession, phone: str) -> None:
    result = await db.execute(select(WebSession).where(WebSession.user_phone == phone, WebSession.is_revoked == False))
    sessions = result.scalars().all()
    now = _utcnow()
    for session in sessions:
        session.is_revoked = True
        session.revoked_at = now
    await db.commit()


async def decode_token(
    creds: HTTPAuthorizationCredentials = Depends(bearer),
    db: AsyncSession = Depends(get_db),
) -> dict:
    try:
        claims = jwt.decode(creds.credentials, SECRET, algorithms=[ALGO])
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token")

    if claims.get("typ") != "access" or not claims.get("phone") or not claims.get("sid") or not claims.get("jti"):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    result = await db.execute(select(WebSession).where(WebSession.id == claims["sid"], WebSession.user_phone == claims["phone"]))
    session = result.scalar_one_or_none()
    now = _utcnow()

    if not session or _session_expired(session, now) or session.current_access_jti != claims["jti"]:
        if session and not session.is_revoked:
            session.is_revoked = True
            session.revoked_at = now
            await db.commit()
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Session expired. Please log in again.")

    session.last_activity_at = now
    await db.commit()
    return {**claims, "session_id": session.id}


async def decode_token_string(token: str, db: AsyncSession) -> dict:
    try:
        claims = jwt.decode(token, SECRET, algorithms=[ALGO])
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token")

    if claims.get("typ") != "access" or not claims.get("phone") or not claims.get("sid") or not claims.get("jti"):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    result = await db.execute(select(WebSession).where(WebSession.id == claims["sid"], WebSession.user_phone == claims["phone"]))
    session = result.scalar_one_or_none()
    now = _utcnow()

    if not session or _session_expired(session, now) or session.current_access_jti != claims["jti"]:
        if session and not session.is_revoked:
            session.is_revoked = True
            session.revoked_at = now
            await db.commit()
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Session expired. Please log in again.")

    session.last_activity_at = now
    await db.commit()
    return {**claims, "session_id": session.id}
