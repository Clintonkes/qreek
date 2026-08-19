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
bearer_optional = HTTPBearer(auto_error=False)


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


CARD_CHECKOUT_TYP = "card_checkout"
CARD_CHECKOUT_MINUTES = 10


def issue_card_checkout_token(phone: str) -> str:
    """
    A narrow, short-lived credential proving "this browser just verified it
    controls this phone number" — issued after an OTP check on the public
    checkout page. Deliberately NOT a real access token: typ differs from
    "access", so decode_token/decode_token_string reject it outright, and it
    carries no session id, so it can never touch dashboard/wallet/payroll
    endpoints. It only ever proves enough to list and charge that phone's
    saved cards for one guest checkout.
    """
    now = _utcnow()
    payload = {
        "typ": CARD_CHECKOUT_TYP,
        "phone": phone,
        "iat": now,
        "exp": now + timedelta(minutes=CARD_CHECKOUT_MINUTES),
    }
    return jwt.encode(payload, SECRET, algorithm=ALGO)


def decode_card_checkout_token(token: str) -> str:
    """Returns the verified phone for a card-checkout token, or raises 401."""
    try:
        claims = jwt.decode(token, SECRET, algorithms=[ALGO])
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="This card session has expired. Verify your phone again.")
    if claims.get("typ") != CARD_CHECKOUT_TYP or not claims.get("phone"):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid card session.")
    return claims["phone"]


async def decode_token_optional(
    creds: HTTPAuthorizationCredentials | None = Depends(bearer_optional),
    db: AsyncSession = Depends(get_db),
) -> dict | None:
    """
    Same validation as decode_token, but returns None instead of raising when no
    (or an invalid/expired) bearer token is present. Used on payer-facing endpoints
    that stay public by default but should recognize a logged-in Qreek user when
    one happens to be present — e.g. saved-card capture on payment link checkout.
    """
    if not creds:
        return None
    try:
        return await decode_token_string(creds.credentials, db)
    except HTTPException:
        return None


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
