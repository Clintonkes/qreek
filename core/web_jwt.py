from jose import jwt, JWTError
from datetime import datetime, timedelta
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import os

SECRET   = os.getenv("JWT_SECRET", "qreek-change-this-in-production-use-openssl-rand-hex-32")
ALGO     = "HS256"
EXPIRE_H = 24 * 30   # 30-day tokens
bearer   = HTTPBearer()


def create_token(payload: dict) -> str:
    data = {**payload, "exp": datetime.utcnow() + timedelta(hours=EXPIRE_H)}
    return jwt.encode(data, SECRET, algorithm=ALGO)


def decode_token(creds: HTTPAuthorizationCredentials = Depends(bearer)) -> dict:
    try:
        return jwt.decode(creds.credentials, SECRET, algorithms=[ALGO])
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token")
