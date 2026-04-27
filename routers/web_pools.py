from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from pydantic import BaseModel
from database.session import get_db
from database.models import Pool, PoolMember, FiatPool, FiatPoolMember
from core.web_jwt import decode_token

router = APIRouter(prefix="/api/v1/pools", tags=["pools"])


class CreatePoolBody(BaseModel):
    name:      str
    pool_type: str = "crypto"


class JoinPoolBody(BaseModel):
    invite_code: str


def _pool_dict(pool: Pool, role: str = "member") -> dict:
    return {
        "id":           pool.id,
        "name":         pool.name,
        "invite_code":  pool.invite_code,
        "pool_type":    pool.pool_type,
        "member_count": pool.member_count,
        "total_volume": pool.total_volume,
        "is_active":    pool.is_active,
        "created_at":   pool.created_at.isoformat() if pool.created_at else None,
        "role":         role,
    }


@router.get("")
async def list_pools(claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db)):
    phone = claims["phone"]

    result = await db.execute(select(PoolMember).where(PoolMember.user_phone == phone))
    memberships = result.scalars().all()

    pools = []
    for m in memberships:
        pr = await db.execute(select(Pool).where(Pool.id == m.pool_id))
        p  = pr.scalar_one_or_none()
        if p and p.is_active:
            pools.append(_pool_dict(p, m.role))

    return {"pools": pools}


@router.post("")
async def create_pool(
    body:   CreatePoolBody,
    claims: dict = Depends(decode_token),
    db:     AsyncSession = Depends(get_db),
):
    phone = claims["phone"]
    if body.pool_type not in ("crypto", "fiat"):
        raise HTTPException(status_code=400, detail="pool_type must be 'crypto' or 'fiat'")

    if body.pool_type == "fiat":
        pool = FiatPool(name=body.name, creator_phone=phone)
        db.add(pool)
        await db.flush()
        db.add(FiatPoolMember(pool_id=pool.id, user_phone=phone, role="admin"))
        await db.commit()
        return {"id": pool.id, "name": pool.name, "invite_code": pool.invite_code, "pool_type": "fiat", "role": "admin"}

    pool = Pool(name=body.name, creator_phone=phone, pool_type="crypto")
    db.add(pool)
    await db.flush()
    db.add(PoolMember(pool_id=pool.id, user_phone=phone, role="admin"))
    await db.commit()
    return _pool_dict(pool, "admin")


@router.post("/join")
async def join_pool(
    body:   JoinPoolBody,
    claims: dict = Depends(decode_token),
    db:     AsyncSession = Depends(get_db),
):
    phone = claims["phone"]
    code  = body.invite_code.strip().upper()

    pr   = await db.execute(select(Pool).where(Pool.invite_code == code))
    pool = pr.scalar_one_or_none()

    if not pool:
        fpr   = await db.execute(select(FiatPool).where(FiatPool.invite_code == code))
        fpool = fpr.scalar_one_or_none()
        if not fpool:
            raise HTTPException(status_code=404, detail="Invalid invite code")
        ex = await db.execute(select(FiatPoolMember).where(FiatPoolMember.pool_id == fpool.id, FiatPoolMember.user_phone == phone))
        if ex.scalar_one_or_none():
            raise HTTPException(status_code=400, detail="Already a member of this pool")
        fpool.member_count = (fpool.member_count or 1) + 1
        db.add(FiatPoolMember(pool_id=fpool.id, user_phone=phone, role="member"))
        await db.commit()
        return {"message": f"Joined fiat pool '{fpool.name}'", "pool_id": fpool.id}

    ex = await db.execute(select(PoolMember).where(PoolMember.pool_id == pool.id, PoolMember.user_phone == phone))
    if ex.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="Already a member of this pool")
    pool.member_count = (pool.member_count or 1) + 1
    db.add(PoolMember(pool_id=pool.id, user_phone=phone, role="member"))
    await db.commit()
    return {"message": f"Joined pool '{pool.name}'", **_pool_dict(pool, "member")}


@router.get("/{pool_id}")
async def get_pool(
    pool_id: str,
    claims:  dict = Depends(decode_token),
    db:      AsyncSession = Depends(get_db),
):
    phone = claims["phone"]
    pr    = await db.execute(select(Pool).where(Pool.id == pool_id))
    pool  = pr.scalar_one_or_none()
    if not pool:
        raise HTTPException(status_code=404, detail="Pool not found")
    access = await db.execute(select(PoolMember).where(PoolMember.pool_id == pool_id, PoolMember.user_phone == phone))
    if not access.scalar_one_or_none():
        raise HTTPException(status_code=403, detail="Not a member of this pool")
    mr      = await db.execute(select(PoolMember).where(PoolMember.pool_id == pool_id))
    members = mr.scalars().all()
    return {
        **_pool_dict(pool),
        "members": [
            {"phone": m.user_phone, "role": m.role, "joined_at": m.joined_at.isoformat() if m.joined_at else None}
            for m in members
        ],
    }
