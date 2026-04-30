import asyncio, uuid
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc
from pydantic import BaseModel
from typing import Optional
from database.session import get_db
from database.models import Pool, PoolMember, FiatPool, FiatPoolMember, PoolTransaction, PaymentRequest, User
from core.web_jwt import decode_token
from core.banks import resolve_bank
from core.payout import best_payout
from services.security_service import verify_pin

router = APIRouter(prefix="/api/v1/pools", tags=["pools"])

FEE_POOL_NGN = 0.003   # 0.3% for fiat pool sends


class CreatePoolBody(BaseModel):
    name:      str
    pool_type: str = "crypto"


class JoinPoolBody(BaseModel):
    invite_code: str


class PoolSendBody(BaseModel):
    amount:          float
    recipient_name:  str
    bank_account:    str
    bank_code:       str
    note:            Optional[str] = None
    pin:             str


class PaymentRequestBody(BaseModel):
    title:    str
    amount:   float
    note:     Optional[str] = None
    due_date: Optional[str] = None   # ISO date string


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
            raise HTTPException(status_code=404, detail="Invalid invite code. Check the code and try again.")
        if fpool.creator_phone == phone:
            raise HTTPException(status_code=400, detail="You created this pool — you're already the admin.")
        ex = await db.execute(select(FiatPoolMember).where(FiatPoolMember.pool_id == fpool.id, FiatPoolMember.user_phone == phone))
        if ex.scalar_one_or_none():
            raise HTTPException(status_code=400, detail="You're already a member of this pool.")
        fpool.member_count = (fpool.member_count or 1) + 1
        db.add(FiatPoolMember(pool_id=fpool.id, user_phone=phone, role="member"))
        await db.commit()
        return {"message": f"Joined fiat pool '{fpool.name}'", "pool_id": fpool.id, "pool_type": "fiat"}

    if pool.creator_phone == phone:
        raise HTTPException(status_code=400, detail="You created this pool — you're already the admin.")
    ex = await db.execute(select(PoolMember).where(PoolMember.pool_id == pool.id, PoolMember.user_phone == phone))
    if ex.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="You're already a member of this pool.")
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

    # Try fiat pool first
    fpr   = await db.execute(select(FiatPool).where(FiatPool.id == pool_id))
    fpool = fpr.scalar_one_or_none()
    if fpool:
        access = await db.execute(select(FiatPoolMember).where(FiatPoolMember.pool_id == pool_id, FiatPoolMember.user_phone == phone))
        if not access.scalar_one_or_none():
            raise HTTPException(status_code=403, detail="Not a member of this pool")
        mr      = await db.execute(select(FiatPoolMember).where(FiatPoolMember.pool_id == pool_id))
        members = mr.scalars().all()
        member_details = []
        for m in members:
            ur = await db.execute(select(User).where(User.phone == m.user_phone))
            u  = ur.scalar_one_or_none()
            member_details.append({
                "phone": m.user_phone, "name": u.name if u else None,
                "role": m.role, "joined_at": m.joined_at.isoformat() if m.joined_at else None,
            })
        return {
            "id": fpool.id, "name": fpool.name, "pool_type": "fiat",
            "invite_code": fpool.invite_code, "member_count": fpool.member_count,
            "total_volume": fpool.total_volume, "is_active": fpool.is_active,
            "created_at": fpool.created_at.isoformat() if fpool.created_at else None,
            "members": member_details,
            "is_admin": any(m["phone"] == phone and m["role"] == "admin" for m in member_details),
        }

    pr   = await db.execute(select(Pool).where(Pool.id == pool_id))
    pool = pr.scalar_one_or_none()
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


# ── Fiat pool — send payment ──────────────────────────────────────────────────

@router.post("/{pool_id}/send")
async def pool_send(
    pool_id: str,
    body:    PoolSendBody,
    claims:  dict = Depends(decode_token),
    db:      AsyncSession = Depends(get_db),
):
    """
    Facilitated NGN payment through a fiat pool.
    Funds are never held — payout fires immediately via Yellow Card / Breet.
    """
    phone = claims["phone"]

    fpr   = await db.execute(select(FiatPool).where(FiatPool.id == pool_id))
    fpool = fpr.scalar_one_or_none()
    if not fpool:
        raise HTTPException(status_code=404, detail="Fiat pool not found.")

    access = await db.execute(select(FiatPoolMember).where(FiatPoolMember.pool_id == pool_id, FiatPoolMember.user_phone == phone))
    if not access.scalar_one_or_none():
        raise HTTPException(status_code=403, detail="You are not a member of this pool.")

    if body.amount < 100:
        raise HTTPException(status_code=400, detail="Minimum send amount is ₦100.")

    bank = resolve_bank(body.bank_code)
    if not bank:
        raise HTTPException(status_code=400, detail=f"Invalid bank code: {body.bank_code}")

    ok = await verify_pin(db, phone, body.pin)
    if not ok:
        raise HTTPException(status_code=401, detail="Incorrect PIN.")

    fee = round(body.amount * FEE_POOL_NGN, 2)
    net = round(body.amount - fee, 2)
    ref = "QRK_PS_" + uuid.uuid4().hex[:10].upper()

    txn = PoolTransaction(
        pool_id=pool_id,
        sender_phone=phone,
        recipient_name=body.recipient_name,
        recipient_bank_account=body.bank_account,
        recipient_bank_code=body.bank_code,
        recipient_bank_name=bank["name"],
        amount=body.amount,
        fee=fee,
        net_amount=net,
        status="processing",
        reference=ref,
        note=body.note,
    )
    db.add(txn)

    fpool.total_volume = (fpool.total_volume or 0) + body.amount
    fpool.total_fees   = (fpool.total_fees or 0) + fee
    await db.commit()
    await db.refresh(txn)

    bank_dict = {"account_number": body.bank_account, "bank_code": body.bank_code}
    asyncio.create_task(_fire_pool_payout(txn.id, phone, net, bank_dict, ref))

    return {
        "message": f"Payment of ₦{body.amount:,.2f} to {body.recipient_name} is processing.",
        "reference": ref, "fee": fee, "net": net,
        "recipient_bank": bank["name"],
    }


async def _fire_pool_payout(txn_id: str, phone: str, net: float, bank: dict, ref: str):
    from database.session import AsyncSessionLocal
    try:
        result = await best_payout(phone, net, bank, ref)
        async with AsyncSessionLocal() as db:
            r   = await db.execute(select(PoolTransaction).where(PoolTransaction.id == txn_id))
            txn = r.scalar_one_or_none()
            if txn:
                txn.status   = "completed"
                txn.provider = result.get("provider")
                await db.commit()
    except Exception as e:
        from database.session import AsyncSessionLocal
        async with AsyncSessionLocal() as db:
            r   = await db.execute(select(PoolTransaction).where(PoolTransaction.id == txn_id))
            txn = r.scalar_one_or_none()
            if txn:
                txn.status = "failed"
                await db.commit()


# ── Fiat pool — activity feed ─────────────────────────────────────────────────

@router.get("/{pool_id}/activity")
async def pool_activity(
    pool_id: str,
    page:    int = 1,
    claims:  dict = Depends(decode_token),
    db:      AsyncSession = Depends(get_db),
):
    phone = claims["phone"]
    access = await db.execute(
        select(FiatPoolMember).where(FiatPoolMember.pool_id == pool_id, FiatPoolMember.user_phone == phone)
    )
    if not access.scalar_one_or_none():
        raise HTTPException(status_code=403, detail="Not a member of this pool.")

    offset = (page - 1) * 20
    r      = await db.execute(
        select(PoolTransaction)
        .where(PoolTransaction.pool_id == pool_id)
        .order_by(desc(PoolTransaction.created_at))
        .offset(offset).limit(20)
    )
    txns = r.scalars().all()

    items = []
    for t in txns:
        ur = await db.execute(select(User).where(User.phone == t.sender_phone))
        u  = ur.scalar_one_or_none()
        items.append({
            "id": t.id, "sender_name": u.name if u else t.sender_phone,
            "sender_phone": t.sender_phone,
            "recipient_name": t.recipient_name,
            "recipient_bank": t.recipient_bank_name,
            "amount": t.amount, "fee": t.fee, "net_amount": t.net_amount,
            "status": t.status, "reference": t.reference, "note": t.note,
            "created_at": t.created_at.isoformat() if t.created_at else None,
        })

    return {"activity": items, "page": page, "has_more": len(txns) == 20}


# ── Fiat pool — payment requests ──────────────────────────────────────────────

@router.post("/{pool_id}/requests")
async def create_request(
    pool_id: str,
    body:    PaymentRequestBody,
    claims:  dict = Depends(decode_token),
    db:      AsyncSession = Depends(get_db),
):
    phone = claims["phone"]
    access = await db.execute(
        select(FiatPoolMember).where(FiatPoolMember.pool_id == pool_id, FiatPoolMember.user_phone == phone)
    )
    mem = access.scalar_one_or_none()
    if not mem:
        raise HTTPException(status_code=403, detail="Not a member of this pool.")
    if mem.role != "admin":
        raise HTTPException(status_code=403, detail="Only pool admins can create payment requests.")

    due = None
    if body.due_date:
        try:
            due = datetime.fromisoformat(body.due_date)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid due_date format. Use ISO 8601.")

    req = PaymentRequest(
        pool_id=pool_id, requested_by=phone,
        title=body.title, amount=body.amount,
        note=body.note, due_date=due,
    )
    db.add(req)
    await db.commit()
    await db.refresh(req)

    return {
        "id": req.id, "title": req.title, "amount": req.amount,
        "note": req.note, "status": req.status,
        "due_date": req.due_date.isoformat() if req.due_date else None,
        "created_at": req.created_at.isoformat() if req.created_at else None,
    }


@router.get("/{pool_id}/requests")
async def list_requests(
    pool_id: str,
    claims:  dict = Depends(decode_token),
    db:      AsyncSession = Depends(get_db),
):
    phone = claims["phone"]
    access = await db.execute(
        select(FiatPoolMember).where(FiatPoolMember.pool_id == pool_id, FiatPoolMember.user_phone == phone)
    )
    if not access.scalar_one_or_none():
        raise HTTPException(status_code=403, detail="Not a member of this pool.")

    r    = await db.execute(
        select(PaymentRequest)
        .where(PaymentRequest.pool_id == pool_id, PaymentRequest.status == "active")
        .order_by(desc(PaymentRequest.created_at))
    )
    reqs = r.scalars().all()
    return {
        "requests": [
            {
                "id": req.id, "title": req.title, "amount": req.amount,
                "note": req.note, "status": req.status,
                "paid_count": req.paid_count, "total_collected": req.total_collected,
                "due_date": req.due_date.isoformat() if req.due_date else None,
                "created_at": req.created_at.isoformat() if req.created_at else None,
            }
            for req in reqs
        ]
    }
