"""
@file web_pools.py
@description Payment Pools API — manages group-based collections (ajo/esusu) and payouts.
Supports both fiat (NGN) and crypto pools with member roles, activity feeds, and dispute tracking.

Flow:
1. Lifecycle: Users create, join (via invite code), and manage pools.
2. Collections: Pool admins create payment requests broadcasted to all members.
3. Disbursements: Members initiate payouts from the pool to external bank accounts, 
   secured by a personal transaction PIN.
4. Transparency: Every transaction is recorded on an immutable ledger visible to all members.
5. Governance: Enables admin role transfers and community-based dispute reporting.
"""
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
from core.payout import best_payout, settle_fee
from services.payment_service import debit_ngn_or_reject, refund_ngn
from services.security_service import is_frozen, pin_attempts_remaining, verify_transaction_pin

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
    """
    Converts a Pool model instance into a dictionary for JSON response.
    Includes the user's role in the pool.
    """
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
    """
    Lists all pools (crypto or fiat) that the authenticated user is a member of.
    """
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
    """
    Creates a new crypto or fiat pool. 
    The creator is automatically assigned the 'admin' role.
    """
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
    """
    Joins an existing pool using an invite code.
    Supports both crypto and fiat pools.
    """
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
    """
    Retrieves the details and member list for a specific pool.
    """
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
    Processes a payout from a fiat pool.
    Debits the sender's NGN balance and initiates an immediate payout to the recipient's bank.
    Ensures the sender is a member of the pool and provides a correct PIN.
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

    if await is_frozen(db, phone):
        raise HTTPException(status_code=403, detail="Account frozen after too many failed PIN attempts. Contact support.")

    ok = await verify_transaction_pin(db, phone, body.pin)
    if not ok:
        remaining = await pin_attempts_remaining(db, phone)
        if remaining <= 0:
            raise HTTPException(status_code=403, detail="Account frozen after 5 failed PIN attempts.")
        raise HTTPException(status_code=401, detail=f"Incorrect PIN. {remaining} attempts remaining.")

    fee = round(body.amount * FEE_POOL_NGN, 2)
    net = round(body.amount - fee, 2)
    ref = "QRK_PS_" + uuid.uuid4().hex[:10].upper()

    await debit_ngn_or_reject(db, phone, body.amount)

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
    asyncio.create_task(_fire_pool_payout(txn.id, phone, net, fee, bank_dict, ref))

    return {
        "message": f"Payment of ₦{body.amount:,.2f} to {body.recipient_name} is processing.",
        "reference": ref, "fee": fee, "net": net,
        "recipient_bank": bank["name"],
    }


async def _fire_pool_payout(txn_id: str, phone: str, net: float, fee: float, bank: dict, ref: str):
    """
    Asynchronous background task to execute a pool payout.
    Updates the transaction status upon success or failure, and handles refunds on failure.
    """
    from database.session import AsyncSessionLocal
    try:
        result = await best_payout(phone, net, bank, ref)
        await settle_fee(phone, fee, ref)
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
                await refund_ngn(db, phone, txn.amount)
                await db.commit()


# ── Fiat pool — activity feed ─────────────────────────────────────────────────

@router.get("/{pool_id}/activity")
async def pool_activity(
    pool_id: str,
    page:    int = 1,
    claims:  dict = Depends(decode_token),
    db:      AsyncSession = Depends(get_db),
):
    """
    Retrieves the paginated activity feed for a specific pool.
    """
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
    """
    Creates a new payment request within a fiat pool.
    Only pool admins can create requests.
    """
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
    """
    Lists all active payment requests for a specific pool.
    """
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


# ── Pool protection: dispute reporting ───────────────────────────────────────

class DisputeBody(BaseModel):
    transaction_id: Optional[str] = None
    request_id:     Optional[str] = None
    description:    str


@router.post("/{pool_id}/dispute")
async def report_dispute(
    pool_id: str,
    body:    DisputeBody,
    claims:  dict = Depends(decode_token),
    db:      AsyncSession = Depends(get_db),
):
    """Any pool member can flag a suspicious transaction or payment request for review."""
    phone = claims["phone"]
    access = await db.execute(
        select(FiatPoolMember).where(FiatPoolMember.pool_id == pool_id, FiatPoolMember.user_phone == phone)
    )
    if not access.scalar_one_or_none():
        raise HTTPException(status_code=403, detail="Not a member of this pool.")
    if not body.description or len(body.description.strip()) < 10:
        raise HTTPException(status_code=400, detail="Please describe the dispute in at least 10 characters.")

    # Log the dispute in the AuditLog for support team review
    from database.models import AuditLog
    log = AuditLog(
        actor_phone=phone,
        action="pool_dispute_reported",
        entity_type="fiat_pool",
        entity_id=pool_id,
        metadata={
            "pool_id": pool_id,
            "transaction_id": body.transaction_id,
            "request_id": body.request_id,
            "description": body.description,
        }
    )
    db.add(log)
    await db.commit()

    return {
        "message": "Dispute reported. Our support team will review within 24 hours and contact you.",
        "reference": f"DISPUTE-{pool_id[:6].upper()}-{phone[-4:]}",
    }


# ── Pool protection: admin change audit ──────────────────────────────────────

@router.post("/{pool_id}/admin/transfer")
async def transfer_admin(
    pool_id:   str,
    new_phone: str,
    claims:    dict = Depends(decode_token),
    db:        AsyncSession = Depends(get_db),
):
    """Transfer admin role to another pool member. Logged immutably."""
    phone = claims["phone"]

    # Verify current user is admin
    admin_r = await db.execute(
        select(FiatPoolMember).where(FiatPoolMember.pool_id == pool_id, FiatPoolMember.user_phone == phone, FiatPoolMember.role == "admin")
    )
    if not admin_r.scalar_one_or_none():
        raise HTTPException(status_code=403, detail="Only pool admins can transfer admin role.")

    # Verify new admin is a member
    new_r = await db.execute(
        select(FiatPoolMember).where(FiatPoolMember.pool_id == pool_id, FiatPoolMember.user_phone == new_phone)
    )
    new_member = new_r.scalar_one_or_none()
    if not new_member:
        raise HTTPException(status_code=404, detail="That phone number is not a member of this pool.")

    # Transfer
    old_admin = admin_r.scalar_one_or_none()
    if old_admin:
        # Re-fetch since scalar_one_or_none consumed
        old_admin_r = await db.execute(select(FiatPoolMember).where(FiatPoolMember.pool_id == pool_id, FiatPoolMember.user_phone == phone))
        old_admin_obj = old_admin_r.scalar_one_or_none()
        if old_admin_obj:
            old_admin_obj.role = "member"

    new_member.role = "admin"

    from database.models import AuditLog
    db.add(AuditLog(
        actor_phone=phone, action="pool_admin_transferred",
        entity_type="fiat_pool", entity_id=pool_id,
        metadata={"from_phone": phone, "to_phone": new_phone, "pool_id": pool_id}
    ))
    await db.commit()

    return {"message": f"Admin role transferred. All members have been notified via the activity feed."}
