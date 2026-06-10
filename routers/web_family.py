"""
@file web_family.py
@description Family API — manages shared family ledgers, requests, transfers, and public intake links.
The family section is a distinct product surface next to dashboard, pools, and payment-links.
It reuses the existing payment-link checkout rail for contribution intake, while family-specific
requests and transfer records live in their own ledger.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import desc, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.banks import resolve_bank
from core.web_jwt import decode_token
from database.models import FamilyGroup, FamilyMember, FamilyRequest, FamilyTransfer, PaymentLink, Transaction, User
from database.session import get_db
from services.payment_event_logger import log_payment_event

router = APIRouter(prefix="/api/v1/family", tags=["family"])


class CreateFamilyBody(BaseModel):
    name: str
    description: Optional[str] = None


class JoinFamilyBody(BaseModel):
    invite_code: str


class FamilyRequestBody(BaseModel):
    title: str
    amount: float
    due_date: str
    note: Optional[str] = None


class FamilyTransferBody(BaseModel):
    beneficiary_name: str
    beneficiary_phone: Optional[str] = None
    bank_account: str
    bank_code: str
    amount: float
    note: Optional[str] = None
    source_request_id: Optional[str] = None


def _family_dict(family: FamilyGroup, role: str = "member") -> dict:
    return {
        "id": family.id,
        "name": family.name,
        "description": family.description,
        "invite_code": family.invite_code,
        "balance_ngn": family.balance_ngn or 0,
        "total_contributed": family.total_contributed or 0,
        "total_transferred": family.total_transferred or 0,
        "member_count": family.member_count or 0,
        "is_active": family.is_active,
        "created_at": family.created_at.isoformat() if family.created_at else None,
        "role": role,
    }


def _member_dict(member: FamilyMember, user: User | None = None) -> dict:
    return {
        "phone": member.user_phone,
        "name": (user.name if user else None) or member.display_name or member.user_phone,
        "display_name": member.display_name,
        "role": member.role,
        "joined_at": member.joined_at.isoformat() if member.joined_at else None,
    }


def _request_dict(req: FamilyRequest) -> dict:
    return {
        "id": req.id,
        "title": req.title,
        "amount": req.amount,
        "note": req.note,
        "due_date": req.due_date.isoformat() if req.due_date else None,
        "status": req.status,
        "approved_by": req.approved_by,
        "approved_at": req.approved_at.isoformat() if req.approved_at else None,
        "total_collected": req.total_collected or 0,
        "created_at": req.created_at.isoformat() if req.created_at else None,
    }


def _transfer_dict(transfer: FamilyTransfer) -> dict:
    return {
        "id": transfer.id,
        "beneficiary_name": transfer.beneficiary_name,
        "beneficiary_phone": transfer.beneficiary_phone,
        "bank_account": transfer.bank_account,
        "bank_code": transfer.bank_code,
        "bank_name": transfer.bank_name,
        "amount": transfer.amount,
        "note": transfer.note,
        "status": transfer.status,
        "source_request_id": transfer.source_request_id,
        "completed_by": transfer.completed_by,
        "completed_at": transfer.completed_at.isoformat() if transfer.completed_at else None,
        "failure_reason": transfer.failure_reason,
        "created_at": transfer.created_at.isoformat() if transfer.created_at else None,
    }


def _ledger_entry_from_tx(tx: Transaction) -> dict:
    return {
        "type": "contribution",
        "reference": tx.reference,
        "name": tx.payer_name or "",
        "phone": tx.payer_phone or "",
        "amount": tx.net_amount or tx.ngn_amount or tx.amount,
        "status": tx.status,
        "description": tx.payment_description,
        "created_at": tx.created_at.isoformat() if tx.created_at else None,
    }


def _ledger_entry_from_transfer(transfer: FamilyTransfer) -> dict:
    return {
        "type": "transfer",
        "reference": transfer.id,
        "name": transfer.beneficiary_name,
        "phone": transfer.beneficiary_phone or "",
        "amount": transfer.amount,
        "status": transfer.status,
        "description": transfer.note,
        "created_at": transfer.created_at.isoformat() if transfer.created_at else None,
    }


async def _require_family_member(db: AsyncSession, family_id: str, phone: str) -> tuple[FamilyGroup, FamilyMember | None, bool]:
    fam_result = await db.execute(select(FamilyGroup).where(FamilyGroup.id == family_id))
    family = fam_result.scalar_one_or_none()
    if not family or not family.is_active:
        raise HTTPException(status_code=404, detail="Family not found.")
    member_result = await db.execute(select(FamilyMember).where(FamilyMember.family_id == family_id, FamilyMember.user_phone == phone))
    member = member_result.scalar_one_or_none()
    is_admin = family.creator_phone == phone or (member and member.role == "admin")
    if not member and not is_admin:
        raise HTTPException(status_code=403, detail="You are not a member of this family.")
    return family, member, bool(is_admin)


@router.get("")
async def list_families(claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db)):
    phone = claims["phone"]
    memberships = await db.execute(select(FamilyMember).where(FamilyMember.user_phone == phone))
    members = memberships.scalars().all()
    families = []
    for member in members:
        fam_result = await db.execute(select(FamilyGroup).where(FamilyGroup.id == member.family_id))
        family = fam_result.scalar_one_or_none()
        if family and family.is_active:
            families.append(_family_dict(family, member.role))
    creator_result = await db.execute(select(FamilyGroup).where(FamilyGroup.creator_phone == phone, FamilyGroup.is_active == True))
    for family in creator_result.scalars().all():
        if not any(item["id"] == family.id for item in families):
            families.append(_family_dict(family, "admin"))
    families.sort(key=lambda f: f.get("created_at") or "", reverse=True)
    return {"families": families}


@router.post("")
async def create_family(body: CreateFamilyBody, claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db)):
    phone = claims["phone"]
    if not body.name.strip():
        raise HTTPException(status_code=400, detail="Family name is required.")
    family = FamilyGroup(name=body.name.strip(), description=body.description.strip() if body.description else None, creator_phone=phone)
    db.add(family)
    await db.flush()
    db.add(FamilyMember(family_id=family.id, user_phone=phone, display_name=body.name.strip(), role="admin"))
    await db.commit()
    await db.refresh(family)
    return {"family": _family_dict(family, "admin"), "id": family.id}


@router.post("/join")
async def join_family(body: JoinFamilyBody, claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db)):
    phone = claims["phone"]
    code = body.invite_code.strip().rstrip("/").split("/")[-1].upper()
    fam_result = await db.execute(select(FamilyGroup).where(FamilyGroup.invite_code == code))
    family = fam_result.scalar_one_or_none()
    if not family or not family.is_active:
        raise HTTPException(status_code=404, detail="Invalid invite code.")
    if family.creator_phone == phone:
        raise HTTPException(status_code=400, detail="You created this family — you're already the admin.")
    existing = await db.execute(select(FamilyMember).where(FamilyMember.family_id == family.id, FamilyMember.user_phone == phone))
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="You're already a member of this family.")
    db.add(FamilyMember(family_id=family.id, user_phone=phone, role="member"))
    family.member_count = (family.member_count or 0) + 1
    await db.commit()
    return {"message": f"Joined family '{family.name}'", "family_id": family.id, **_family_dict(family, "member")}


@router.get("/{family_id}")
async def get_family(family_id: str, claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db)):
    phone = claims["phone"]
    family, member, is_admin = await _require_family_member(db, family_id, phone)

    members_result = await db.execute(select(FamilyMember).where(FamilyMember.family_id == family_id))
    members = members_result.scalars().all()
    member_details = []
    for m in members:
        user_result = await db.execute(select(User).where(User.phone == m.user_phone))
        member_details.append(_member_dict(m, user_result.scalar_one_or_none()))

    requests_result = await db.execute(select(FamilyRequest).where(FamilyRequest.family_id == family_id).order_by(desc(FamilyRequest.created_at)))
    requests = requests_result.scalars().all()

    transfers_result = await db.execute(select(FamilyTransfer).where(FamilyTransfer.family_id == family_id).order_by(desc(FamilyTransfer.created_at)))
    transfers = transfers_result.scalars().all()

    links_result = await db.execute(select(PaymentLink).where(PaymentLink.family_id == family_id).order_by(desc(PaymentLink.created_at)))
    links = links_result.scalars().all()

    tx_result = await db.execute(
        select(Transaction).where(Transaction.family_id == family_id, Transaction.tx_type == "payment_link").order_by(desc(Transaction.created_at)).limit(50)
    )
    ledger = [_ledger_entry_from_tx(tx) for tx in tx_result.scalars().all()]
    ledger.extend(_ledger_entry_from_transfer(t) for t in transfers)
    ledger.sort(key=lambda item: item.get("created_at") or "", reverse=True)

    return {
        "family": _family_dict(family, member.role if member else ("admin" if is_admin else "member")),
        "is_admin": is_admin,
        "members": member_details,
        "requests": [_request_dict(r) for r in requests],
        "transfers": [_transfer_dict(t) for t in transfers],
        "links": [{
            "id": l.id,
            "code": l.code,
            "title": l.title,
            "description": l.description,
            "amount": l.amount,
            "is_flexible": l.is_flexible,
            "bank_name": l.bank_name,
            "total_collected": l.total_collected,
            "use_count": l.use_count,
            "is_active": l.is_active,
            "expires_at": l.expires_at.isoformat() if l.expires_at else None,
            "created_at": l.created_at.isoformat() if l.created_at else None,
        } for l in links],
        "ledger": ledger,
        "summary": {
            "balance_ngn": family.balance_ngn or 0,
            "total_contributed": family.total_contributed or 0,
            "total_transferred": family.total_transferred or 0,
            "member_count": family.member_count or len(member_details),
            "request_count": len(requests),
            "transfer_count": len(transfers),
            "link_count": len(links),
        },
    }


@router.get("/{family_id}/ledger")
async def get_family_ledger(family_id: str, claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db), page: int = 1, per_page: int = 25):
    phone = claims["phone"]
    await _require_family_member(db, family_id, phone)
    page = max(int(page or 1), 1)
    per_page = min(max(int(per_page or 25), 1), 100)

    tx_result = await db.execute(
        select(Transaction).where(Transaction.family_id == family_id, Transaction.tx_type == "payment_link").order_by(desc(Transaction.created_at))
    )
    transfer_result = await db.execute(select(FamilyTransfer).where(FamilyTransfer.family_id == family_id).order_by(desc(FamilyTransfer.created_at)))
    entries = [_ledger_entry_from_tx(tx) for tx in tx_result.scalars().all()] + [_ledger_entry_from_transfer(t) for t in transfer_result.scalars().all()]
    entries.sort(key=lambda item: item.get("created_at") or "", reverse=True)
    total = len(entries)
    start = (page - 1) * per_page
    end = start + per_page
    return {"ledger": entries[start:end], "total": total, "page": page, "per_page": per_page, "total_pages": (total + per_page - 1) // per_page if per_page else 1}


@router.post("/{family_id}/requests")
async def create_request(family_id: str, body: FamilyRequestBody, claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db)):
    phone = claims["phone"]
    family, member, is_admin = await _require_family_member(db, family_id, phone)
    if not is_admin and not member:
        raise HTTPException(status_code=403, detail="Only family members can create requests.")
    if not body.title.strip():
        raise HTTPException(status_code=400, detail="Request title is required.")
    if body.amount <= 0:
        raise HTTPException(status_code=400, detail="Amount must be greater than zero.")
    try:
        due = datetime.fromisoformat(body.due_date)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid due_date format. Use ISO 8601.")
    req = FamilyRequest(family_id=family_id, requested_by=phone, title=body.title.strip(), amount=body.amount, note=body.note.strip() if body.note else None, due_date=due)
    db.add(req)
    await db.commit()
    await db.refresh(req)
    return {"request": _request_dict(req)}


@router.post("/{family_id}/requests/{request_id}/approve")
async def approve_request(family_id: str, request_id: str, claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db)):
    phone = claims["phone"]
    family, _, is_admin = await _require_family_member(db, family_id, phone)
    if not is_admin:
        raise HTTPException(status_code=403, detail="Only the family admin can approve requests.")
    req_result = await db.execute(select(FamilyRequest).where(FamilyRequest.id == request_id, FamilyRequest.family_id == family_id))
    req = req_result.scalar_one_or_none()
    if not req:
        raise HTTPException(status_code=404, detail="Request not found.")
    req.status = "approved"
    req.approved_by = phone
    req.approved_at = datetime.utcnow()
    await db.commit()
    return {"request": _request_dict(req)}


@router.post("/{family_id}/transfers")
async def create_transfer(family_id: str, body: FamilyTransferBody, claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db)):
    phone = claims["phone"]
    family, _, is_admin = await _require_family_member(db, family_id, phone)
    if not is_admin:
        raise HTTPException(status_code=403, detail="Only the family admin can record transfers.")
    if body.amount <= 0:
        raise HTTPException(status_code=400, detail="Transfer amount must be greater than zero.")
    bank = resolve_bank(body.bank_code)
    if not bank:
        raise HTTPException(status_code=400, detail=f"Invalid bank code: {body.bank_code}")
    transfer = FamilyTransfer(
        family_id=family_id,
        requested_by=phone,
        beneficiary_name=body.beneficiary_name.strip(),
        beneficiary_phone=body.beneficiary_phone.strip() if body.beneficiary_phone else None,
        bank_account=body.bank_account.strip(),
        bank_code=body.bank_code.strip(),
        bank_name=bank["name"],
        amount=body.amount,
        note=body.note.strip() if body.note else None,
        source_request_id=body.source_request_id,
        status="pending",
    )
    db.add(transfer)
    await db.commit()
    await db.refresh(transfer)
    return {"transfer": _transfer_dict(transfer)}


@router.post("/{family_id}/transfers/{transfer_id}/complete")
async def complete_transfer(family_id: str, transfer_id: str, claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db)):
    phone = claims["phone"]
    family, _, is_admin = await _require_family_member(db, family_id, phone)
    if not is_admin:
        raise HTTPException(status_code=403, detail="Only the family admin can complete transfers.")
    transfer_result = await db.execute(select(FamilyTransfer).where(FamilyTransfer.id == transfer_id, FamilyTransfer.family_id == family_id).with_for_update())
    transfer = transfer_result.scalar_one_or_none()
    if not transfer:
        raise HTTPException(status_code=404, detail="Transfer not found.")
    if transfer.status == "completed":
        return {"transfer": _transfer_dict(transfer)}
    amount = round(float(transfer.amount or 0), 2)
    if amount <= 0:
        raise HTTPException(status_code=400, detail="Invalid transfer amount.")
    if (family.balance_ngn or 0) < amount:
        raise HTTPException(status_code=400, detail="Insufficient family balance to complete this transfer.")
    family.balance_ngn = round((family.balance_ngn or 0) - amount, 2)
    family.total_transferred = round((family.total_transferred or 0) + amount, 2)
    transfer.status = "completed"
    transfer.completed_by = phone
    transfer.completed_at = datetime.utcnow()
    await db.commit()
    await db.refresh(transfer)
    await log_payment_event(db, event_type="family.transfer.completed", reference=transfer.id, status="completed", payload={"family_id": family_id, "amount": amount})
    return {"transfer": _transfer_dict(transfer)}
