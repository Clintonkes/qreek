"""
@file web_saved_cards.py
@description Account-level management of a Qreek user's saved cards — list,
remove, set default. Cards are only ever created via a successful, verified
Flutterwave charge (see finalize_flutterwave_link_payment's save_card_for_phone
path and the charge-saved-card flow in web_payment_links.py); this router never
accepts raw card data, only manages the resulting tokens.
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from database.session import get_db
from database.models import SavedCard
from core.web_jwt import decode_token

router = APIRouter(prefix="/api/v1/cards", tags=["cards"])


def _card_dict(c: SavedCard) -> dict:
    return {
        "id": c.id,
        "brand": c.card_brand,
        "last4": c.last4,
        "exp_month": c.exp_month,
        "exp_year": c.exp_year,
        "bank": c.bank,
        "is_default": c.is_default,
        "created_at": c.created_at.isoformat() if c.created_at else None,
    }


@router.get("")
async def list_saved_cards(claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(SavedCard).where(SavedCard.owner_phone == claims["phone"]).order_by(SavedCard.created_at.desc())
    )
    return {"cards": [_card_dict(c) for c in result.scalars().all()]}


@router.delete("/{card_id}")
async def delete_saved_card(card_id: str, claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(SavedCard).where(SavedCard.id == card_id, SavedCard.owner_phone == claims["phone"]))
    card = result.scalar_one_or_none()
    if not card:
        raise HTTPException(status_code=404, detail="Saved card not found.")
    was_default = card.is_default
    await db.delete(card)
    await db.flush()

    if was_default:
        remaining_result = await db.execute(
            select(SavedCard).where(SavedCard.owner_phone == claims["phone"]).order_by(SavedCard.created_at.desc())
        )
        remaining = remaining_result.scalar_one_or_none()
        if remaining:
            remaining.is_default = True

    await db.commit()
    return {"message": "Card removed."}


@router.put("/{card_id}/default")
async def set_default_card(card_id: str, claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(SavedCard).where(SavedCard.owner_phone == claims["phone"]))
    cards = result.scalars().all()
    found = False
    for c in cards:
        if c.id == card_id:
            c.is_default = True
            found = True
        else:
            c.is_default = False
    if not found:
        raise HTTPException(status_code=404, detail="Saved card not found.")
    await db.commit()
    return {"message": "Default card updated."}
