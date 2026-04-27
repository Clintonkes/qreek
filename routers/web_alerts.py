from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from pydantic import BaseModel
from database.session import get_db
from database.models import PriceAlert
from core.web_jwt import decode_token
from core.rate_engine import get_rate

router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])

SUPPORTED = {"USDT", "BTC", "ETH", "BNB", "SOL", "USDC"}


class CreateAlertBody(BaseModel):
    currency:     str
    target_price: float
    direction:    str | None = None


@router.get("")
async def list_alerts(claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db)):
    phone  = claims["phone"]
    result = await db.execute(
        select(PriceAlert).where(PriceAlert.user_phone == phone, PriceAlert.triggered == False)
    )
    alerts = result.scalars().all()
    return {
        "alerts": [
            {
                "id":           a.id,
                "currency":     a.currency,
                "target_price": a.target_price,
                "direction":    a.direction,
                "triggered":    a.triggered,
                "created_at":   a.created_at.isoformat() if a.created_at else None,
            }
            for a in alerts
        ]
    }


@router.post("")
async def create_alert(
    body:   CreateAlertBody,
    claims: dict = Depends(decode_token),
    db:     AsyncSession = Depends(get_db),
):
    phone    = claims["phone"]
    currency = body.currency.upper()

    if currency not in SUPPORTED:
        raise HTTPException(status_code=400, detail=f"Unsupported currency. Choose from: {', '.join(SUPPORTED)}")

    direction = body.direction
    if not direction:
        current   = await get_rate(currency)
        direction = "above" if body.target_price > current else "below"

    alert = PriceAlert(
        user_phone=phone, currency=currency,
        target_price=body.target_price, direction=direction,
    )
    db.add(alert)
    await db.commit()
    return {
        "id":           alert.id,
        "currency":     alert.currency,
        "target_price": alert.target_price,
        "direction":    alert.direction,
        "message":      f"Alert set: notify when {currency} goes {direction} ₦{body.target_price:,.0f}",
    }


@router.delete("/{alert_id}")
async def delete_alert(
    alert_id: str,
    claims:   dict = Depends(decode_token),
    db:       AsyncSession = Depends(get_db),
):
    phone  = claims["phone"]
    result = await db.execute(
        select(PriceAlert).where(PriceAlert.id == alert_id, PriceAlert.user_phone == phone)
    )
    alert = result.scalar_one_or_none()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    await db.delete(alert)
    await db.commit()
    return {"message": "Alert deleted"}
