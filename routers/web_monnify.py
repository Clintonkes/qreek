from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from database.session import get_db
from database.models import User, Transaction
from core.web_jwt import decode_token
from services.monnify_service import reserve_virtual_account, verify_webhook_signature, logger
import traceback

router = APIRouter(prefix="/api/v1/monnify", tags=["monnify"])


@router.post("/reserve-account")
async def request_virtual_account(claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db)):
    """
    Generates a Monnify virtual account for the logged-in user and saves it.
    """
    phone = claims["phone"]
    
    result = await db.execute(select(User).where(User.phone == phone).with_for_update())
    user = result.scalar_one_or_none()
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
        
    # If the user already has a bank account set up, return it
    if user.bank_account and user.bank_code:
        return {
            "account_number": user.bank_account,
            "bank_name": user.bank_name,
            "bank_code": user.bank_code
        }
        
    try:
        # User email is not strictly enforced in the Qreek User model, so use a placeholder if empty
        email = f"{phone}@qreekfinance.org" 
        name = user.name or f"User {phone}"
        
        account_data = await reserve_virtual_account(phone, name, email)
        
        user.bank_account = account_data["account_number"]
        user.bank_code = account_data["bank_code"]
        user.bank_name = account_data["bank_name"]
        
        await db.commit()
        
        return {
            "account_number": user.bank_account,
            "bank_name": user.bank_name,
            "bank_code": user.bank_code,
            "message": "Virtual account successfully reserved"
        }
    except Exception as e:
        logger.error(f"Error reserving account for {phone}: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail="Could not reserve virtual account at this time.")


@router.post("/webhook")
async def monnify_webhook(request: Request, db: AsyncSession = Depends(get_db)):
    """
    Handles incoming Monnify webhooks for successful transactions.
    """
    try:
        # Get raw body for signature verification
        payload_bytes = await request.body()
        monnify_signature = request.headers.get("monnify-signature")
        
        if not monnify_signature:
            logger.warning("Webhook received without monnify-signature header.")
            return Response(status_code=400, content="Missing signature")
            
        if not verify_webhook_signature(payload_bytes, monnify_signature):
            logger.warning("Webhook signature verification failed.")
            return Response(status_code=401, content="Invalid signature")
            
        # Parse JSON
        payload = await request.json()
        event_type = payload.get("eventType")
        event_data = payload.get("eventData", {})
        
        logger.info(f"Monnify Webhook received: {event_type} - {event_data.get('transactionReference')}")
        
        if event_type == "SUCCESSFUL_TRANSACTION":
            product = event_data.get("product", {})
            if product.get("type") == "RESERVED_ACCOUNT":
                reference = product.get("reference", "")
                
                # Our reference is QREEK-<phone>
                if reference.startswith("QREEK-"):
                    phone = reference.replace("QREEK-", "")
                    amount = float(event_data.get("settlementAmount", event_data.get("amountPaid", 0)))
                    tx_reference = event_data.get("transactionReference")
                    
                    logger.info(f"Processing deposit of {amount} NGN for {phone}")
                    
                    # Check if transaction already exists to avoid double crediting
                    tx_check = await db.execute(select(Transaction).where(Transaction.reference == tx_reference))
                    if tx_check.scalar_one_or_none():
                        logger.info(f"Transaction {tx_reference} already processed. Skipping.")
                        return Response(status_code=200, content="OK")
                    
                    # Lock user record
                    result = await db.execute(select(User).where(User.phone == phone).with_for_update())
                    user = result.scalar_one_or_none()
                    
                    if user:
                        # Credit user
                        user.balance_ngn = round((user.balance_ngn or 0) + amount, 2)
                        
                        # Create transaction record
                        tx = Transaction(
                            user_phone=phone,
                            tx_type="deposit",
                            currency="NGN",
                            amount=amount,
                            ngn_amount=amount,
                            fee=float(event_data.get("amountPaid", amount)) - amount,
                            status="completed",
                            provider="monnify",
                            reference=tx_reference
                        )
                        db.add(tx)
                        await db.commit()
                        logger.info(f"Successfully credited {amount} NGN to {phone}.")
                    else:
                        logger.error(f"User {phone} not found for Monnify deposit {tx_reference}")
                        await db.rollback()
        
        return Response(status_code=200, content="OK")
        
    except Exception as e:
        logger.error(f"Error processing webhook: {e}")
        logger.error(traceback.format_exc())
        return Response(status_code=500, content="Internal Server Error")
