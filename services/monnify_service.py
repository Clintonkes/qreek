import os
import base64
import httpx
import hmac
import hashlib
import logging

# Configure basic logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# Avoid duplicating handlers if already added
if not logger.handlers:
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

MONNIFY_API_KEY = os.getenv("MONNIFY_API_KEY")
MONNIFY_SECRET_KEY = os.getenv("MONNIFY_SECRET_KEY")
MONNIFY_CONTRACT_CODE = os.getenv("MONNIFY_CONTRACT_CODE")
MONNIFY_BASE_URL = os.getenv("MONNIFY_BASE_URL", "https://sandbox.monnify.com/api/v1")


class MonnifyConfigError(Exception):
    pass


def _get_client():
    return httpx.AsyncClient(timeout=15.0)


async def generate_token() -> str:
    """
    Authenticates with Monnify and returns an access token.
    """
    if not MONNIFY_API_KEY or not MONNIFY_SECRET_KEY:
        logger.error("Monnify API Key or Secret Key is not configured.")
        raise MonnifyConfigError("Monnify credentials missing in environment.")

    credentials = f"{MONNIFY_API_KEY}:{MONNIFY_SECRET_KEY}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()

    headers = {
        "Authorization": f"Basic {encoded_credentials}",
        "Content-Type": "application/json"
    }

    url = f"{MONNIFY_BASE_URL}/auth/login"
    logger.info(f"Generating Monnify token at {url}")

    async with _get_client() as client:
        try:
            response = await client.post(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            if data.get("requestSuccessful"):
                return data["responseBody"]["accessToken"]
            else:
                logger.error(f"Monnify token generation failed: {data.get('responseMessage')}")
                raise Exception(f"Monnify token generation failed: {data.get('responseMessage')}")
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error during token generation: {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Error generating Monnify token: {e}")
            raise


async def reserve_virtual_account(user_phone: str, user_name: str, user_email: str) -> dict:
    """
    Reserves a dedicated virtual account on Monnify for the user.
    """
    if not MONNIFY_CONTRACT_CODE:
        logger.error("MONNIFY_CONTRACT_CODE is not configured.")
        raise MonnifyConfigError("Monnify contract code missing.")

    try:
        token = await generate_token()
    except Exception as e:
        logger.error("Cannot reserve account because token generation failed.")
        raise e

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Reference should be unique, we use the user's phone
    account_reference = f"QREEK-{user_phone}"

    # If user doesn't have email/name, provide fallbacks
    safe_name = user_name if user_name else f"User {user_phone}"
    safe_email = user_email if user_email else f"{user_phone}@qreekfinance.org"

    payload = {
        "accountReference": account_reference,
        "accountName": safe_name,
        "currencyCode": "NGN",
        "contractCode": MONNIFY_CONTRACT_CODE,
        "customerEmail": safe_email,
        "customerName": safe_name,
        "getAllAvailableBanks": True
    }

    url = f"{MONNIFY_BASE_URL}/bank-transfer/reserved-accounts"
    logger.info(f"Reserving Monnify virtual account for {user_phone} with reference {account_reference}")

    async with _get_client() as client:
        try:
            response = await client.post(url, headers=headers, json=payload)
            data = response.json()

            # 200 OK could be either successful creation or already created (Monnify treats existing reference as success and returns it)
            if response.status_code == 200 and data.get("requestSuccessful"):
                logger.info(f"Successfully reserved account for {user_phone}")
                # We can grab the first bank account provided
                accounts = data["responseBody"].get("accounts", [])
                if not accounts:
                    logger.warning(f"No accounts returned in response for {user_phone}")
                    return {"account_number": "N/A", "bank_name": "Monnify", "bank_code": ""}
                
                primary_account = accounts[0]
                return {
                    "account_number": primary_account.get("accountNumber"),
                    "bank_name": primary_account.get("bankName"),
                    "bank_code": primary_account.get("bankCode"),
                    "reference": account_reference
                }
            else:
                logger.error(f"Failed to reserve account for {user_phone}: {data.get('responseMessage')}")
                raise Exception(f"Monnify error: {data.get('responseMessage')}")
                
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error reserving account: {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Exception during account reservation for {user_phone}: {e}")
            raise


def verify_webhook_signature(payload_bytes: bytes, monnify_signature: str) -> bool:
    """
    Verifies that the webhook payload came from Monnify by comparing the HMAC SHA512 signature.
    """
    if not MONNIFY_SECRET_KEY:
        logger.error("MONNIFY_SECRET_KEY missing, cannot verify signature.")
        return False
        
    try:
        calculated_hash = hmac.new(
            MONNIFY_SECRET_KEY.encode('utf-8'),
            payload_bytes,
            hashlib.sha512
        ).hexdigest()
        
        is_valid = calculated_hash == monnify_signature
        if not is_valid:
            logger.warning(f"Webhook signature mismatch! Expected: {calculated_hash}, Got: {monnify_signature}")
        return is_valid
    except Exception as e:
        logger.error(f"Error validating webhook signature: {e}")
        return False
