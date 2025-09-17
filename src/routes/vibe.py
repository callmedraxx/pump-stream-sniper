import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ..services.vibe_station import prepare_purchase_tx

logger = logging.getLogger(__name__)
router = APIRouter()


class PreparePurchaseRequest(BaseModel):
    mint: str = Field(..., description="Mint address to purchase")
    amountSOL: float = Field(..., gt=0, description="Amount in SOL to spend")


@router.post("/vibe/prepare-purchase")
async def prepare_purchase(payload: PreparePurchaseRequest):
    """Prepare a purchase transaction for buying `mint` with given SOL amount.

    Returns a serialized transaction as base64 that the client can sign/send.
    """
    try:
        mint = payload.mint
        amount_sol = float(payload.amountSOL)
    except Exception as e:
        raise HTTPException(status_code=400, detail={"error": f"invalid request body: {e}"})

    try:
        result = await prepare_purchase_tx(mint, amount_sol)
    except NotImplementedError as e:
        logger.exception("prepare_purchase not implemented: %s", e)
        raise HTTPException(status_code=501, detail={"error": str(e)})
    except ValueError as e:
        raise HTTPException(status_code=400, detail={"error": str(e)})
    except Exception as e:
        logger.exception("prepare_purchase failed: %s", e)
        raise HTTPException(status_code=500, detail={"error": "internal server error"})

    # result should be a dict with txBase64 and optional explorerUrl
    if not isinstance(result, dict) or "txBase64" not in result:
        logger.error("unexpected prepare_purchase result: %s", result)
        raise HTTPException(status_code=500, detail={"error": "invalid backend response"})

    return result
