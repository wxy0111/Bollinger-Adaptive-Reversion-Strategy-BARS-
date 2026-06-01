"""Small async wrapper around the OKX REST API.

The strategy uses this client for market data, account state, order placement,
order cancellation, position close, and internal account transfers.
"""
import hmac
import base64
import hashlib
import json
from datetime import datetime, timezone
from typing import Optional

import aiohttp
from loguru import logger

from src.config import API_KEY, SECRET_KEY, PASSPHRASE, FLAG
from src.logging_utils import log_action

BASE_URL = "https://www.okx.com"


def _is_benign_cancel_error(err: Exception) -> bool:
    """Return whether a cancel failure is likely an already-closed order."""
    text = str(err).lower()
    benign_markers = (
        "all operations failed",
        "order does not exist",
        "order not exist",
        "already canceled",
        "already cancelled",
        "already filled",
        "already closed",
        "not found",
        "51603",
        "51604",
    )
    return any(marker in text for marker in benign_markers)


def _sign(timestamp: str, method: str, path: str, body: str = "") -> str:
    """Build an OKX API HMAC signature."""
    msg = timestamp + method.upper() + path + body
    return base64.b64encode(
        hmac.new(SECRET_KEY.encode(), msg.encode(), hashlib.sha256).digest()
    ).decode()


def _headers(method: str, path: str, body: str = "") -> dict:
    """Build authenticated OKX request headers."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return {
        "OK-ACCESS-KEY":        API_KEY,
        "OK-ACCESS-SIGN":       _sign(ts, method, path, body),
        "OK-ACCESS-TIMESTAMP":  ts,
        "OK-ACCESS-PASSPHRASE": PASSPHRASE,
        "x-simulated-trading":  FLAG,
        "Content-Type":         "application/json",
    }


class OKXClient:
    """Async OKX REST client.

    Args:
        session: Shared ``aiohttp.ClientSession`` used for all HTTP requests.
    """

    def __init__(self, session: aiohttp.ClientSession):
        self._s = session

    async def _get(self, path: str, params: Optional[dict] = None) -> dict:
        """Send an authenticated GET request and validate OKX response code."""
        qs = ("?" + "&".join(f"{k}={v}" for k, v in params.items())) if params else ""
        full_path = path + qs
        headers = _headers("GET", full_path)
        async with self._s.get(BASE_URL + full_path, headers=headers) as r:
            data = await r.json()
        if data.get("code") != "0":
            raise RuntimeError(f"GET {path} -> {data.get('code')} {data.get('msg')}")
        return data

    async def _post(self, path: str, payload) -> dict:
        """Send an authenticated POST request and validate OKX response code."""
        body = json.dumps(payload)
        headers = _headers("POST", path, body)
        async with self._s.post(BASE_URL + path, headers=headers, data=body) as r:
            data = await r.json()
        if data.get("code") != "0":
            raise RuntimeError(f"POST {path} -> {data.get('code')} {data.get('msg')}")
        return data

    async def get_klines(self, inst_id: str, bar: str, limit: int = 200) -> list:
        """Return recent candles for an instrument."""
        data = await self._get("/api/v5/market/candles",
                               {"instId": inst_id, "bar": bar, "limit": limit})
        return data["data"]

    async def get_mark_price(self, inst_id: str) -> float:
        """Return the current mark price for a swap instrument."""
        data = await self._get("/api/v5/public/mark-price",
                               {"instId": inst_id, "instType": "SWAP"})
        return float(data["data"][0]["markPx"])

    async def get_balance(self, ccy: str = "USDT") -> float:
        """Return available trading-account balance for a currency."""
        data = await self._get("/api/v5/account/balance", {"ccy": ccy})
        for d in data["data"][0]["details"]:
            if d["ccy"] == ccy:
                return float(d["availBal"])
        return 0.0

    async def get_equity(self, ccy: str = "USDT") -> float:
        """Return account equity for a currency when available."""
        data = await self._get("/api/v5/account/balance", {"ccy": ccy})
        for d in data["data"][0]["details"]:
            if d["ccy"] == ccy:
                return float(d.get("eq") or d.get("cashBal") or 0)
        return float(data["data"][0].get("totalEq") or 0)

    async def get_position(self, inst_id: str) -> Optional[dict]:
        """Return the first non-zero position for an instrument."""
        data = await self._get("/api/v5/account/positions", {"instId": inst_id})
        positions = [p for p in data["data"] if float(p.get("pos", 0)) != 0]
        return positions[0] if positions else None

    async def set_leverage(self, inst_id: str, lever: int, mgn_mode: str = "cross") -> None:
        """Set leverage for the instrument."""
        await self._post("/api/v5/account/set-leverage",
                         {"instId": inst_id, "lever": str(lever), "mgnMode": mgn_mode})
        logger.info(f"杠杆设置 {lever}x")

    async def get_funding_balance(self, ccy: str = "USDT") -> float:
        """Return available funding-account balance for a currency."""
        data = await self._get("/api/v5/asset/balances", {"ccy": ccy})
        for d in data["data"]:
            if d["ccy"] == ccy:
                return float(d.get("availBal") or 0)
        return 0.0

    async def transfer(self, amt: float, from_acct: str, to_acct: str, ccy: str = "USDT") -> None:
        """Transfer funds between OKX accounts.

        Args:
            amt: Transfer amount.
            from_acct: OKX account id. ``"6"`` is funding, ``"18"`` is trading.
            to_acct: OKX account id. ``"6"`` is funding, ``"18"`` is trading.
            ccy: Currency code.
        """
        payload = {"ccy": ccy, "amt": f"{amt:.4f}", "from": from_acct, "to": to_acct, "type": "0"}
        await self._post("/api/v5/asset/transfer", payload)
        direction = "交易->资金" if from_acct == "18" else "资金->交易"
        log_action(f"划转 {direction}  {amt:.4f} {ccy}")

    async def place_order(self, inst_id: str, side: str, pos_side: str, sz: str,
                          ord_type: str = "market", px: Optional[str] = None,
                          tp_px: Optional[str] = None, sl_px: Optional[str] = None,
                          reduce_only: bool = False) -> dict:
        """Place a normal OKX order.

        Args:
            inst_id: Instrument id.
            side: Order side, ``"buy"`` or ``"sell"``.
            pos_side: Position side, ``"long"`` or ``"short"``.
            sz: Order size in contracts.
            ord_type: OKX order type.
            px: Optional limit price.
            tp_px: Optional attached take-profit trigger price.
            sl_px: Optional attached stop-loss trigger price.
            reduce_only: Whether the order should only reduce position size.

        Returns:
            OKX order result object.
        """
        payload: dict = {
            "instId": inst_id, "tdMode": "cross",
            "side": side, "posSide": pos_side,
            "ordType": ord_type, "sz": sz,
        }
        if px:
            payload["px"] = px
        if reduce_only:
            payload["reduceOnly"] = "true"
        if tp_px:
            payload.update({"tpTriggerPx": tp_px, "tpOrdPx": "-1", "tpTriggerPxType": "mark"})
        if sl_px:
            payload.update({"slTriggerPx": sl_px, "slOrdPx": "-1", "slTriggerPxType": "mark"})
        result = await self._post("/api/v5/trade/order", payload)
        log_action(f"下单 {side}/{pos_side} sz={sz} px={px or 'market'}")
        return result["data"][0]

    async def get_order(self, inst_id: str, ord_id: str) -> dict:
        """Return one order by OKX order id."""
        data = await self._get("/api/v5/trade/order", {"instId": inst_id, "ordId": ord_id})
        return data["data"][0]

    async def get_open_orders(self, inst_id: str) -> list:
        """Return open normal orders for an instrument."""
        data = await self._get("/api/v5/trade/orders-pending", {"instId": inst_id})
        return data["data"]

    async def get_open_algo_orders(self, inst_id: str) -> list:
        """Return open conditional algo orders for an instrument."""
        data = await self._get("/api/v5/trade/orders-algo-pending",
                               {"instId": inst_id, "ordType": "conditional"})
        return data["data"]

    async def cancel_order(self, inst_id: str, ord_id: str) -> None:
        """Cancel a normal OKX order, ignoring already-closed failures."""
        try:
            await self._post("/api/v5/trade/cancel-order", {"instId": inst_id, "ordId": ord_id})
            log_action(f"撤单 ordId={ord_id}")
        except Exception as e:
            if _is_benign_cancel_error(e):
                log_action(f"撤单跳过，订单可能已成交/已撤/不存在 ordId={ord_id}: {e}")
                return
            logger.warning(f"撤单失败(可能已成交/不存在): {e}")

    async def place_algo_order(self, inst_id: str, side: str, pos_side: str, sz: str,
                               sl_trigger_px: str, sl_trigger_px_type: str = "mark") -> dict:
        """Place a reduce-only conditional stop-loss order."""
        payload = {
            "instId": inst_id, "tdMode": "cross",
            "side": side, "posSide": pos_side,
            "ordType": "conditional", "sz": sz,
            "slTriggerPx": sl_trigger_px, "slOrdPx": "-1",
            "slTriggerPxType": sl_trigger_px_type, "reduceOnly": "true",
        }
        result = await self._post("/api/v5/trade/order-algo", payload)
        log_action(f"条件止损单 触发={sl_trigger_px}  sz={sz}")
        return result["data"][0]

    async def cancel_algo_order(self, inst_id: str, algo_id: str) -> None:
        """Cancel a conditional algo order, ignoring already-closed failures."""
        try:
            await self._post("/api/v5/trade/cancel-algos", [{"instId": inst_id, "algoId": algo_id}])
            log_action(f"撤销条件单 algoId={algo_id}")
        except Exception as e:
            if _is_benign_cancel_error(e):
                log_action(f"撤条件单跳过，订单可能已触发/已撤/不存在 algoId={algo_id}: {e}")
                return
            logger.warning(f"撤条件单失败: {e}")

    async def close_position(self, inst_id: str, pos_side: str) -> dict:
        """Close the full position for one position side at market."""
        result = await self._post("/api/v5/trade/close-position",
                                  {"instId": inst_id, "posSide": pos_side, "mgnMode": "cross"})
        log_action(f"市价平仓 {pos_side}")
        return result["data"][0]
