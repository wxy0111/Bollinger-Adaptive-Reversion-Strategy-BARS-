"""OKX REST API 封装。"""
import hmac
import base64
import hashlib
import json
from datetime import datetime, timezone
from typing import Optional

import aiohttp
from loguru import logger

from src.config import API_KEY, SECRET_KEY, PASSPHRASE, FLAG

BASE_URL = "https://www.okx.com"


def _sign(timestamp: str, method: str, path: str, body: str = "") -> str:
    msg = timestamp + method.upper() + path + body
    return base64.b64encode(
        hmac.new(SECRET_KEY.encode(), msg.encode(), hashlib.sha256).digest()
    ).decode()


def _headers(method: str, path: str, body: str = "") -> dict:
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
    def __init__(self, session: aiohttp.ClientSession):
        self._s = session

    # ── 基础请求 ──────────────────────────────────────────────────────────

    async def _get(self, path: str, params: Optional[dict] = None) -> dict:
        qs = ("?" + "&".join(f"{k}={v}" for k, v in params.items())) if params else ""
        full_path = path + qs
        headers = _headers("GET", full_path)
        async with self._s.get(BASE_URL + full_path, headers=headers) as r:
            data = await r.json()
        if data.get("code") != "0":
            raise RuntimeError(f"GET {path} → {data.get('code')} {data.get('msg')}")
        return data

    async def _post(self, path: str, payload: dict) -> dict:
        body = json.dumps(payload)
        headers = _headers("POST", path, body)
        async with self._s.post(BASE_URL + path, headers=headers, data=body) as r:
            data = await r.json()
        if data.get("code") != "0":
            raise RuntimeError(f"POST {path} → {data.get('code')} {data.get('msg')}")
        return data

    # ── 行情 ──────────────────────────────────────────────────────────────

    async def get_klines(self, inst_id: str, bar: str, limit: int = 200) -> list:
        data = await self._get("/api/v5/market/candles",
                               {"instId": inst_id, "bar": bar, "limit": limit})
        return data["data"]

    async def get_mark_price(self, inst_id: str) -> float:
        data = await self._get("/api/v5/public/mark-price",
                               {"instId": inst_id, "instType": "SWAP"})
        return float(data["data"][0]["markPx"])

    # ── 账户 ──────────────────────────────────────────────────────────────

    async def get_balance(self, ccy: str = "USDT") -> float:
        data = await self._get("/api/v5/account/balance", {"ccy": ccy})
        for d in data["data"][0]["details"]:
            if d["ccy"] == ccy:
                return float(d["availBal"])
        return 0.0

    async def get_position(self, inst_id: str) -> Optional[dict]:
        data = await self._get("/api/v5/account/positions", {"instId": inst_id})
        positions = [p for p in data["data"] if float(p.get("pos", 0)) != 0]
        return positions[0] if positions else None

    async def set_leverage(self, inst_id: str, lever: int, mgn_mode: str = "cross") -> None:
        await self._post("/api/v5/account/set-leverage",
                         {"instId": inst_id, "lever": str(lever), "mgnMode": mgn_mode})
        logger.info(f"杠杆设置 {lever}x")

    # ── 普通下单 ──────────────────────────────────────────────────────────

    async def place_order(
        self,
        inst_id: str,
        side: str,
        pos_side: str,
        sz: str,
        ord_type: str = "market",
        px: Optional[str] = None,
        tp_px: Optional[str] = None,
        sl_px: Optional[str] = None,
        reduce_only: bool = False,
    ) -> dict:
        payload: dict = {
            "instId":  inst_id,
            "tdMode":  "cross",
            "side":    side,
            "posSide": pos_side,
            "ordType": ord_type,
            "sz":      sz,
        }
        if px:
            payload["px"] = px
        if reduce_only:
            payload["reduceOnly"] = "true"
        if tp_px:
            payload.update({"tpTriggerPx": tp_px, "tpOrdPx": "-1",
                            "tpTriggerPxType": "mark"})
        if sl_px:
            payload.update({"slTriggerPx": sl_px, "slOrdPx": "-1",
                            "slTriggerPxType": "mark"})
        result = await self._post("/api/v5/trade/order", payload)
        logger.info(f"下单 {side}/{pos_side} sz={sz} px={px or 'market'}")
        return result["data"][0]

    # ── 查询/撤销订单 ─────────────────────────────────────────────────────

    async def get_order(self, inst_id: str, ord_id: str) -> dict:
        data = await self._get("/api/v5/trade/order",
                               {"instId": inst_id, "ordId": ord_id})
        return data["data"][0]

    async def cancel_order(self, inst_id: str, ord_id: str) -> None:
        try:
            await self._post("/api/v5/trade/cancel-order",
                             {"instId": inst_id, "ordId": ord_id})
            logger.info(f"撤单 ordId={ord_id}")
        except Exception as e:
            logger.warning(f"撤单失败（可能已成交/不存在）: {e}")

    # ── 策略委托（止损条件单）────────────────────────────────────────────

    async def place_algo_order(
        self,
        inst_id: str,
        side: str,
        pos_side: str,
        sz: str,
        sl_trigger_px: str,
        sl_trigger_px_type: str = "mark",
    ) -> dict:
        payload = {
            "instId":          inst_id,
            "tdMode":          "cross",
            "side":            side,
            "posSide":         pos_side,
            "ordType":         "conditional",
            "sz":              sz,
            "slTriggerPx":     sl_trigger_px,
            "slOrdPx":         "-1",
            "slTriggerPxType": sl_trigger_px_type,
            "reduceOnly":      "true",
        }
        result = await self._post("/api/v5/trade/order-algo", payload)
        logger.info(f"条件止损单 触发={sl_trigger_px}  sz={sz}")
        return result["data"][0]

    async def cancel_algo_order(self, inst_id: str, algo_id: str) -> None:
        try:
            await self._post("/api/v5/trade/cancel-algos",
                             [{"instId": inst_id, "algoId": algo_id}])
            logger.info(f"撤销条件单 algoId={algo_id}")
        except Exception as e:
            logger.warning(f"撤条件单失败: {e}")

    # ── 市价平仓 ──────────────────────────────────────────────────────────

    async def close_position(self, inst_id: str, pos_side: str) -> dict:
        result = await self._post("/api/v5/trade/close-position",
                                  {"instId": inst_id, "posSide": pos_side, "mgnMode": "cross"})
        logger.info(f"市价平仓 {pos_side}")
        return result["data"][0]
