"""Smoke checks for dashboard UX and history-log behavior."""
import asyncio
import re
import sys
from pathlib import Path

from aiohttp.test_utils import TestClient, TestServer

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src import dashboard


async def _run_api_checks() -> None:
    app = dashboard.create_dashboard_app()
    async with TestClient(TestServer(app)) as client:
        logs_response = await client.get("/api/logs")
        assert logs_response.status == 200
        logs = await logs_response.json()
        if logs:
            names = [item["name"] for item in logs]
            assert "__all__" in names
            assert names[-1] == "__all__"
            assert names[0] != "__all__"
            latest = next((name for name in names if name != "__all__"), "")
            assert latest.startswith("boll_pin_")

        state_response = await client.get("/api/state")
        assert state_response.status == 200
        state_payload = await state_response.json()
        assert "updated_at" in state_payload


def _run_source_checks() -> None:
    source = Path("src/dashboard.py").read_text(encoding="utf-8")
    assert source.count("_DESIGN_HTML =") == 1
    assert not re.search(r"^_HTML\s=", source, flags=re.MULTILINE)
    assert "summary-attention" in source
    assert "escapeHtml" in source
    assert "logs.find(log => log.name !== '__all__')" in source
    assert "HISTORY_CACHE" in source
    assert "DASHBOARD_RISK_THRESHOLDS" in source


def main() -> None:
    _run_source_checks()
    asyncio.run(_run_api_checks())
    print("dashboard smoke checks passed")


if __name__ == "__main__":
    main()
