# Changelog

All notable strategy, risk-control, dashboard, and optimizer changes should be recorded here before uploading to GitHub.

Use this file to answer: what changed, why it changed, how it was tested, and what risk remains.

## 2026-06-20 - Realized Close PnL From Fills

### Changed

- Changed close PnL reporting to prefer OKX fill history instead of whole-account equity differences.
- Net close PnL now uses exchange-reported `fillPnl + fee`, so actual fees are included from the fills.
- Account-equity movement is kept as a diagnostic log only and is ignored for close PnL because other positions can change account equity while this strategy closes.
- When close fills are unavailable, the strategy falls back to estimated display PnL and skips realized-PnL rebalance instead of using trading-balance or account-equity differences.

### Tested

- `python -m compileall -q main.py src backtest tools`
- `git diff --check -- src\strategy.py`

## 2026-06-20 - Dashboard UX Status Strip And Safer History Loading

### Added

- Added a live trade status strip to the dashboard with:
  - Current position.
  - Unrealized PnL.
  - Take-profit distance.
  - Liquidation buffer.
  - Daily PnL.
  - Data age.
- Added online, delayed, and offline dashboard freshness states.
- Added history loading status and failure messages.

### Changed

- Reframed the risk ring as liquidation-buffer / safety-margin feedback with red, yellow, and green states.
- Changed the history tab to default to the latest single-day log instead of auto-loading `全部日志`.
- Kept `全部日志` available for manual full-period review, with a warning that it can take longer to parse.
- Updated README dashboard documentation in English and Chinese.

### Tested

- `.venv\Scripts\python.exe -m py_compile src\dashboard.py`
- Extracted dashboard JavaScript and checked it with `node --check`.
- Checked `/api/logs`.
- Checked single-day `/api/history`.
- `git diff --check -- src\dashboard.py`

### Risk Notes

- Browser visual verification could not run in this sandboxed session, so final pixel/layout QA should be done after restarting the local dashboard.
- Full `全部日志` parsing can still be slow on large log sets; it is now manual instead of automatic.

## 2026-06-20 - WxPusher Notifications And Levels

### Added

- Added `NOTIFY_PROVIDER` with `serverchan`, `wxpusher`, and `none` options.
- Added `NOTIFY_MIN_LEVEL` with `info`, `trade`, and `critical` levels.
- Added WxPusher configuration:
  - `WXPUSHER_APP_TOKEN`
  - `WXPUSHER_UIDS`
  - `WXPUSHER_TOPIC_IDS`

### Changed

- Rebuilt notification routing so all existing strategy notifications can use either ServerChan or WxPusher.
- Classified routine submitted-order messages as `info`.
- Classified fills, closes, and capital restoration as `trade`.
- Classified liquidation warning, trend risk, capital shortage, drawdown, and cross-copy protection as `critical`.
- Updated README and `.env.example` with WxPusher setup.

### Tested

- `python -m compileall -q main.py src backtest tools`

## 2026-06-19 - Profit Transfer Switch

### Added

- Added `TRANSFER_PROFIT_AFTER_CLOSE_ENABLED`.

### Changed

- When `TRANSFER_PROFIT_AFTER_CLOSE_ENABLED = False`, realized profit and excess trading-account balance stay in the trading account after a close.
- Loss top-up from funding to trading still runs in fixed-capital mode.
- README capital-mode documentation now describes the new switch.

### Tested

- `python -m compileall -q main.py src backtest tools`

## 2026-06-15 - Narrow Bollinger TP Experiment Default Off

### Added

- Added optional narrow-Bollinger take-profit controls:
  - `LOW_BOLL_WIDTH_TP_ENABLED`
  - `LOW_BOLL_WIDTH_REF_PCT`
  - `LOW_BOLL_WIDTH_MIN_TP_RETURN`
  - `LOW_BOLL_WIDTH_MAX_TP_RETURN`
  - `LOW_BOLL_WIDTH_TP_CAPTURE_RATIO`
  - `LOW_BOLL_WIDTH_SIZE_MULT`
- Added per-cycle TP target persistence so a restarted strategy keeps the TP target selected at entry time.
- Added `BOLL_MID_COST_TP_RETURN` for the Bollinger-mid cost trigger.

### Changed

- `LOW_BOLL_WIDTH_TP_ENABLED` is disabled by default.
- The default entry width remains `MIN_BOLL_WIDTH_PCT = 0.015`.
- Dynamic TP no longer restores to the original TP target after activation.
- Bollinger-mid cost trigger now reprices take-profit near `BOLL_MID_COST_TP_RETURN` instead of immediately market-closing the position.
- Log replay and optimizer parameter reports now mirror the live narrow-width TP and Bollinger-mid TP reprice logic.

### Tested

- `python -m compileall -q main.py src backtest`
- Current settings replay at 15-second sampling.
- Parameter spot checks for relaxed width thresholds: `0.008`, `0.010`, `0.012`, and `0.014`.

### Risk Notes

- Relaxing the minimum Bollinger width below 1.5% performed poorly in the tested local and extreme logs, even with closer TP and smaller first batch.
- The feature remains available for future controlled experiments, but it is intentionally off by default.
- GitHub CLI was not authenticated in this environment, so PR creation may require a separate authenticated session.

## 2026-06-12

### Added

- Added `PENDING_ORDER_BAND_GUARD_ENABLED`.
- Added pending-order Bollinger band guard:
  - Long pending entries are kept only when `order_price <= current_boll_lower`.
  - Short pending entries are kept only when `order_price >= current_boll_upper`.
- Added README documentation for pending-order maintenance and candle-lock behavior.
- Added this changelog.

### Changed

- Candle locking now follows real fills instead of order placement:
  - Placing an order does not lock the current 15-minute candle.
  - Canceling an unfilled order does not lock the current candle.
  - Repricing an unfilled order does not lock the current candle.
  - A real fill locks the candle, so one 15-minute candle can have at most one actual entry or add-on fill.
- Pending-entry cancellation now clears `_last_entry_check_kline_ts` so the same candle can re-evaluate when no fill happened.
- Reprice failure after canceling an old pending order now releases the pending-entry check lock.
- Log replay was updated to mirror the live pending-order band guard and fill-only candle lock.

### Tested

- `python -m py_compile src\strategy.py src\config.py backtest\log_parameter_optimizer.py`
- Local log replay at 15s, 30s, and 60s sampling.
- Extreme log replay at 30s and 60s sampling.
- Direct guard checks:
  - Long `order_price > lower` cancels.
  - Long `order_price <= lower` stays.
  - Short `order_price < upper` cancels.
  - Short `order_price >= upper` stays.

### Risk Notes

- Current extreme-log 30s replay can still report `wipeout_risk=True`; this is a strategy risk, not a pending-order lock bug.
- Log replay uses logged mark price and Bollinger snapshots, not full order-book fills.
- Different replay sampling intervals can still produce materially different results.

## 2026-06-12 - Entry Disaster Mid-Distance Filter

### Added

- Added `ENTRY_DISASTER_FAR_MID_RATIO = 0.85`.

### Changed

- Entry disaster scoring no longer uses the simple `below_mid` / `above_mid` reason.
- Fresh long entries now score `far_below_mid` only when:

```text
(boll_mid - price) / boll_width >= ENTRY_DISASTER_FAR_MID_RATIO
```

- Fresh short entries now score `far_above_mid` only when:

```text
(price - boll_mid) / boll_width >= ENTRY_DISASTER_FAR_MID_RATIO
```

- Optimizer replay was updated to use the same entry disaster scoring logic as live trading.

### Tested

- `python -m py_compile src\strategy.py src\config.py backtest\log_parameter_optimizer.py`
- Local log replay for ratios `0.70`, `0.85`, `1.00`, and `1.15`.
- Extreme log replay for the same ratio set.

### Risk Notes

- `ENTRY_DISASTER_FAR_MID_RATIO` prevents normal band-break entries from being blocked just because price is naturally below or above the middle band.
- It is not a standalone extreme-market protection tool; extreme risk still depends on entry max width, disaster scoring, fixed-loss stops, and cost-midline stops.

## 2026-06-11

### Fixed

- Restored missing strategy helper methods after refactors.
- Restored dashboard state update hooks.
- Fixed missing probe-state reset helper.
- Refreshed stop orders after liquidation-price updates so stop protection follows exchange-synced liquidation changes.

### Changed

- README was expanded with English documentation.
- Optimizer documentation was updated around the slim core parameter set.

### Tested

- Python compile checks on strategy, config, and optimizer files.
- Live startup errors around missing helper methods were resolved.

## Earlier Major Changes

### Strategy Naming And Documentation

- Renamed the strategy identity to **Bollinger Adaptive Reversion Strategy (BARS)**.
- Added BARS logo assets for README, GitHub, favicon, and dashboard use.
- Updated dashboard branding.

### Entry And Add-On Logic

- Moved away from fixed batch sizing toward dynamic entry ratios.
- Made the second batch dynamic.
- Added dynamic later add-on ratios.
- Added add-on guards for:
  - effective gap from the last real fill,
  - total entry cap,
  - completed-candle extreme guard,
  - fixed-loss head buffer,
  - take-profit improvement.
- Added first-entry maximum Bollinger width filtering.
- Added disaster-style entry filtering.

### Take Profit And Stop Logic

- Replaced fixed 10U take-profit distance with margin-return based take profit.
- Added dynamic take-profit lock thresholds.
- Added fixed-loss conditional stop based on strategy risk equity.
- Added liquidation-line stop refresh.
- Added disaster stop that closes the current position and keeps the program running.
- Added Bollinger-mid cost stop.
- Replaced BTG with the trend risk guard.
- Trend risk guard currently supports notification and add-on freezing; market close remains controlled by `TREND_RISK_GUARD_CLOSE_ENABLED`.

### Capital Management

- Added fixed-capital mode with profit transfer to funding.
- Added rolling compound mode.
- Added cross-copy protected-equity sizing.
- Added capital shortage pause and restoration notification.

### Dashboard And Logs

- Improved local dashboard design and chart markers.
- Added daily trade/profit views from logs.
- Improved terminal log categories and colors.
- Added 1-second market logging while preserving strategy decision cadence.

### Optimizer And Replay

- Slimmed the optimizer to focus on core return/risk parameters.
- Added cache and faster replay paths.
- Added current-config comparison in optimizer reports.
- Added extreme-log replay support for generated crash scenarios.

### Risk Notes

- The project still relies on log-based replay, not full exchange order-book simulation.
- Extreme market protection remains the main area for continued testing.
