"""Live Bollinger-band mean-reversion strategy.

The strategy watches 15-minute Bollinger bands on ``ETH-USDT-SWAP``. When mark
price moves outside the band and stops making new extremes, it places a batch
limit-entry plan. Filled positions receive dynamic take-profit orders based on
the exchange average entry price and a liquidation-line stop order.
"""
import asyncio
import json
import math
import time
import aiohttp
import pandas as pd
from pathlib import Path
from loguru import logger

from src.config import (
    INST_ID, BAR_15M, LEVER, KLINE_LIMIT,
    BOLL_INCLUDE_CURRENT,
    PRICE_LOG_INTERVAL, POLL_INTERVAL, BOLL_PERIOD,
    TP_PROFIT_USD, MIN_ENTRY_GAP_USD,
    MIN_BOLL_WIDTH_USD, MIN_BOLL_WIDTH_PCT,
    BOLL_WIDTH_BASE_PRICE, BOLL_WIDTH_BASE_USD,
    MIN_BOLL_WIDTH_FLOOR_USD, BOLL_WIDTH_GAP_MULT,
    BOLL_WIDTH_TP_SPACE_ENABLED, BOLL_WIDTH_TP_SPACE_MULT,
    ENTRY_MAX_BOLL_WIDTH_FILTER_ENABLED, ENTRY_MAX_BOLL_WIDTH_PCT,
    ENTRY_MAX_BOLL_WIDTH_USD,
    ENTRY_TREND_FILTER_ENABLED, ENTRY_TREND_FILTER_KLINES,
    ENTRY_TREND_FILTER_SCORE_THRESHOLD,
    ENTRY_TREND_FILTER_MID_SLOPE_PCT_PER_HOUR,
    ENTRY_TREND_FILTER_EDGE_SLOPE_PCT_PER_HOUR,
    ENTRY_TREND_FILTER_WIDTH_SLOPE_PCT_PER_HOUR,
    ENTRY_TREND_FILTER_STRONG_BREAK_ENABLED,
    ENTRY_TREND_FILTER_STRONG_KLINES,
    TP_TARGET_MARGIN_RETURN, DYNAMIC_TP_ENABLED,
    DYNAMIC_TP_ARM_RETURN, DYNAMIC_TP_RESTORE_RETURN,
    DYNAMIC_TP_REPRICE_GAP_USD,
    BOLL_TP_COMPRESSION_ENABLED, BOLL_TP_COMPRESSION_MIN_RETURN,
    BOLL_TP_COMPRESSION_EXIT_OFFSET_USD,
    MIN_HEAD_LIQ_BUFFER_PCT, DYNAMIC_ENTRY_GAP_ENABLED,
    DYNAMIC_ENTRY_GAP_MAX_USD, OKX_MAINTENANCE_MARGIN_RATE,
    OKX_LIQ_FEE_RATE,
    ADDON_DYNAMIC_GAP_ENABLED, ADDON_DYNAMIC_GAP_MAX_USD,
    ADDON_DYNAMIC_GAP_BOLL_START, ADDON_DYNAMIC_GAP_BOLL_STRONG,
    ADDON_DYNAMIC_GAP_BOLL_MAX_MULT,
    ADDON_DYNAMIC_GAP_HEAD_START_PCT, ADDON_DYNAMIC_GAP_HEAD_STRONG_PCT,
    ADDON_DYNAMIC_GAP_HEAD_MAX_MULT,
    ADDON_DYNAMIC_GAP_TREND_KLINES, ADDON_DYNAMIC_GAP_TREND_MULT,
    ADDON_RISK_BUDGET_ENABLED, ADDON_MIN_AVG_IMPROVE_USD,
    ADDON_MIN_AVG_IMPROVE_GAP_RATIO,
    ADDON_MAX_BOLL_WIDTH_FILTER_ENABLED, ADDON_MAX_BOLL_WIDTH_PCT,
    ADDON_MAX_BOLL_WIDTH_USD,
    ADDON_EXTREME_GUARD_ENABLED,
    ENTRY_EXTREME_GAP_ADJUST_ENABLED, ENTRY_EXTREME_GAP_BASE_PCT,
    ENTRY_EXTREME_GAP_FULL_PCT, ENTRY_EXTREME_GAP_MAX_MULT,
    ENTRY_24H_TICKER_CACHE_SEC,
    LIQ_STOP_OFFSET_USD, LIQ_WARNING_DISTANCE_USD,
    LIQ_WARNING_REPEAT_SEC,
    NO_NEW_EXTREME_TICKS,
    REPRICE_GAP_USD, INSIDE_BAND_CANCEL_KLINES,
    STRATEGY_EQUITY_CAP_USDT, CT_VAL, CONTRACT_STEP,
    TRADING_ACCOUNT_TARGET,
    CROSS_COPY_PROTECT_ENABLED, CROSS_COPY_PROTECT_EQUITY_USDT,
    CROSS_COPY_DYNAMIC_SIZING_ENABLED,
    SIZING_EQUITY_LOG_THRESHOLD_USDT,
    CAPITAL_REBALANCE_TOLERANCE_USDT, CAPITAL_REBALANCE_DELAY_SEC,
    COPY_FIXED_LOSS_STOP_ENABLED, COPY_FIXED_LOSS_STOP_USDT,
    COPY_FIXED_LOSS_STOP_RATIO,
    FIXED_LOSS_HEAD_BUFFER_ENABLED, FIXED_LOSS_HEAD_BUFFER_PCT,
    DISASTER_STOP_ENABLED, DISASTER_HEAD_DROP_PCT, DISASTER_LOSS_RATIO,
    TREND_RISK_GUARD_ENABLED, TREND_RISK_GUARD_CLOSE_ENABLED,
    TREND_RISK_FREEZE_ADDON_ENABLED,
    TREND_RISK_SCORE_THRESHOLD,
    TREND_RISK_HEAD_ADVERSE_PCT, TREND_RISK_KLINE_COUNT,
    TREND_RISK_MIN_HOLD_MIN, TREND_RISK_SLOPE_WINDOW_MIN,
    TREND_RISK_MID_SLOPE_PCT_PER_HOUR, TREND_RISK_EDGE_SLOPE_PCT_PER_HOUR,
    TREND_RISK_WIDTH_EXPAND, TREND_RISK_NOTIFY_INTERVAL_SEC,
    MAX_ENTRY_BATCHES, MAX_TOTAL_ENTRY_RATIO,
    FIRST_BATCH_RATIO,
    SECOND_BATCH_DYNAMIC_BASE_RATIO,
    SECOND_BATCH_DYNAMIC_MIN_RATIO, SECOND_BATCH_DYNAMIC_MAX_RATIO,
    SECOND_BATCH_DYNAMIC_FULL_GAP_USD,
    DYNAMIC_BASE_ENTRY_RATIO, DYNAMIC_MIN_ENTRY_RATIO, DYNAMIC_MAX_ENTRY_RATIO,
)
from src.okx_client import OKXClient
from src.indicators import build_df, add_boll
from src.risk import build_batch_plan
from src.position_manager import PositionState, OpenBatch
from src.notify import (
    notify_entry_order, notify_open, notify_close, notify_liq_warning,
    notify_capital_shortage, notify_capital_restored,
    notify_cross_copy_protect, notify_trend_risk_guard,
)
from src.logging_utils import log_action, log_check, log_market
import src.dashboard as dashboard


STATE_FILE = Path("logs/runtime_state.json")
COOLDOWN_FILE = Path("logs/close_cooldown.json")


class BollPinStrategy:
    """Stateful live trading strategy.

    The exchange position is treated as the source of truth for size, average
    entry, and liquidation price. Local state is used to track strategy-owned
    entry batches, exit orders, and restart recovery metadata.
    """

    def __init__(self):
        """Initialize local strategy state."""
        self._state   = PositionState(direction="none")
        self._peak_eq = 0.0
        self._running = False
        self._last_plan_kline_ts = None
        self._last_plan_entry_price = 0.0
        self._last_batch_kline_ts = None
        self._last_recovery_kline_ts = None
        self._last_entry_check_kline_ts = None
        self._last_close_kline_ts = None
        self._probe_kline_ts = None
        self._probe_direction = "none"
        self._probe_entry_price = 0.0
        self._inside_band_kline_count = 0
        self._last_inside_band_kline_ts = None
        self._recent_prices = []
        self._sizing_equity = 0.0
        self._fixed_batch_sizes = []
        self._restored_from_file = False
        self._capital_shortage_active = False
        self._last_liq_warning_ts = 0.0
        self._last_liq_warning_gap_usd = None
        self._dynamic_tp_active = False
        self._entry_extreme_gap_pct = 0.0
        self._entry_extreme_gap_mult = 1.0
        self._ticker_24h_cache = None
        self._ticker_24h_cache_ts = 0.0
        self._addon_extreme_guard_price = 0.0
        self._addon_extreme_guard_kline_ts = None
        self._addon_extreme_guard_batch_idx = -1
        self._addon_extreme_guard_started = False
        self._boll_history = []
        self._gap_context_df = None
        self._gap_context_row = None
        self._trend_entry_width = 0.0
        self._trend_entry_width_pct = 0.0
        self._trend_entry_time = None
        self._trend_last_notify_ts = 0.0
        self._trend_risk_guard_active = False

    async def run(self):
        """Run the strategy loop until stopped."""
        self._running = True
        logger.info("Strategy started: {} Bollinger mean-reversion {}x", INST_ID, LEVER)

        async with aiohttp.ClientSession() as session:
            client = OKXClient(session)
            try:
                await client.set_leverage(INST_ID, LEVER)
            except Exception as e:
                logger.warning(f"Set leverage failed; please verify {LEVER}x in OKX App: {e}")
            self._load_runtime_state()
            self._load_close_cooldown()
            await self._ensure_fixed_batch_sizes(client)
            await self._sync_state(client)

            last_strategy_tick = 0.0
            while self._running:
                try:
                    now = time.monotonic()
                    if now - last_strategy_tick >= POLL_INTERVAL:
                        await self._tick(client)
                        last_strategy_tick = time.monotonic()
                    else:
                        await self._log_market_snapshot(client)
                except Exception as e:
                    logger.exception(f"tick 异常: {e}")
                await asyncio.sleep(PRICE_LOG_INTERVAL)

    # 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁惧墽鎳撻—鍐偓锝庝簼閹癸綁鏌ｉ鐐搭棞闁靛棙甯掗～婵嬫晲閸涱剙顥氶梺璇叉唉椤煤閿曞倸鍨傞悹楦挎閺嗭妇绱掔€ｎ収鍤﹂柡鍐ㄧ墕閻掑灚銇勯幒鎴濐仼缁炬儳銈搁弻鏇熺節韫囨搩娲紓浣叉閸嬫挸鈹戦悩鍨毄濠殿喗鎸冲畷鎰磼濡粯鐝烽梺鍝勬川婵澹曟總鍛婄厽婵せ鍋撴繛浣冲洤鐓濋柛顐犲劜閻撴盯鎮橀悙鎻掆挃婵炲弶娼欓埞鎴︽晬閸曨偄骞嬪銈冨灪閻熲晠骞冮埄鍐╁劅妞ゆ梹鍨濆锕傛⒒?闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁惧墽鎳撻—鍐偓锝庝簼閹癸綁鏌ｉ鐐搭棞闁靛棙甯掗～婵嬫晲閸涱剙顥氬┑掳鍊楁慨鐑藉磻濞戔懞鍥偨缁嬫寧鐎梺鐟板⒔缁垶宕戦幇顓滀簻闁哄啫鍊归崵鈧繛瀛樼矒缁犳牠寮诲☉銏犵疀闂傚牊绋掗悘鍫澪旈悩闈涗杭闁搞劎鍎ょ粚杈ㄧ節閸ャ劌鈧兘鎮楀☉娆樼劷妞わ负鍎靛娲捶椤撴稒瀚涢梺绋款儏閿曨亪寮幇鐗堝€风€瑰壊鍠氶崣鍡涙⒑閸撴彃浜濈紒璇插瀹曟繈鏁冮埀顒勨€旈崘顔嘉ч柛鈩冾殘閻熴劑鏌ｆ惔銏犳惛闁告梹鍨垮畷娲焵?tick 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁惧墽鎳撻—鍐偓锝庝簼閹癸綁鏌ｉ鐐搭棞闁靛棙甯掗～婵嬫晲閸涱剙顥氶梺璇叉唉椤煤閿曞倸鍨傞悹楦挎閺嗭妇绱掔€ｎ収鍤﹂柡鍐ㄧ墕閻掑灚銇勯幒鎴濐仼缁炬儳銈搁弻鏇熺節韫囨搩娲紓浣叉閸嬫挸鈹戦悩鍨毄濠殿喗鎸冲畷鎰磼濡粯鐝烽梺鍝勬川婵澹曟總鍛婄厽婵せ鍋撴繛浣冲洤鐓濋柛顐犲劜閻撴盯鎮橀悙鎻掆挃婵炲弶娼欓埞鎴︽晬閸曨偄骞嬪銈冨灪閻熲晠骞冮埄鍐╁劅妞ゆ梹鍨濆锕傛⒒閸屾艾鈧绮堟笟鈧獮鏍敃閿旂粯鏅為梺鍛婃处閸ㄩ亶宕愰崸妤佺叆闁哄洨鍋涢埀顒€鎽滅划濠氭倷閻戞鍘繝鐢靛仜閻忔繈宕濈€涙绠鹃柟鐐墯閻撳ジ鏌熼鑲╃Ш鐎规洖鐖奸、鏃堝礋椤撶儐妲辨繝鐢靛О閸ㄥジ锝炴径濞掓椽鎮㈡總澶嬬稁缂傚倷鐒﹁摫濠殿垱鎸抽弻褑绠涢幘鍓佹殯闂侀€炲苯澧柨鏇ㄤ邯瀵鏁撻悩鎻掔獩濡炪倖鏌ㄦ晶浠嬫偪閸曨垱鍊甸悷娆忓缁€鍐煕閵婏箑顕滃ǎ鍥э躬閹虫粓妫冨☉姘辩嵁濠电姷鏁告慨鎾疮椤栨績鍙㈠┑鐘垫暩婵挳鎯€婢舵劕绾ч幖瀛樻尭娴滈箖鏌￠崶銉ョ仼缂佺姷濞€楠炴牕菐椤掆偓婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻?

    def _desired_sizing_equity(self, account_equity: float) -> float:
        """Return the fixed equity base used to size strategy batches."""
        if CROSS_COPY_DYNAMIC_SIZING_ENABLED:
            protected = CROSS_COPY_PROTECT_EQUITY_USDT if CROSS_COPY_PROTECT_ENABLED else 0.0
            available_for_strategy = max(account_equity - protected, 0.0)
            if TRADING_ACCOUNT_TARGET > 0:
                base_equity = min(TRADING_ACCOUNT_TARGET, available_for_strategy)
            else:
                base_equity = available_for_strategy
        elif self._capital_shortage_active and TRADING_ACCOUNT_TARGET > 0:
            base_equity = min(account_equity, TRADING_ACCOUNT_TARGET)
        else:
            base_equity = TRADING_ACCOUNT_TARGET if TRADING_ACCOUNT_TARGET > 0 else account_equity
        if STRATEGY_EQUITY_CAP_USDT > 0:
            return min(base_equity, STRATEGY_EQUITY_CAP_USDT)
        return base_equity

    def _sizing_equity_log_threshold(self) -> float:
        """Return the minimum sizing-equity change worth printing."""
        target_threshold = TRADING_ACCOUNT_TARGET * 0.10 if TRADING_ACCOUNT_TARGET > 0 else 0.0
        return max(SIZING_EQUITY_LOG_THRESHOLD_USDT, target_threshold)

    async def _sizing_account_equity(self, client: OKXClient) -> float:
        """Return the account value used for sizing decisions."""
        if CROSS_COPY_DYNAMIC_SIZING_ENABLED:
            return await client.get_equity("USDT")
        return await client.get_balance("USDT")

    async def _refresh_sizing_equity(self, client: OKXClient, account_equity: float) -> None:
        """Refresh dynamic sizing equity from the latest account equity."""
        desired_equity = self._desired_sizing_equity(account_equity)
        if abs(self._sizing_equity - desired_equity) <= 0.01:
            return

        previous = self._sizing_equity
        self._sizing_equity = desired_equity
        if self._state.batches:
            if self._sync_known_batch_sizes():
                self._save_runtime_state()
            if abs(previous - desired_equity) >= self._sizing_equity_log_threshold():
                log_check(
                    f"Sizing equity refreshed {previous:.2f} -> {desired_equity:.2f}; "
                    f"known sizes={self._fixed_batch_sizes}"
                )
            return

        await self._init_fixed_batch_sizes(
            client,
            account_equity=account_equity,
            sizing_equity=desired_equity,
        )

    async def _check_cross_copy_protection(self, client: OKXClient, account_equity: float) -> bool:
        """Close and stop when account equity reaches the protected line."""
        if not CROSS_COPY_PROTECT_ENABLED:
            return False
        if CROSS_COPY_PROTECT_EQUITY_USDT <= 0:
            return False
        if account_equity <= 0:
            logger.warning(
                "Account equity read as 0; skip cross copy protection for this tick"
            )
            return False
        if account_equity > CROSS_COPY_PROTECT_EQUITY_USDT:
            return False

        log_action(
            f"Cross copy protection triggered equity={account_equity:.2f} "
            f"protected={CROSS_COPY_PROTECT_EQUITY_USDT:.2f}; cancel orders and stop"
        )
        direction = self._state.direction
        total_sz = self._state.total_sz
        await self._cancel_entry_orders(client)
        await self._cancel_exchange_exit_orders(client)
        await self._cancel_exit_orders(client)
        self._reset_probe_state()
        self._state.reset()
        self._clear_runtime_state()
        self._running = False
        await notify_cross_copy_protect(
            account_equity,
            CROSS_COPY_PROTECT_EQUITY_USDT,
            direction,
            total_sz,
        )
        return True

    def _head_adverse_move_pct(self, mark_price: float) -> float:
        """Return mark-price adverse move from the first filled batch."""
        head_price = self._head_entry_price(mark_price)
        if head_price <= 0 or mark_price <= 0:
            return 0.0
        if self._state.direction == "long":
            return max((head_price - mark_price) / head_price, 0.0)
        if self._state.direction == "short":
            return max((mark_price - head_price) / head_price, 0.0)
        return 0.0

    def _remember_boll_snapshot(self, row, mark_price: float) -> None:
        """Keep recent Bollinger and candle-shape data for trend-risk checks."""
        if mark_price <= 0:
            return
        lower = float(row["boll_lower"])
        mid = float(row["boll_mid"])
        upper = float(row["boll_upper"])
        width = upper - lower
        half_width = upper - mid
        z_score = (mark_price - mid) / half_width if half_width else 0.0
        self._boll_history.append(
            {
                "ts": pd.Timestamp(row["ts"]),
                "high": float(row.get("high", mark_price) or mark_price),
                "low": float(row.get("low", mark_price) or mark_price),
                "lower": lower,
                "mid": mid,
                "upper": upper,
                "width": width,
                "width_pct": width / mark_price,
                "z": z_score,
            }
        )
        keep_after = pd.Timestamp(row["ts"]) - pd.Timedelta(
            minutes=max(TREND_RISK_SLOPE_WINDOW_MIN * 2, 180)
        )
        while self._boll_history and self._boll_history[0]["ts"] < keep_after:
            self._boll_history.pop(0)

    def _record_trend_entry_reference(self, row, mark_price: float) -> None:
        """Record the Bollinger width at the first filled batch."""
        if not self._state.is_active() or self._trend_entry_width > 0:
            return
        width = float(row["boll_upper"] - row["boll_lower"])
        if width <= 0 or mark_price <= 0:
            return
        self._trend_entry_width = width
        self._trend_entry_width_pct = width / mark_price
        self._trend_entry_time = pd.Timestamp(row["ts"])
        self._trend_last_notify_ts = 0.0
        self._save_runtime_state()

    def _series_slope_pct_per_hour(self, values: list[float], start_ts, end_ts) -> float:
        """Return percent-per-hour slope across a time window."""
        if len(values) < 2 or not values[0]:
            return 0.0
        hours = (pd.Timestamp(end_ts) - pd.Timestamp(start_ts)).total_seconds() / 3600
        if hours <= 0:
            return 0.0
        return (values[-1] - values[0]) / values[0] * 100 / hours

    def _recent_unique_boll_history(self, count: int) -> list[dict]:
        """Return recent unique candle snapshots from Bollinger history."""
        unique = []
        seen = set()
        for item in reversed(self._boll_history):
            ts = item["ts"]
            if ts in seen:
                continue
            unique.append(item)
            seen.add(ts)
            if len(unique) >= count:
                break
        return list(reversed(unique))

    def _entry_trend_filter_blocks(self, direction: str, mark_price: float) -> bool:
        """Return whether the current pre-entry trend shape is too directional."""
        if not ENTRY_TREND_FILTER_ENABLED or direction not in ("long", "short"):
            return False
        count = max(ENTRY_TREND_FILTER_KLINES, ENTRY_TREND_FILTER_STRONG_KLINES + 1, 2)
        recent = self._recent_unique_boll_history(count)
        if len(recent) < count:
            return False

        current = recent[-1]
        completed = recent[:-1]
        if ENTRY_TREND_FILTER_STRONG_BREAK_ENABLED and len(completed) >= ENTRY_TREND_FILTER_STRONG_KLINES:
            strong = completed[-ENTRY_TREND_FILTER_STRONG_KLINES:]
            lows = [item["low"] for item in strong]
            highs = [item["high"] for item in strong]
            lower_lows = all(lows[i] < lows[i - 1] for i in range(1, len(lows)))
            higher_highs = all(highs[i] > highs[i - 1] for i in range(1, len(highs)))
            if direction == "long" and lower_lows and mark_price <= lows[-1]:
                log_check(
                    "Entry trend filter blocked long: completed lower-lows and current breaks low"
                )
                return True
            if direction == "short" and higher_highs and mark_price >= highs[-1]:
                log_check(
                    "Entry trend filter blocked short: completed higher-highs and current breaks high"
                )
                return True

        window = recent[-ENTRY_TREND_FILTER_KLINES:]
        start, end = window[0], window[-1]
        mid_slope = self._series_slope_pct_per_hour(
            [item["mid"] for item in window],
            start["ts"],
            end["ts"],
        )
        lower_slope = self._series_slope_pct_per_hour(
            [item["lower"] for item in window],
            start["ts"],
            end["ts"],
        )
        upper_slope = self._series_slope_pct_per_hour(
            [item["upper"] for item in window],
            start["ts"],
            end["ts"],
        )
        width_slope = self._series_slope_pct_per_hour(
            [item["width_pct"] for item in window],
            start["ts"],
            end["ts"],
        )
        lows = [item["low"] for item in window]
        highs = [item["high"] for item in window]
        lower_lows = all(lows[i] < lows[i - 1] for i in range(1, len(lows)))
        higher_highs = all(highs[i] > highs[i - 1] for i in range(1, len(highs)))

        score = 0
        reasons = []
        if width_slope >= ENTRY_TREND_FILTER_WIDTH_SLOPE_PCT_PER_HOUR:
            score += 1
            reasons.append("width_expand")
        if direction == "long":
            if lower_lows:
                score += 1
                reasons.append("lower_lows")
            if mid_slope <= -ENTRY_TREND_FILTER_MID_SLOPE_PCT_PER_HOUR:
                score += 1
                reasons.append("mid_down")
            if lower_slope <= -ENTRY_TREND_FILTER_EDGE_SLOPE_PCT_PER_HOUR:
                score += 1
                reasons.append("lower_down")
        else:
            if higher_highs:
                score += 1
                reasons.append("higher_highs")
            if mid_slope >= ENTRY_TREND_FILTER_MID_SLOPE_PCT_PER_HOUR:
                score += 1
                reasons.append("mid_up")
            if upper_slope >= ENTRY_TREND_FILTER_EDGE_SLOPE_PCT_PER_HOUR:
                score += 1
                reasons.append("upper_up")

        if score < ENTRY_TREND_FILTER_SCORE_THRESHOLD:
            return False
        log_check(
            f"Entry trend filter blocked {direction}: score={score} "
            f"reasons={','.join(reasons)} width_slope={width_slope:.2f}%/h "
            f"mid_slope={mid_slope:.3f}%/h lower_slope={lower_slope:.3f}%/h "
            f"upper_slope={upper_slope:.3f}%/h"
        )
        return True

    def _trend_risk_signal(self, row, mark_price: float) -> dict | None:
        """Return trend-risk metrics when adverse trend conditions stack up."""
        if not TREND_RISK_GUARD_ENABLED:
            return None
        if not self._state.is_active():
            return None
        if self._trend_entry_width <= 0:
            self._record_trend_entry_reference(row, mark_price)
            return None
        if self._trend_entry_time is None:
            self._trend_entry_time = pd.Timestamp(row["ts"])
            return None

        now = pd.Timestamp(row["ts"])
        hold_min = (now - pd.Timestamp(self._trend_entry_time)).total_seconds() / 60
        if hold_min < TREND_RISK_MIN_HOLD_MIN:
            return None

        adverse_pct = self._head_adverse_move_pct(mark_price)
        if adverse_pct < TREND_RISK_HEAD_ADVERSE_PCT:
            return None

        width = float(row["boll_upper"] - row["boll_lower"])
        width_expand = width / self._trend_entry_width if self._trend_entry_width > 0 else 0.0
        width_pct = width / mark_price if mark_price > 0 else 0.0
        mid = float(row["boll_mid"])

        window_start = now - pd.Timedelta(minutes=TREND_RISK_SLOPE_WINDOW_MIN)
        window = [item for item in self._boll_history if item["ts"] >= window_start]
        if len(window) < 2:
            return None

        mid_slope = self._series_slope_pct_per_hour(
            [item["mid"] for item in window],
            window[0]["ts"],
            window[-1]["ts"],
        )
        lower_slope = self._series_slope_pct_per_hour(
            [item["lower"] for item in window],
            window[0]["ts"],
            window[-1]["ts"],
        )
        upper_slope = self._series_slope_pct_per_hour(
            [item["upper"] for item in window],
            window[0]["ts"],
            window[-1]["ts"],
        )

        recent = self._recent_unique_boll_history(max(TREND_RISK_KLINE_COUNT, 2))
        lows = [item["low"] for item in recent]
        highs = [item["high"] for item in recent]
        lower_lows = len(lows) >= TREND_RISK_KLINE_COUNT and all(
            lows[i] < lows[i - 1] for i in range(1, len(lows))
        )
        higher_highs = len(highs) >= TREND_RISK_KLINE_COUNT and all(
            highs[i] > highs[i - 1] for i in range(1, len(highs))
        )

        reasons = ["head_adverse"]
        if width_expand >= TREND_RISK_WIDTH_EXPAND:
            reasons.append("width_expand")

        if self._state.direction == "long":
            if mark_price < mid:
                reasons.append("below_mid")
            if lower_lows:
                reasons.append("lower_lows")
            if mid_slope <= -TREND_RISK_MID_SLOPE_PCT_PER_HOUR:
                reasons.append("mid_slope_down")
            if lower_slope <= -TREND_RISK_EDGE_SLOPE_PCT_PER_HOUR:
                reasons.append("lower_band_down")
        elif self._state.direction == "short":
            if mark_price > mid:
                reasons.append("above_mid")
            if higher_highs:
                reasons.append("higher_highs")
            if mid_slope >= TREND_RISK_MID_SLOPE_PCT_PER_HOUR:
                reasons.append("mid_slope_up")
            if upper_slope >= TREND_RISK_EDGE_SLOPE_PCT_PER_HOUR:
                reasons.append("upper_band_up")
        else:
            return None

        score = len(reasons)
        if score < TREND_RISK_SCORE_THRESHOLD:
            return None

        return {
            "head_price": self._head_entry_price(mark_price),
            "mark_price": mark_price,
            "adverse_pct": adverse_pct,
            "width_expand": width_expand,
            "width_pct": width_pct,
            "mid_slope": mid_slope,
            "lower_slope": lower_slope,
            "upper_slope": upper_slope,
            "score": score,
            "reasons": reasons,
            "hold_min": hold_min,
        }

    async def _check_trend_risk_guard(self, client: OKXClient, row, mark_price: float) -> bool:
        """Handle stacked trend-risk signals for the current position."""
        signal = self._trend_risk_signal(row, mark_price)
        if signal is None:
            return False

        if not self._trend_risk_guard_active:
            self._trend_risk_guard_active = True
            log_check(
                "Trend risk guard active; freeze add-on orders "
                f"direction={self._state.direction} score={signal['score']}"
            )
            self._save_runtime_state()

        now = time.time()
        should_notify = now - self._trend_last_notify_ts >= TREND_RISK_NOTIFY_INTERVAL_SEC
        if should_notify:
            log_action(
                "Trend risk guard triggered "
                f"direction={self._state.direction} mark={mark_price:.2f} "
                f"head={signal['head_price']:.2f} adverse={signal['adverse_pct']:.2%} "
                f"score={signal['score']} reasons={','.join(signal['reasons'])} "
                f"width_expand={signal['width_expand']:.3f} "
                f"width_pct={signal['width_pct']:.2%} "
                f"mid_slope={signal['mid_slope']:.3f}%/h "
                f"lower_slope={signal['lower_slope']:.3f}%/h "
                f"upper_slope={signal['upper_slope']:.3f}%/h"
            )
            self._trend_last_notify_ts = now
            self._save_runtime_state()
            await notify_trend_risk_guard(
                self._state.direction,
                mark_price,
                signal["head_price"],
                signal["adverse_pct"],
                signal["score"],
                signal["reasons"],
                signal["width_expand"],
                signal["width_pct"],
                signal["mid_slope"],
                signal["lower_slope"],
                signal["upper_slope"],
                TREND_RISK_GUARD_CLOSE_ENABLED,
            )

        if not TREND_RISK_GUARD_CLOSE_ENABLED:
            return False

        log_action("Trend risk guard close; strategy keeps running")
        await self._emergency_close(client, reason="trend_risk_guard")
        return True

    def _strategy_unrealized_pnl(self, mark_price: float) -> float:
        """Return current unrealized PnL for this strategy position."""
        if self._state.avg_entry <= 0 or self._state.total_sz <= 0:
            return 0.0
        if self._state.direction == "long":
            return (mark_price - self._state.avg_entry) * self._state.total_sz * CT_VAL
        if self._state.direction == "short":
            return (self._state.avg_entry - mark_price) * self._state.total_sz * CT_VAL
        return 0.0

    async def _check_disaster_stop(self, client: OKXClient, mark_price: float) -> bool:
        """Close the current cycle when disaster risk limits are reached."""
        if not DISASTER_STOP_ENABLED:
            return False
        if not self._state.is_active():
            return False
        if TRADING_ACCOUNT_TARGET <= 0:
            return False
        if DISASTER_HEAD_DROP_PCT <= 0 or DISASTER_LOSS_RATIO <= 0:
            return False

        head_move_pct = self._head_adverse_move_pct(mark_price)
        unrealized_pnl = self._strategy_unrealized_pnl(mark_price)
        loss_threshold = TRADING_ACCOUNT_TARGET * DISASTER_LOSS_RATIO
        if head_move_pct < DISASTER_HEAD_DROP_PCT:
            return False
        if unrealized_pnl > -loss_threshold:
            return False

        log_action(
            "Disaster stop triggered "
            f"direction={self._state.direction} mark={mark_price:.2f} "
            f"head={self._head_entry_price(mark_price):.2f} "
            f"head_move={head_move_pct:.2%} "
            f"unrealized={unrealized_pnl:+.4f} USDT "
            f"threshold={loss_threshold:.4f} USDT"
        )
        await self._emergency_close(client, reason="disaster_stop")
        return True

    def _floor_contract_size(self, raw_sz: float) -> float:
        """Floor a raw contract size to the exchange contract step."""
        step_count = int(raw_sz / CONTRACT_STEP)
        return round(step_count * CONTRACT_STEP, 8)

    def _set_fixed_batch_size(self, batch_idx: int, sz: float) -> None:
        """Store a planned size for a batch index."""
        while len(self._fixed_batch_sizes) <= batch_idx:
            self._fixed_batch_sizes.append(0.0)
        self._fixed_batch_sizes[batch_idx] = sz

    def _used_entry_ratio(self, exclude_batch_idx: int | None = None) -> float:
        """Estimate used entry margin ratio from filled and pending batches."""
        if self._sizing_equity <= 0:
            return 0.0
        used_margin = 0.0
        for batch in self._state.batches:
            if exclude_batch_idx is not None and batch.batch_idx == exclude_batch_idx:
                continue
            if batch.sz <= 0 or batch.price <= 0:
                continue
            used_margin += batch.sz * CT_VAL * batch.price / LEVER
        return used_margin / self._sizing_equity

    def _dynamic_entry_ratio(self, batch_idx: int, candidate_price: float) -> float:
        """Calculate the next entry margin ratio from recent price gaps."""
        if batch_idx == 0:
            return FIRST_BATCH_RATIO
        if batch_idx == 1:
            head_batch = next(
                (batch for batch in self._state.filled_batches() if batch.batch_idx == 0),
                None,
            )
            if head_batch is None or head_batch.price <= 0 or SECOND_BATCH_DYNAMIC_FULL_GAP_USD <= 0:
                return 0.0
            gap = abs(candidate_price - head_batch.price)
            dynamic_ratio = SECOND_BATCH_DYNAMIC_BASE_RATIO * gap / SECOND_BATCH_DYNAMIC_FULL_GAP_USD
            dynamic_ratio = max(SECOND_BATCH_DYNAMIC_MIN_RATIO, dynamic_ratio)
            dynamic_ratio = min(SECOND_BATCH_DYNAMIC_MAX_RATIO, dynamic_ratio)
            return dynamic_ratio

        filled = sorted(self._state.filled_batches(), key=lambda batch: batch.batch_idx)
        if len(filled) < 2:
            dynamic_ratio = DYNAMIC_BASE_ENTRY_RATIO
        else:
            prev_batch = filled[-2]
            last_batch = filled[-1]
            prev_gap = abs(last_batch.price - prev_batch.price)
            current_gap = abs(candidate_price - last_batch.price)
            if prev_gap <= 0:
                dynamic_ratio = DYNAMIC_BASE_ENTRY_RATIO
            else:
                dynamic_ratio = DYNAMIC_BASE_ENTRY_RATIO * (current_gap / prev_gap)
        dynamic_ratio = max(DYNAMIC_MIN_ENTRY_RATIO, dynamic_ratio)
        dynamic_ratio = min(DYNAMIC_MAX_ENTRY_RATIO, dynamic_ratio)
        return dynamic_ratio

    def _prepare_dynamic_batch_size(self, batch_idx: int, candidate_price: float) -> bool:
        """Calculate and store the dynamic size for the next planned batch."""
        if batch_idx >= MAX_ENTRY_BATCHES:
            return False
        if candidate_price <= 0 or self._sizing_equity <= 0:
            return False

        ratio = self._dynamic_entry_ratio(batch_idx, candidate_price)
        if ratio <= 0:
            return False

        margin_budget = self._sizing_equity * ratio
        raw_sz = margin_budget * LEVER / (candidate_price * CT_VAL)
        sz = self._floor_contract_size(raw_sz)
        if sz <= 0:
            return False

        used_ratio = self._used_entry_ratio(exclude_batch_idx=batch_idx)
        candidate_margin = candidate_price * sz * CT_VAL / LEVER
        candidate_ratio = candidate_margin / self._sizing_equity
        if used_ratio + candidate_ratio > MAX_TOTAL_ENTRY_RATIO:
            log_check(
                f"Dynamic batch skipped: batch={batch_idx + 1} "
                f"used={used_ratio:.2%} candidate={candidate_ratio:.2%} "
                f"limit={MAX_TOTAL_ENTRY_RATIO:.2%}"
            )
            return False

        if not self._fixed_loss_head_buffer_allows(batch_idx, candidate_price, sz):
            return False

        self._set_fixed_batch_size(batch_idx, sz)
        log_check(
            f"Dynamic batch prepared: batch={batch_idx + 1} "
            f"ratio={ratio:.2%} price={candidate_price:.2f} sz={sz}"
        )
        return True

    def _fixed_loss_target_usdt(self) -> float:
        """Return the configured fixed-loss amount for one strategy cycle."""
        if not COPY_FIXED_LOSS_STOP_ENABLED:
            return 0.0
        target_loss = COPY_FIXED_LOSS_STOP_USDT
        if target_loss <= 0:
            target_loss = TRADING_ACCOUNT_TARGET * COPY_FIXED_LOSS_STOP_RATIO
        return max(target_loss, 0.0)

    def _fixed_loss_stop_price(self, direction: str, avg_entry: float, total_sz: float) -> float:
        """Return the fixed-loss stop price for a simulated position."""
        target_loss = self._fixed_loss_target_usdt()
        if target_loss <= 0 or avg_entry <= 0 or total_sz <= 0:
            return 0.0
        price_delta = target_loss / (total_sz * CT_VAL)
        if direction == "long":
            return avg_entry - price_delta
        if direction == "short":
            return avg_entry + price_delta
        return 0.0

    def _simulated_entry_totals(self, batch_idx: int, candidate_price: float, candidate_sz: float):
        """Return average entry and size after replacing/adding one batch."""
        entries = [
            batch for batch in self._state.batches
            if batch.batch_idx != batch_idx and batch.price > 0 and batch.sz > 0
        ]
        entries.append(OpenBatch(
            batch_idx=batch_idx,
            ord_id="simulated",
            price=candidate_price,
            sz=candidate_sz,
            filled=False,
        ))
        total_sz = sum(batch.sz for batch in entries)
        if total_sz <= 0:
            return 0.0, 0.0
        avg_entry = sum(batch.price * batch.sz for batch in entries) / total_sz
        return avg_entry, total_sz

    def _fixed_loss_head_buffer_allows(self, batch_idx: int, candidate_price: float, candidate_sz: float) -> bool:
        """Return whether an add-on keeps fixed-loss stop beyond head buffer."""
        if not FIXED_LOSS_HEAD_BUFFER_ENABLED or batch_idx <= 0:
            return True
        if self._state.direction not in ("long", "short"):
            return True
        head_price = self._head_entry_price(candidate_price)
        if head_price <= 0 or candidate_price <= 0 or candidate_sz <= 0:
            return True

        avg_entry, total_sz = self._simulated_entry_totals(batch_idx, candidate_price, candidate_sz)
        stop_price = self._fixed_loss_stop_price(self._state.direction, avg_entry, total_sz)
        if stop_price <= 0:
            return True

        if self._state.direction == "long":
            required_stop = head_price * (1 - FIXED_LOSS_HEAD_BUFFER_PCT)
            if stop_price <= required_stop:
                return True
            log_check(
                f"Fixed-loss head buffer skipped: batch={batch_idx + 1} "
                f"stop={stop_price:.2f} must<= {required_stop:.2f} "
                f"head={head_price:.2f} buffer={FIXED_LOSS_HEAD_BUFFER_PCT:.2%}"
            )
            return False

        required_stop = head_price * (1 + FIXED_LOSS_HEAD_BUFFER_PCT)
        if stop_price >= required_stop:
            return True
        log_check(
            f"Fixed-loss head buffer skipped: batch={batch_idx + 1} "
            f"stop={stop_price:.2f} must>= {required_stop:.2f} "
            f"head={head_price:.2f} buffer={FIXED_LOSS_HEAD_BUFFER_PCT:.2%}"
        )
        return False

    def _addon_risk_budget_allows(
        self,
        batch_idx: int,
        candidate_price: float,
        candidate_sz: float,
        mark_price: float,
    ) -> bool:
        """Return whether an add-on improves average entry enough for its risk."""
        if not ADDON_RISK_BUDGET_ENABLED or batch_idx <= 0:
            return True
        if self._state.avg_entry <= 0 or candidate_price <= 0 or candidate_sz <= 0:
            return True

        avg_entry, _ = self._simulated_entry_totals(batch_idx, candidate_price, candidate_sz)
        if avg_entry <= 0:
            return True
        if self._state.direction == "long":
            improvement = self._state.avg_entry - avg_entry
        elif self._state.direction == "short":
            improvement = avg_entry - self._state.avg_entry
        else:
            return True

        required = max(
            ADDON_MIN_AVG_IMPROVE_USD,
            self._effective_entry_gap(mark_price) * ADDON_MIN_AVG_IMPROVE_GAP_RATIO,
        )
        if improvement >= required:
            return True
        log_check(
            f"Add-on risk budget skipped: batch={batch_idx + 1} "
            f"avg_improve={improvement:.2f} < required={required:.2f} "
            f"current_avg={self._state.avg_entry:.2f} candidate_avg={avg_entry:.2f}"
        )
        return False

    def _sync_known_batch_sizes(self) -> bool:
        """Keep only known filled or pending batch sizes in local runtime state."""
        known_batches = [batch for batch in self._state.batches if batch.batch_idx >= 0 and batch.sz > 0]
        if not known_batches:
            changed = bool(self._fixed_batch_sizes)
            self._fixed_batch_sizes = []
            return changed

        max_idx = max(batch.batch_idx for batch in known_batches)
        synced_sizes = [0.0] * (max_idx + 1)
        for batch in known_batches:
            synced_sizes[batch.batch_idx] = batch.sz

        if synced_sizes == self._fixed_batch_sizes:
            return False
        self._fixed_batch_sizes = synced_sizes
        return True

    async def _ensure_fixed_batch_sizes(self, client: OKXClient):
        """Keep known batch sizes aligned with the current sizing target."""
        account_equity = await self._sizing_account_equity(client)
        desired_equity = self._desired_sizing_equity(account_equity)
        previous_sizing_equity = self._sizing_equity
        self._sizing_equity = desired_equity
        if self._state.batches:
            changed = self._sync_known_batch_sizes()
            log_check(
                f"Known batch sizes synced sizing_equity={self._sizing_equity:.2f} "
                f"sizes={self._fixed_batch_sizes}; future add-ons use dynamic sizing"
            )
            if changed:
                self._save_runtime_state()
            return

        has_valid_sizes = len(self._fixed_batch_sizes) >= 2 and all(sz > 0 for sz in self._fixed_batch_sizes[:2])
        if has_valid_sizes and abs(previous_sizing_equity - desired_equity) <= 0.01:
            log_check(
                f"Using saved first/second batch sizes sizing_equity={self._sizing_equity:.2f} "
                f"sizes={self._fixed_batch_sizes}"
            )
            return

        if self._fixed_batch_sizes:
            log_check(
                f"Saved batch sizing_equity={previous_sizing_equity:.2f} "
                f"differs from target={desired_equity:.2f}; recalculating first/second sizes"
            )
        await self._init_fixed_batch_sizes(client, account_equity=account_equity, sizing_equity=desired_equity)

    async def _init_fixed_batch_sizes(self, client: OKXClient, account_equity: float | None = None, sizing_equity: float | None = None):
        """Calculate first and second batch sizes; later add-ons are dynamic."""
        equity = account_equity if account_equity is not None else await self._sizing_account_equity(client)
        self._sizing_equity = sizing_equity if sizing_equity is not None else self._desired_sizing_equity(equity)
        mark_price = await client.get_mark_price(INST_ID)
        batch_sizes = []
        margin_budget = self._sizing_equity * FIRST_BATCH_RATIO
        raw_sz = margin_budget * LEVER / (mark_price * CT_VAL)
        batch_sizes.append(self._floor_contract_size(raw_sz))
        self._fixed_batch_sizes = batch_sizes
        log_check(
            f"Head batch reference size available={equity:.2f} "
            f"sizing_equity={self._sizing_equity:.2f} sizes={self._fixed_batch_sizes}; "
            "actual order size is recalculated from live price before placing"
        )

    def _ts_to_str(self, value):
        """Serialize a timestamp-like value for runtime-state JSON."""
        if value is None:
            return None
        try:
            return pd.Timestamp(value).isoformat()
        except Exception:
            return str(value)

    def _str_to_ts(self, value):
        """Parse a timestamp from runtime-state JSON."""
        if not value:
            return None
        try:
            return pd.to_datetime(value)
        except Exception:
            return None

    def _state_payload(self) -> dict:
        """Build the runtime-state payload persisted to disk."""
        self._sanitize_runtime_state()
        return {
            "version": 1,
            "inst_id": INST_ID,
            "saved_at": pd.Timestamp.utcnow().isoformat(),
            "state": {
                "direction": self._state.direction,
                "batches": [
                    {
                        "batch_idx": b.batch_idx,
                        "ord_id": b.ord_id,
                        "price": b.price,
                        "sz": b.sz,
                        "filled": b.filled,
                    }
                    for b in self._state.batches
                ],
                "tp_ord_id": self._state.tp_ord_id,
                "sl_ord_id": self._state.sl_ord_id,
                "plan_liq_price": self._state.plan_liq_price,
                "plan_sl_price": self._state.plan_sl_price,
                "plan_tp_price": self._state.plan_tp_price,
                "avg_entry": self._state.avg_entry,
                "total_sz": self._state.total_sz,
                "remaining_batches_placed": self._state.remaining_batches_placed,
                "cycle_start_account_value": self._state.cycle_start_account_value,
                "cycle_start_ts": self._state.cycle_start_ts,
            },
            "strategy": {
                "peak_eq": self._peak_eq,
                "last_plan_kline_ts": self._ts_to_str(self._last_plan_kline_ts),
                "last_plan_entry_price": self._last_plan_entry_price,
                "last_batch_kline_ts": self._ts_to_str(self._last_batch_kline_ts),
                "last_recovery_kline_ts": self._ts_to_str(self._last_recovery_kline_ts),
                "last_entry_check_kline_ts": self._ts_to_str(self._last_entry_check_kline_ts),
                "probe_kline_ts": self._ts_to_str(self._probe_kline_ts),
                "probe_direction": self._probe_direction,
                "probe_entry_price": self._probe_entry_price,
                "inside_band_kline_count": self._inside_band_kline_count,
                "last_inside_band_kline_ts": self._ts_to_str(self._last_inside_band_kline_ts),
                "sizing_equity": self._sizing_equity,
                "fixed_batch_sizes": self._fixed_batch_sizes,
                "capital_shortage_active": self._capital_shortage_active,
                "dynamic_tp_active": self._dynamic_tp_active and self._state.is_active(),
                "entry_extreme_gap_pct": self._entry_extreme_gap_pct if self._state.has_working_plan() else 0.0,
                "entry_extreme_gap_mult": self._entry_extreme_gap_mult if self._state.has_working_plan() else 1.0,
                "addon_extreme_guard_price": (
                    self._addon_extreme_guard_price if self._state.has_working_plan() else 0.0
                ),
                "addon_extreme_guard_kline_ts": (
                    self._ts_to_str(self._addon_extreme_guard_kline_ts)
                    if self._state.has_working_plan() else None
                ),
                "addon_extreme_guard_batch_idx": (
                    self._addon_extreme_guard_batch_idx if self._state.has_working_plan() else -1
                ),
                "addon_extreme_guard_started": (
                    self._addon_extreme_guard_started if self._state.has_working_plan() else False
                ),
                "trend_entry_width": self._trend_entry_width if self._state.is_active() else 0.0,
                "trend_entry_width_pct": self._trend_entry_width_pct if self._state.is_active() else 0.0,
                "trend_entry_time": self._ts_to_str(self._trend_entry_time) if self._state.is_active() else None,
                "trend_last_notify_ts": self._trend_last_notify_ts if self._state.is_active() else 0.0,
                "trend_risk_guard_active": (
                    self._trend_risk_guard_active if self._state.is_active() else False
                ),
            },
        }

    def _save_runtime_state(self):
        """Persist local strategy state to disk."""
        try:
            STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
            tmp = STATE_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(self._state_payload(), ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(STATE_FILE)
        except Exception as e:
            logger.warning(f"Save runtime state failed: {e}")

    def _filled_batch_summary(self) -> str:
        """Return a compact readable summary of filled strategy batches."""
        filled = sorted(self._state.filled_batches(), key=lambda b: b.batch_idx)
        if not filled:
            return "none"
        return ", ".join(
            f"#{batch.batch_idx + 1} px={batch.price:.2f} sz={batch.sz:g} ord={batch.ord_id or '--'}"
            for batch in filled
        )

    def _log_runtime_state_summary(self, label: str) -> None:
        """Log the restored local-vs-exchange state in one scannable line."""
        log_check(
            f"{label}: direction={self._state.direction} avg={self._state.avg_entry:.2f} "
            f"sz={self._state.total_sz:g} tp={self._state.plan_tp_price:.2f} "
            f"sl={self._state.plan_sl_price:.2f} liq={self._state.plan_liq_price:.2f} "
            f"filled=[{self._filled_batch_summary()}]"
        )

    def _clear_runtime_state(self):
        """Delete the persisted runtime-state file."""
        self._dynamic_tp_active = False
        self._entry_extreme_gap_pct = 0.0
        self._entry_extreme_gap_mult = 1.0
        self._addon_extreme_guard_price = 0.0
        self._addon_extreme_guard_kline_ts = None
        self._addon_extreme_guard_batch_idx = -1
        self._addon_extreme_guard_started = False
        self._trend_entry_width = 0.0
        self._trend_entry_width_pct = 0.0
        self._trend_entry_time = None
        self._trend_last_notify_ts = 0.0
        self._trend_risk_guard_active = False
        try:
            if STATE_FILE.exists():
                STATE_FILE.unlink()
        except Exception as e:
            logger.warning(f"Clear runtime state failed: {e}")

    def _save_close_cooldown(self):
        """Persist the last close kline so restart cannot re-enter too soon."""
        if self._last_close_kline_ts is None:
            return
        try:
            COOLDOWN_FILE.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "inst_id": INST_ID,
                "last_close_kline_ts": self._ts_to_str(self._last_close_kline_ts),
            }
            COOLDOWN_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as e:
            logger.warning(f"Save close cooldown failed: {e}")

    def _load_close_cooldown(self):
        """Load the last close-kline cooldown marker if it exists."""
        if not COOLDOWN_FILE.exists():
            return
        try:
            payload = json.loads(COOLDOWN_FILE.read_text(encoding="utf-8"))
            if payload.get("inst_id") != INST_ID:
                return
            self._last_close_kline_ts = self._str_to_ts(payload.get("last_close_kline_ts"))
        except Exception as e:
            logger.warning(f"Load close cooldown failed: {e}")

    def _clear_close_cooldown(self):
        """Clear the close-kline cooldown marker after the next kline arrives."""
        try:
            if COOLDOWN_FILE.exists():
                COOLDOWN_FILE.unlink()
        except Exception as e:
            logger.warning(f"Clear close cooldown failed: {e}")

    def _sanitize_runtime_state(self):
        """Drop impossible local position residue before persisting or using it."""
        has_position = self._state.total_sz > 0
        has_batch = bool(self._state.batches)
        if self._state.direction in ("long", "short") and not has_position and not has_batch:
            logger.info("Local direction has no position or batches; clearing residue")
            self._reset_probe_state()
            self._state.reset()

    def _load_runtime_state(self):
        """Load local strategy state from disk when available."""
        if not STATE_FILE.exists():
            return
        try:
            payload = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            if payload.get("inst_id") != INST_ID:
                logger.warning("Local strategy state inst_id mismatch; ignoring saved state")
                return

            state = payload.get("state", {})
            self._state = PositionState(direction=state.get("direction", "none"))
            self._state.batches = [
                OpenBatch(
                    batch_idx=int(b.get("batch_idx", 0)),
                    ord_id=str(b.get("ord_id", "")),
                    price=float(b.get("price", 0) or 0),
                    sz=float(b.get("sz", 0) or 0),
                    filled=bool(b.get("filled", False)),
                )
                for b in state.get("batches", [])
            ]
            self._state.tp_ord_id = state.get("tp_ord_id")
            self._state.sl_ord_id = state.get("sl_ord_id")
            self._state.plan_liq_price = float(state.get("plan_liq_price", 0) or 0)
            self._state.plan_sl_price = float(state.get("plan_sl_price", 0) or 0)
            self._state.plan_tp_price = float(state.get("plan_tp_price", 0) or 0)
            self._state.avg_entry = float(state.get("avg_entry", 0) or 0)
            self._state.total_sz = float(state.get("total_sz", 0) or 0)
            self._state.remaining_batches_placed = bool(state.get("remaining_batches_placed", False))
            self._state.cycle_start_account_value = float(state.get("cycle_start_account_value", 0) or 0)
            self._state.cycle_start_ts = str(state.get("cycle_start_ts", "") or "")

            strategy = payload.get("strategy", {})
            self._peak_eq = float(strategy.get("peak_eq", 0) or 0)
            self._last_plan_kline_ts = self._str_to_ts(strategy.get("last_plan_kline_ts"))
            self._last_plan_entry_price = float(strategy.get("last_plan_entry_price", 0) or 0)
            self._last_batch_kline_ts = self._str_to_ts(strategy.get("last_batch_kline_ts"))
            self._last_recovery_kline_ts = self._str_to_ts(strategy.get("last_recovery_kline_ts"))
            self._last_entry_check_kline_ts = self._str_to_ts(strategy.get("last_entry_check_kline_ts"))
            self._probe_kline_ts = self._str_to_ts(strategy.get("probe_kline_ts"))
            self._probe_direction = strategy.get("probe_direction", "none")
            self._probe_entry_price = float(strategy.get("probe_entry_price", 0) or 0)
            self._inside_band_kline_count = int(strategy.get("inside_band_kline_count", 0) or 0)
            self._last_inside_band_kline_ts = self._str_to_ts(strategy.get("last_inside_band_kline_ts"))
            self._sizing_equity = float(strategy.get("sizing_equity", 0) or 0)
            self._fixed_batch_sizes = [float(x) for x in strategy.get("fixed_batch_sizes", [])]
            self._capital_shortage_active = bool(strategy.get("capital_shortage_active", False))
            self._dynamic_tp_active = bool(strategy.get("dynamic_tp_active", False))
            self._entry_extreme_gap_pct = float(strategy.get("entry_extreme_gap_pct", 0) or 0)
            self._entry_extreme_gap_mult = float(strategy.get("entry_extreme_gap_mult", 1) or 1)
            self._addon_extreme_guard_price = float(strategy.get("addon_extreme_guard_price", 0) or 0)
            self._addon_extreme_guard_kline_ts = self._str_to_ts(strategy.get("addon_extreme_guard_kline_ts"))
            self._addon_extreme_guard_batch_idx = int(strategy.get("addon_extreme_guard_batch_idx", -1) or -1)
            self._addon_extreme_guard_started = bool(strategy.get("addon_extreme_guard_started", False))
            self._trend_entry_width = float(
                strategy.get("trend_entry_width", strategy.get("btg_entry_width", 0)) or 0
            )
            self._trend_entry_width_pct = float(
                strategy.get("trend_entry_width_pct", strategy.get("btg_entry_width_pct", 0)) or 0
            )
            self._trend_entry_time = self._str_to_ts(
                strategy.get("trend_entry_time", strategy.get("btg_entry_time"))
            )
            self._trend_last_notify_ts = float(
                strategy.get("trend_last_notify_ts", strategy.get("btg_last_notify_ts", 0)) or 0
            )
            self._trend_risk_guard_active = bool(strategy.get("trend_risk_guard_active", False))
            self._sanitize_runtime_state()
            if self._sync_known_batch_sizes():
                self._save_runtime_state()
            self._restored_from_file = True
            logger.info(
                f"Loaded local strategy state: direction={self._state.direction} "
                f"batches={len(self._state.batches)} known_sizes={self._fixed_batch_sizes}"
            )
            self._log_runtime_state_summary("Loaded local runtime snapshot")
        except Exception as e:
            logger.warning(f"Load local runtime state failed: {e}")

    async def _fetch_market_snapshot(self, client: OKXClient):
        """Return latest candles, Bollinger row, and mark price."""
        raw = await client.get_klines(INST_ID, BAR_15M, KLINE_LIMIT)
        if not raw:
            logger.warning("Kline data is empty; skip this cycle")
            return None
        current_kline_ts = pd.to_datetime(int(raw[0][0]), unit="ms")
        df = build_df(raw, include_unconfirmed=BOLL_INCLUDE_CURRENT)
        df = add_boll(df)
        if df.empty:
            logger.warning("Kline or Bollinger data is not ready; skip this cycle")
            return None
        mark_price = await client.get_mark_price(INST_ID)
        last = df.iloc[-1].copy()
        last["ts"] = current_kline_ts
        return df, last, mark_price

    async def _log_market_snapshot(self, client: OKXClient):
        """Write a market snapshot without running trading decisions."""
        snapshot = await self._fetch_market_snapshot(client)
        if snapshot is None:
            return
        _, last, mark_price = snapshot
        equity = dashboard.state.equity
        width = float(last["boll_upper"] - last["boll_lower"])
        width_pct = width / mark_price if mark_price > 0 else 0.0
        log_market(
            f"price={mark_price:.2f}  Boll[{last['boll_lower']:.2f}"
            f" | {last['boll_mid']:.2f} | {last['boll_upper']:.2f}]"
            f"  position={self._state.direction}  equity={equity:.2f}"
            f"  kline={last['ts']} width={width:.2f} width_pct={width_pct:.4%}"
        )
        self._update_dashboard(mark_price, last, equity)

    async def _tick(self, client: OKXClient):
        """Run one strategy iteration."""
        # 1. Kline and Bollinger data.
        raw = await client.get_klines(INST_ID, BAR_15M, KLINE_LIMIT)
        if not raw:
            logger.warning("Kline data is empty; skip this tick")
            return
        current_kline_ts = pd.to_datetime(int(raw[0][0]), unit="ms")
        df  = build_df(raw, include_unconfirmed=BOLL_INCLUDE_CURRENT)
        df  = add_boll(df)
        if df.empty:
            logger.warning("Kline or Bollinger data is insufficient; skip this tick")
            return

        # 2. Account balances: trading balance funds entries; equity guards total risk.
        trading_balance = await client.get_balance("USDT")
        account_equity = await client.get_equity("USDT")
        if await self._check_cross_copy_protection(client, account_equity):
            return
        await self._check_capital_restored(client, trading_balance)
        if self._peak_eq == 0:
            self._peak_eq = account_equity
        self._peak_eq = max(self._peak_eq, account_equity)

        mark_price = await client.get_mark_price(INST_ID)
        await self._refresh_sizing_equity(client, account_equity)
        self._remember_price(mark_price)
        last = df.iloc[-1].copy()
        last["ts"] = current_kline_ts
        width = float(last["boll_upper"] - last["boll_lower"])
        width_pct = width / mark_price if mark_price > 0 else 0.0
        log_market(
            f"price={mark_price:.2f}  Boll[{last['boll_lower']:.2f}"
            f" | {last['boll_mid']:.2f} | {last['boll_upper']:.2f}]"
            f"  position={self._state.direction}  equity={account_equity:.2f}"
            f"  kline={last['ts']} width={width:.2f} width_pct={width_pct:.4%}"
            f" trading_balance={trading_balance:.2f}",
            terminal=True,
        )
        self._remember_boll_snapshot(last, mark_price)
        self._gap_context_df = df
        self._gap_context_row = last

        # 3. 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁惧墽鎳撻—鍐偓锝庝簼閹癸綁鏌ｉ鐐搭棞闁靛棙甯掗～婵嬫晲閸涱剙顥氬┑掳鍊楁慨鐑藉磻濞戔懞鍥偨缁嬫寧鐎梺鐟板⒔缁垶宕戦幇鐗堢厱闁归偊鍓欑痪褔鏌ｉ妶鍛仼闁宠鍨堕獮濠囨煕婵炑冩噹缁躲倕霉閻樺樊鍎忛柣銈庡枟閵囧嫰骞囬埡浣插亾閹版澘纾婚柟鐐墯濞尖晜銇勯幒鎴Ч閺佸牓姊绘笟鈧褍煤閵堝洠鍋撳顐㈠祮闁绘侗鍣ｉ獮鎺懳旈埀顒傜不閿濆棛绡€闁割煈鍋勬慨鍐磼鏉堛劎绠炴慨濠勭帛閹峰懘鎳為妷锝傚亾閸愵亞纾奸柍褜鍓氶幏鍛存嚃濠靛洨鈽夐柍瑙勫灩閳ь剨缍嗘禍锝夊箺閺囥垺鈷戦柟绋挎捣缁犳挻銇勯敂璇茬仯缂侇喛顕ч埥澶娾枎瀹ュ嫮鐩庨梻浣烘嚀閹碱偆绮旈崼鏇炵鐎广儱顦伴悡鏇㈠箹濞ｎ剙鐏悘蹇曟暩缁辨帗娼忛妸锕€闉嶉梺鐟板槻閹虫ê鐣烽锕€绀嬬痪鐗埫禍楣冩煙閹冾暢缁惧墽鏅埀顒€绠嶉崕杈┾偓姘煎枤缁綁寮崒妤€浜炬繛鍫濈仢閺嬫稒銇勯鐘插幋妤犵偛鍟存慨鈧柕鍫濇噽椤ρ囨⒑閸忚偐銈撮柡鍛⊕閹便劌煤椤忓應鎷绘繛杈剧悼閹虫捇顢氬鍕箚妞ゆ劧绱曢ˇ锕傛煃缂佹ɑ顥堢€殿喗鎸虫慨鈧柣妯活問濡查亶鏌ｉ悢鍝ョ煂濠⒀勵殘閺侇喖螖閸涱厾鍘?
        await self._sync_fills(client, mark_price, last["ts"], df)

        # 4. 濠电姷鏁告慨鐑藉极閸涘﹥鍙忛柣鎴ｆ閺嬩線鏌涘☉姗堟敾闁告瑥绻橀弻锝夊箣濠垫劖缍楅梺閫炲苯澧柛濠傛健楠炴劖绻濋崘顏嗗骄闂佸啿鎼鍥╃矓椤旈敮鍋撶憴鍕８闁告梹鍨甸锝夊醇閺囩偟顓洪梺缁樼懃閹虫劙鐛姀锛勭瘈闁汇垽娼ф禒锕傛煙缁嬫鐓肩€规洘妞藉畷姗€顢欓懖鈺嬬幢闂備浇顫夐崕鎶芥倶閸儱纾婚柟鎹愬煐閸犲棝鏌涢弴銊ュ妞わ富鍙冨铏规兜閸涱喚褰ч梺瑙勬倐缁犳牕鐣烽敐澶婂窛妞ゆ挆鍕槣闂備線娼ч悧鍡涘箠閹邦喚涓嶅ù鐓庣摠閻撴瑩鏌涢幇顓炵祷妞ゆ帇鍨荤槐鎺楀磼濮樻瘷銏ゆ懚閺嶎厽鐓曟繛鎴濆船閺嬫捇鏌熼柨瀣仢闁哄矉缍侀幃鈺呭礂閸涙澘鐒婚梻浣告啞閺屻劑鎯岄崒姘煎殨闁归棿绀佸Λ姗€骞栫€涙ɑ灏伴柡鍌楀亾濠碉紕鍋戦崐鏍ь潖婵犳艾鐓曢柛顐犲劚閸氬綊鏌ｉ弮鍥仩缁炬儳鍚嬮妵鍕籍閸屾瀚涢梺缁樻崄閸嬫劙鍩€椤掍緡鍟忛柛鐘崇☉閳绘柨鈽夊鍛綍闂傚倸鍊搁崐鎼佹偋婵犲嫮鐭欓柟鎯у閻挻绻涘顔荤凹闁绘挻绋戦湁闁挎繂娲﹂崵鈧繝娈垮枛閻楀繘鍩€椤掆偓閻忔艾顭垮Ο灏栧亾濮樼厧澧查柣蹇斿笒閳规垿鎮欑捄铏规缂備緡鍣崹鎯版＂濠电偞鍨惰彜闁衡偓娴犲鐓熸俊顖濇娴犳盯鏌￠崱蹇旀珚闁哄本绋撻埀顒婄秵閸嬪棗煤閹绢喗瀵犳繝闈涙储娴滄粓鏌熼幆褍鑸归柣蹇婃櫊閺屾盯濡搁妷銉㈠亾閹间焦绠掗梻浣虹帛閿氭俊顖氾躬瀹曟洝绠涘☉娆戝弮闂佸憡鍔︽禍婊堝几濞戙垺鐓涢悘鐐额嚙婵倿鏌熼鍝勭伈鐎规洦鍋婂畷鐔煎箣濞嗗繒浼勭紓浣介哺鐢繝宕洪埀顒併亜閹烘垵鈧敻宕戦幘缁樻櫜閹肩补鍓濋悘宥夋⒑閹惰姤鏁遍悽顖ょ節瀵鈽夐姀鈺傛櫇闂侀潧鐗嗛幊蹇涙倶娓氣偓濮婃椽妫冨☉娆樻！闁汇埄鍨辩敮鈥筹耿娓氣偓濮婅櫣绱掑鍫滅返闂佺顑呴幊搴ㄥ煝瀹ュ棛绡€闁告劏鏅涘鎸庣節閻㈤潧孝闁瑰啿绻橀、鏃堟偐缂佹鍘垫俊鐐差儏妤犳悂鍩㈤崼銉︾厱闁靛绠戦崝銈夋煟閿濆洤鍘寸€规洖鐖奸弫鍌炴寠婢跺苯骞堢紓鍌氬€搁崐鎼佸磹閹间礁纾瑰瀣婵ジ鏌＄仦璇插姎缁炬儳顭烽弻鐔煎礈瑜嶆禒娲煃瑜滈崜姘辨暜閹烘缍栨繝闈涱儐閺呮煡鏌涘☉鍗炲妞ゃ儲鑹鹃埞鎴︽晬閸曨偂鏉梺绋匡攻閸ㄥ灝鐣烽悷鎳婃椽顢旈崨顓濈敾闂備線娼ц噹闁告侗鍓涢悷婵囩節閻㈤潧浠﹂柛銊﹀劶瑜版粌鈹戦埄鍐ㄧ祷闁绘锕ョ粚杈ㄧ節閸ヨ埖鏅┑鐘欏懎浜鹃悗姘洴濮婃椽宕妷銉ょ钵缂備緡鍠楅悷銉╋綖韫囨洜纾兼俊顖濐嚙椤庢捇姊洪崨濠勨槈闁挎洏鍔庡☉鐢稿焵椤掑嫭鈷掑ù锝勮閻掑墽绱掗妸锔姐仢鐎规洘鍔曢埞鎴犫偓锝庘偓顓滃劦閺屾盯骞囬棃娑欑亪濡ょ姷鍋戦崹铏规崲濞戙垹骞㈡俊銈勭劍瀹曟娊姊洪崨濠冨蔼闁告柨鐭傞崺鐐哄箣閿旇棄鈧兘鏌涘▎蹇ｆ▓婵☆偓绻濆娲捶椤撗呭姼濡炪値鍘鹃崗妯虹暦閸濆嫧妲堥柕蹇曞Х椤撴椽姊洪幐搴⑩拻闁哄拋鍋婂畷銏ゆ偨閻㈢數锛濇繛杈剧到婢瑰﹪宕曢幇鐗堝€电紒妤佺☉濞层倗绮婚弻銉︾叆婵犻潧妫Σ褰掓煟閹惧啿鏆ｉ柡宀嬬畱铻ｅ〒姘煎灡閳绘挸鈹戦埥鍡楃仚闁稿鎹囧缁樻媴閸涘﹥鍎撻柣鐐村嚬閸嬪﹤鐣烽幇鏉垮嵆闁绘ɑ褰冮悿楣冩⒒娴ｈ棄鍚瑰┑顔芥綑鐓ら柍鍝勫暕閻掑﹥绻涢崱妯哄妞も晝鍏橀弻鐔兼⒒鐎电濡介梺鎶芥敱閸ㄥ潡寮婚敐澶嬪亜缂佸顑欏Λ鍡涙⒑閹稿海鈽夌紒澶婄秺瀵鈽夐姀鈥充汗閻庤娲栧ú銈夊煕鐏炶娇鏃堟偐闂堟稐绮堕梺鍝ュ枎閻°劑骞堥妸鈺佺劦妞ゆ帒瀚悡蹇涙煕椤愶絿绠栨い銉︾矊闇夋繝濠傚濞堟粓鏌″畝鈧崰鏍箠濠靛鍋嬮柛顐ｇ箖闁款厾绱撻崒娆戝妽鐟滄澘鍟…鍥灳閹颁礁娈ㄩ梺瑙勫劶濡嫰锝為崨瀛樼厪闁割偅绻冮ˉ鎴︽煙妞嬪海甯涚紒缁樼洴楠炴﹢寮堕幋鐘插Р闂備胶顭堥鍡涘箰閼姐倖宕叉繛鎴炵懄婵挳鏌涢幇顒€绾ч柛锝堟閳ь剝顫夊ú姗€鎮￠敓鐘茶摕闁绘柨鍚嬮崐缁樹繆椤栨繍鍤欑痪鏉跨Ч濮婃椽骞栭悙鎻掝瀴濠殿喖锕ょ紞濠冧繆閻㈢绀嬫い鏍ㄦ皑椤旀帡鏌ｉ悩鑽ょ窗闁靛棌鍋撻梺绋款儐閹瑰洭寮幇顓炵窞閻庯綆鍋呴悵鎶芥⒒娴ｈ櫣銆婇柛鎾寸箞閹柉顦归柟顖欑窔瀹曠厧鈹戦崘鈺傛澑婵＄偑鍊栧褰掑几缂佹鐟规繛鎴欏灪閻撴洘鎱ㄥ璇蹭壕缂備胶濮甸悧鏇㈡偩閻戣棄顫呴柕鍫濇噽椤旀劖绻涙潏鍓у埌闁告ɑ绮撻獮蹇撁洪鍛嫼闂佸憡绋戦敃锕傚煡婢舵劖鐓ラ柡鍥埀顒佺墵楠炲牓濡搁埡浣哄€炲銈嗗笂缁€渚€鍩€椤掆偓閻忔岸骞堥妸銉庣喖鎮℃惔鈥茬帛濠电姭鎷冮崘鎯ф闂侀€炲苯澧叉い顐㈩槸鐓ゆ慨妞诲亾鐎规洘绻傝灃闁告侗鍘鹃鍡涙⒑缂佹﹩鐒炬い銉ユ瀹曠兘顢樺☉妯瑰闂佹寧绻傛鍛婄閻愯鐟邦煥閸曨厽鍣板┑顔硷功缁垳绮悢鐓庣劦妞ゆ巻鍋撴い顓炴穿椤︽挳鏌熼獮鍨伈妤犵偞甯￠獮姗€鎳犻鍌滄毎缂傚倷鑳堕崑鎾诲磿閹剁瓔鏁勯柛鎰ㄦ櫇椤╄尙鎲搁悧鍫濈瑲闁绘挻鐟╅弻锝夊箣閻愬棙鍨规禍鎼佹偋閸垻顔曢梺鍛婁緱閸犳岸鎯岄幒鎾村弿濠电姴鍟妵婵堚偓瑙勬磸閸斿秶鎹㈠┑鍥ㄥ闁惧繐婀遍悾鎶芥⒒閸屾瑧鍔嶉柟顔肩埣瀹曟繂鐣濋埀顒傚垝閺冨倹鍠嗛柛鏇ㄤ簽缁犳岸姊洪崜鎻掍簼婵炲弶鐗犻幃娆愮節閸ャ劎鍙嗗┑鐘绘涧濡瑩宕崇粙娆剧唵閻熸瑥瀚粈瀣煛瀹€瀣М闁诡喓鍨藉畷顐﹀Ψ閿曗偓濞呮垿姊虹拠鎻掝劉闁告垵缍婂畷鎶芥晲婢跺苯绁﹀┑掳鍊曢幊搴ｇ矆閸愨斂浜滄い鎾跺枎閻忥箓鎮楅棃娑氱劯闁哄矉绲鹃幆鏃堝Ω閿斾粙鏁┑鐘灮閹虫捇鏁冮鍫濈畺闁绘劗鍎ら崐閿嬨亜閹存繂缍栫紒銊ヮ煼濮婃椽宕崟顒€顦╅梺鎸庡哺閺屾盯寮幘鎰佹喘闂侀€炲苯澧叉い顐㈩槸鐓ゆ繝濠傜墕缁愭鏌″搴″箲闁逞屽厸缁€浣界亙婵炶揪绲块幊鎾活敁閹剧粯鈷戦柟顖嗗懐顔囨繝鈷€宥囩М濠德ゅ煐瀵板嫮鈧急鍕伜婵犵數鍋犻幓顏嗗緤閸фせ鈧箓宕奸妷銉﹁緢闂備緡鍓欑粔鐢告偂濞嗘垹妫柡澶婄仢閼哥懓霉濠婂嫬顥嬮柍褜鍓濋～澶娒哄鈧畷婵嬪冀椤愶絽搴婂┑鐘绘涧濡厼顭囬埡鍌樹簻闁瑰搫绉电粊鎵磼闊彃鐏叉慨濠勭帛閹峰懘鎼归悷鎵偧婵＄偑鍊ら崢鐓幟洪妸鈺佺闁圭儤顨忛弫宥夋煟閹邦厽缍戝ù婊勵殜濮婅櫣绱掑Ο鑽ゅ弳闂佸憡鑹鹃澶愬箖閿熺姵鍋勯柛蹇氬亹閸樼敻姊绘笟鍥у伎缂佺姵鍨堕弲鑸电節濮橆厾鍘遍梺闈涚墕濞层倝寮稿☉銏＄厸閻忕偟鏅倴缂備緡鍣崣鍐ㄧ暦椤愶箑绀嬮柕濞垮劙婢规洖鈹戦悩缁樻锭妞ゆ垵鎳橀幏鎴︽偄閸濄儳顔曢梺鐟邦嚟閸嬬喖骞婇崟顖涚厱閹艰揪绲介弸娑㈡煛鐏炵偓绀夌紒鐘崇洴瀵挳鎮滈崱蹇撲壕閻忕偛褰炵换鍡樸亜閹扳晛鐏い銉ｅ灪閹便劍绻濋崘鈹夸虎閻庤娲忛崝宥囨崲濠靛纾兼繝濠傛噺閸ゅ啴姊绘担鍦菇闁糕晛瀚板畷褰掝敂閸繄顦┑鐘绘涧濞层劑鍩炲鍛斀闁绘ê寮堕幖鎰磼閻樺灚鍤€闂囧鏌ㄥ┑鍡樺櫤闁瑰弶鎮傞弻娑樜熼悜妯烘殘缂備胶绮粙鎺戭焽韫囨稑绀堢憸宥夘敋闁秵鐓熼柣姗嗗亜娴滈箖姊洪幐搴㈢闁稿﹤鎽滅槐?
        if self._state.is_active():
            await self._check_position_closed(client, mark_price, last["ts"])
        if self._state.is_active():
            self._record_trend_entry_reference(last, mark_price)
        if self._state.is_active() and self._update_addon_extreme_guard_from_completed_kline(df, last):
            self._save_runtime_state()
        if await self._check_trend_risk_guard(client, last, mark_price):
            self._update_dashboard(mark_price, last, account_equity)
            return
        if await self._check_disaster_stop(client, mark_price):
            self._update_dashboard(mark_price, last, account_equity)
            return
        if self._state.is_active():
            await self._recover_missing_entry_orders(client, df, last, self._sizing_equity, mark_price)

        # 5. 闂傚倸鍊搁崐鎼佸磹閹间礁纾圭€瑰嫭鍣磋ぐ鎺戠倞妞ゆ帒顦伴弲顏堟偡濠婂啰绠婚柛鈹惧亾濡炪倖甯婇懗鍫曞煝閹剧粯鐓涢柛娑卞灠閳诲牓鏌曢崱鏇狀槮闁宠閰ｉ獮姗€宕橀幓鎺撴殢濠碉紕鍋戦崐鏍箰妤ｅ啫纾婚柣鏂垮悑閸嬫﹢鏌曟径鍡樻珕闁抽攱鍨块弻娑樷攽閸℃浼€闂佸疇顕чˇ鐢稿蓟濞戞鐔煎垂椤旂粯鐫忕紓鍌欑贰閸犳稑鐣烽悽绋跨疅闁圭虎鍠栫粈瀣煃鐞涒€充壕缂備降鍔岄…宄邦潖閾忚鍠嗛柛鏇ㄥ墰閸戯繝姊虹紒妯煎⒈闁告鍥ｂ偓鏍ㄧ節閸ヨ埖鏅梺閫炲苯澧寸€殿喖顭烽弫鎰緞婵犲嫷鍟嬮梻浣告啞椤ㄥ牓宕戦悢绋款嚤闁割偁鍎查埛鎴︽煕濠靛棗顏悗姘缁辨帗寰勭仦鎯ф畬闂佷紮绲块崗姗€銆侀弴銏℃櫇闁逞屽墰缁牏鈧綆鍋佹禍婊堟煙閼割剙濡烽柛瀣崌閹煎綊顢曢妶鍌氫壕鐎规洖娲ㄧ壕浠嬫煕鐏炴崘澹橀柍褜鍓涢崗姗€骞冮悙鐑樻櫇闁稿本淇洪崺鐐烘⒑閻撳寒娼熼柛濠冾殘缁牊绻濋崒妯峰亾閹烘埈娼╅柨婵嗘噸婢规洟鏌ｆ惔銏╁晱闁哥姵鐩、姘愁樄闁糕斂鍎插鍕箛椤掑缍傞梻浣虹帛钃辩憸鏉垮暟缁﹪顢氶埀顒€顫忕紒妯肩懝闁逞屽墮椤洩顦归柍銉畵瀹曞ジ濡烽妷褝绱甸梻浣告惈鐞氼偊宕曢弻銉ョ；闁绘梹鎮舵禍婊堟煛閸ヮ煈娈斿ù婊勫劤閳规垿顢欑涵閿嬫暰濠碉紕鍋犲Λ鍕偩閻戣棄惟闁挎柨澧介惁鍫ユ⒑閸涘﹤濮﹀ù婊勭矊椤曪綁骞庨懞銉㈡嫽婵炶揪绲肩拃锕傚绩閻楀牏绠鹃柛娑卞枟缁€瀣煛娴ｇ鏆ｅ┑顔瑰亾闂佺粯鐟㈤崑鎾绘煕閵堝棙绀€闁宠鍨块幃鈺冣偓鍦Т椤ユ繈鏌熼婊冩灈婵﹥妞藉Λ鍐ㄢ槈鏉堛剱銈夋⒑閹肩偛濡芥俊鐐扮矙閺佹劙鎮欏顔兼倯闂佸憡渚楁禍婵嬪棘閳ь剟姊绘担鍝ユ瀮婵☆偄瀚灋婵°倕鎳忛崐鍫曟煥濠靛棭妲归柣鎾存礃娣囧﹪顢涘鍐冦垺绻涚仦鍌氣偓婵嬪箖閻愬绡€婵﹩鍘鹃崢鍓х磼閻愵剚绶茬€规洦鍓氶弲鍫曞箥椤旂偓锛忛梺纭咁潐閸旀牠藟婢舵劖顥嗗鑸靛姈閻撱儲绻濋棃娑欘棡濠㈣泛瀚伴弻娑㈠Χ閸屾矮澹曞┑鐘垫暩婵敻顢欓弽顓炵獥闁哄稁鍘介崑瀣叓閸ャ劍绀冩い顐ｆ礋閺岀喖鎮滃Ο鑽ゅ幐闂佺顑嗛幑鍥极閹邦厽鍎熼柍銉ㄥ皺閻╁酣姊绘担绛嬪殭缂佺粯鍔欓幃娲Ω閳轰絼锕傛煕閺囥劌鐏犳い顐㈡嚇閺屽秹濡烽妷銉︽瘣闂佸湱鏅弫璇差潖閻戞ɑ濮滈柟娈垮枛婵′粙姊洪崨濠冣拹闁绘娲熼、姘跺Ψ閳轰胶顦板銈嗗笒閸婂鎯侀崼鐔虹閺夊牆澧界粔顒佺箾閸滃啰绉€规洘鍔欓幃娆撴倻濡攱瀚介梻浣侯焾閺堫剟鎮疯钘濋柨鏇炲€归悡娆愮箾閺夋埈鍎岄柛瀣尵閹肩偓鎷呯憴鍕╀虎闂佽桨绀侀崯鏉戠暦閹烘埈娼╂い鎺戝€甸崑鎾活敇閵忥紕鍘告繝銏ｆ硾閿曪附鏅堕幇鐗堢厸闁告侗鍠氶幊鍥煙?< 3%?
        await self._maybe_notify_liq_warning(mark_price)
        if self._state.is_active():
            compressed = await self._maybe_update_boll_tp_compression(client, mark_price, last)
            if not compressed:
                await self._maybe_update_dynamic_tp(client, mark_price)

        # 6. 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁炬儳缍婇弻锝夊箣閿濆憛鎾绘煕閵堝懎顏柡灞剧洴椤㈡洟鏁愰崱娆樻К缂備胶鍋撻崕鍐差焽閿熺姴钃熼柨婵嗩槸椤懘鏌曡箛濠冩珖闁告梹鎮傚鍝勑ч崶褉鍋撳Δ鍛；闁规崘鍩栧畷鍙夌節闂堟稒宸濈紒鈾€鍋撻梻浣侯焾閺堫剛绮欓幋婵冩瀺闁靛牆娲ㄧ壕钘壝归敐鍛儓閺嶏繝鏌ｉ姀鈺佺仚闁逞屽墯閸撴艾顭囬弽顐ょ＝濞达綀鍋傞幋鐐插灁闁圭虎鍠楅悡鏇熺節闂堟稒顥滄い蹇婃櫅闇夐柣妯碱劜閼版寧鎱ㄦ繝鍛仩闁归濞€閹崇娀顢楅崒銈呮暭闂佽楠搁崢婊堝磻閹惧墎纾奸悗锝庡幗绾爼鏌￠崱顓烆洭闁汇儺浜、姗€鎮欓弶鎴烆仦濠电偛鐡ㄧ划宀€绱炴繝鍥ц摕闁绘柨鍚嬮崐缁樹繆椤栨繃顏犲ù鐘靛帶椤啴濡堕崱妯煎弳濠碘槅鍋呯换鍌烇綖韫囨稒鎯為柛锔诲幘閿涙粌鈹戦绛嬫當婵☆偅鐩畷婵堢矙濞嗙偓瀵岄梺闈涚墕濡瑩鎳栭悩缁樼厱婵炴垵宕悘锝夋煟閹烘垯鍋㈤柡宀嬬稻閹棃顢涘鍛咃綁姊虹粙娆惧剮闁绘帪濡囩划瀣吋閸滀胶鍙嗛梺鍓插亞閸犳捇宕㈤悽鍛娾拺闂侇偅绋撶粊閿嬬箾閸涱喗绀€闁宠绉瑰畷銊р偓娑欘焽閸樹粙妫呴銏＄カ缂佽尪妫勯埢宥咁吋婢跺鍘搁柣蹇曞仧閺咁偄鏆╂俊鐐紘閸屾粎鐛㈤梺鍝勬湰閻╊垶鐛Ο灏栧亾闂堟稒鍟為柛锝勫嵆濮婃椽鎮欓挊澶婂Х缂備胶濮甸崹鍧楀箖妤ｅ啯鏅搁柣妯虹－閸旂兘姊洪幐搴⑩拻闁哄拋鍋嗙划娆愭媴閸︻厾鐦堥梺姹囧灲濞佳勭濠婂嫪绻嗘い鏍ㄧ啲閺€鑽ょ磼閸屾氨效鐎规洖銈稿鎾倷鐎电硶鍋撻鍕拺闁告稑锕ゆ慨锕傛煕閻樻剚娈滈柟顕嗙節婵＄兘鏁傞崜褏妲囬梻鍌氬€搁悧濠勭矙閹烘澶婎煥閸曗晙绨婚悗鐢靛濠㈡ê煤閿曞倸纾归柛顭戝亞缁犻箖鏌熺€电浠滅紒妤佺缁绘盯宕ㄩ鐔风獩缂備浇椴哥敮妤€顭囪箛娑樜╅柨鏇楀亾闂傚绉瑰娲偡閺夋寧些闂佺娅曢崝妤呭礆閹烘鍋ㄧ痪鐗埫禍楣冩煥濠靛棛鍑圭紒銊ュ悑閵囧嫰寮崹顕呬純濠殿喖锕︾划顖炲箯閸涙潙宸濆┑鐘插暙閺嬫垿姊绘担鍛婃喐濠殿喚鏁诲畷婵嬪即閳垛斁鍋撻敃鍌氱倞鐟滃寮告惔銊︾厵闁绘劦鍓氱紞鎴澝归悩灞傚仮婵﹨娅ｇ槐鎺懳熼搹閫涚礃婵犵妲呴崑鍕偓姘煎墴瀵偊顢氶埀顒€顫忕紒妯诲闁荤喖鍋婇崵瀣攽閳藉棗浜楃紒鈧担鐣屼罕闂備礁鎲″ú锕傚垂閹殿喚鐭嗗鑸靛姈閻撴洘绻涢幋鐑嗙劷闁圭晫濞€閺屽秹鏌ㄧ€ｎ亞浼屽┑顔硷工椤嘲鐣锋總鍛婂亜闁诡厽宸婚崑鎾诲箳閺冨倻锛滈梺缁樕戣ぐ鍐汲闁秵鐓冮悷娆忓閻忓瓨銇勯姀锛勬创闁轰焦鍔欏畷銊╊敇閻欌偓閸熷洭姊绘担鐟扮鐎规洜鏁诲畷鎴︽倷鐎电硶鏀虫繝鐢靛Т濞层倕娲块梻浣告啞娓氭宕伴弽褉鏋嶉柕鍫濐槹閳锋垿鏌熼幆鏉啃撻柡渚€浜堕弻娑㈠Ω閵夛箑浠撮悗娈垮枦椤曆囧煡婢舵劕顫呴柍鈺佸暞閻濇洟姊洪懡銈呅㈡繛璇х畳閵囨劙宕橀浣镐壕婵炴垵纾粔顔芥叏婵犲懏顏犵紒杈ㄥ笒铻ｉ柣鎴烆焾閻т胶绱撻崒娆掑厡濠殿喚鏁婚幃褔鎮╁顔兼闁荤姴娲︾粊鏉懳ｉ崼鐔虹闁糕剝锚閻忊晠鏌ｉ敃鈧悧鎾愁潖濞差亜宸濆┑鐘插暟椤︻參姊烘潪鎵妽闁诡喖鍊搁悾宄扳攽鐎ｎ亞顔愭繛杈剧到閸樻粓骞忛悜妯肩闁哄鍨甸幃鎴︽煟閻旀繂娲ょ粻鏍р攽閸屾碍鍟為柣鎾存礋閻擃偊宕堕妸锔绘闂佽偐澧楃€笛囥€冮妷鈺傚€烽柟缁樺笚濞堢粯绻濆▓鍨灀闁稿鎹囧娲濞戞艾顣洪梺纭呮珪閸旀瑨妫熼悷婊勬煥椤繘鎼归崷顓犵厯闁荤姵浜介崝搴敊閸ヮ剚鈷戦柛娑橈龚婢规ɑ銇勯幋婵愭█妤犵偛鍟撮幃浠嬪礈閸欏娅囬梻浣瑰濡浇顣鹃梺褰掓敱濡炰粙寮婚敐澶嬪亹闁告瑥顦遍埞娑㈡煟韫囨挾绠查柣鐔濆懎鍨濆┑鐘宠壘缁狀噣鏌ら幁鎺戝姢闁告﹢浜堕弻锝嗘償椤栨粎校闂佺顑呭Λ婵嬪春婵犲洤鍗抽柕蹇ョ磿閸樻悂姊洪崨濠佺繁闁告﹢绠栧畷娲晲婢跺鍘电紓浣割儏鐏忓懘寮ㄩ懡銈囩＜闁哄啫鍊搁弸娑㈡煕閳哄纾块柍褜鍓ㄧ紞鍡涘礈濞戞壕鍙哄┑鐘垫暩婵兘銆傛禒瀣婵犻潧顑呯粻鏍煕瀹€鈧崑娑㈠及閵夆晜鐓ラ柣鏂挎惈瀛濈紒鐐劤閸氬鎹㈠┑鍥╃瘈闁稿本纰嶅▓顓犵磽娴ｅ搫小闁告鍟块～蹇涙惞鐟欏嫬鐝伴梺鍝勮閸庢椽鍩€椤掍緡娈樼紒杈ㄥ浮閹晠鎳犻顐庡應鍋撶憴鍕婵炶尙鍠庨悾鐑芥晸閻樺啿鈧墎绱撴担鑲℃垿濡靛┑鍡忔斀闁斥晛鍟崐鎰版煕閳规儳澧查柟宄版噺缁楃喖顢涘顓熺彎闂傚倸鍊风粈渚€骞栭位鍥敍閻愭潙浜遍梺鍛婂姦閸犳牠鎮块悙顒傜瘈闂傚牊绋撴晶鏇熴亜閵夛箑鍝洪柡宀嬬畱铻ｅ〒姘煎灡閿涘棝鎮峰鍛暭閻㈩垱顨婂鏌ュ蓟閵夛妇鍘卞┑鐐村灥瀹曨剟寮搁悢鍏肩厱閹兼番鍨虹亸鎵磼缂佹銆掗柍褜鍓氱粙鎺椻€﹂崶顒佸剹闁圭儤姊荤壕濂告煟濡櫣锛嶅褍鐏氶〃銉╂倷閹绘帗娈柧缁樼墵閺屾盯骞囬崗鍝ユ晼闂佷紮缍€濞夋盯鍩為幋锔藉€烽柡澶嬪灩娴犳悂姊洪幐搴″摵闁哄矉绻濆畷鍗炍熷ú缁橆棃闂備礁鎼懟顖滅矓閻戦摪銊︾瑹閳ь剟寮诲☉銏犵閻庢稒顭囧▓銈夋⒑鏉炴壆璐伴柛锝忕秮瀵偊骞樼紒妯轰汗闂佽偐鈷堥崜娑㈠几閸愨晝绡€闁汇垽娼у瓭闂佹寧娲忛崐婵嬪箖瑜庣换婵嬪炊娴ｅ壊妯€鐎规洖銈搁幃銏ゆ惞鐠団€虫櫗闂傚倷鑳堕幊鎾活敋椤撱垹纾婚柣鎰劋閸庡秹鏌熸潏楣冩闁绘挾鍠愮换娑㈠箣濠靛棜鍩為梺鍝勵儑閸犳挾妲愰幒鏃傜＜婵☆垵鍋愰悾鐢告⒑瀹曞洨甯涢柟鐟版搐閻ｇ柉銇愰幒婵囨櫓闂佷紮绲芥總鏃堝箟妤ｅ啯鈷掗柛灞剧懅閸斿秹鎮楃粭娑樺幘閸濆嫷鍚嬪璺猴功閺屟囨⒑缂佹﹩鐒鹃悘蹇旂懇瀵娊鏁冮崒娑氬帾闂婎偄娲㈤崕宕囧閹稿簺浜滈柍鍝勫暙閸樻挳鏌熼绛嬫疁闁轰焦鍔栭幆鏃堝灳閼碱剛娉挎繝鐢靛仜閻°劎鍒掑澶堚偓鍐╃節閸屻倖缍庡┑鐐叉▕娴滄繈鎮炴繝姘厽闁归偊鍨伴拕濂告倵濮橆厽绶叉い顓″劵椤т線鏌涢妸銉э紞婵″弶鍔欓獮鎺楀籍閸屾粣绱叉繝纰樻閸ㄧ敻宕戦幇鐗堝仾闁告洦鍨遍埛鎴犵磼椤栨稒绀冩繛鍛閺岋綁鍩℃繝鍌滀桓閻庤娲樺浠嬪春閳ь剚銇勯幒宥夋濞存粍绮撻弻鐔兼倻濡櫣浠村銈呯箚閺呮繈鍩€椤掑倹鍤€闁硅绱曢幑銏ゅ礃椤斿槈锕傛煕閺囥劌鐏犵紒鐘差煼閹妫冨☉娆愬枑闂佷紮绲介妶绋款潖缂佹ɑ濯撮柣鎴炆戦悵顏堟⒑鏉炴壆顦﹂柟鑺ョ矊鍗?
        if self._capital_shortage_active:
            pending_batch = self._state.pending_batch()
            if pending_batch is not None:
                logger.warning("Capital shortage active; cancel pending entry order and pause new entries")
                await self._cancel_entry_orders(client)
                if not self._state.is_active():
                    self._reset_probe_state()
                    self._state.reset()
                    self._save_runtime_state()
            else:
                logger.info("Capital shortage active; pause new entries until trading balance reaches target")
            self._update_dashboard(mark_price, last, account_equity)
            return

        if self._state.has_working_plan():
            if self._state.is_active():
                await self._maybe_place_next_batch(client, df, last, self._sizing_equity, mark_price)
            else:
                await self._maybe_reprice_probe_batch(client, df, last, self._sizing_equity, mark_price)
        else:
            if not self._boll_width_ok(last, mark_price):
                self._log_boll_width_skip("entry_skip", last, mark_price)
            elif not self._entry_max_boll_width_ok(last, mark_price):
                self._log_entry_max_boll_width_skip("entry_skip", last, mark_price)
            else:
                await self._maybe_place_probe_batch(client, df, last, mark_price, self._sizing_equity)

        # 7. 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁惧墽鎳撻—鍐偓锝庝簼閹癸綁鏌ｉ鐐搭棞闁靛棙甯掗～婵嬫晲閸涱剙顥氬┑掳鍊楁慨鐑藉磻閻愮儤鍋嬮柣妯荤湽閳ь兛绶氬鎾閻橀潧骞堟繝娈垮枟閿曗晠宕㈡禒瀣︽繝闈涙閺€浠嬫⒔閸ヮ剙鏄ラ柡宓苯娈梺鍛婃处閸樻悂宕戦幘缁樻櫜閹煎瓨绻勯懗鍝勨攽閳ュ啿绾ч柛鏃€鐟ラ～蹇曠磼濡偐鎳濋梺閫炲苯澧い顓炴穿椤﹁泛顭胯缁诲牆顫忓ú顏勪紶闁告洦鍓欏▍銈夋⒑閻戔晜娅撻柛銊ョ埣閻涱喛绠涘☉妯碱吅闂佹寧妫佸Λ鍕濠婂牊鐓熼煫鍥ㄦ尵缁狅綁鏌ｉ幒鐐电暤鐎殿噮鍓熼崺鈧い鎺戝閳锋帒霉閿濆牊顏犻悽顖涚洴閺屻劌顫濋幍浣镐壕婵炲牆鐏濋弸锕傛煕閳哄倻澧い鏇樺劦瀹曠喖顢涘槌栨Ч婵＄偑鍊栭悧妤冪矙閹捐鍌ㄩ梺顒€绉甸悡娆撴煕韫囨艾浜归柡鍡橈耿閺屾盯濡搁妷褏楔闂佽鍠楅敃銏ょ嵁濮椻偓椤㈡瑩鎮剧仦钘夌濠碉紕鍋戦崐鏍ь潖婵犳碍鍋ら柡鍌氱氨閺嬫梹绻濇繝鍌滃闁绘挻绋戦湁闁挎繂娲﹂崵鈧繝娈垮枛閻楁捇寮婚悢纰辨晬婵炴垶鐟Λ鍐⒑閹肩偛濮傜紒鐘崇墵楠炲﹪鎮╁ú缁樻櫌闂侀€炲苯澧寸€规洜鏁婚、妤呭磼濠婂拑绱?
        self._update_dashboard(mark_price, last, account_equity)

    def _boll_width_ok(self, last, mark_price: float) -> bool:
        """Return whether current Bollinger width allows new entries."""
        width = float(last["boll_width"])
        return width >= self._effective_min_boll_width(mark_price)

    def _log_boll_width_skip(self, reason: str, last, mark_price: float) -> None:
        """Log a contextual reason when Bollinger width blocks an action."""
        width = float(last["boll_width"])
        width_pct = width / mark_price if mark_price > 0 else 0.0
        required = self._effective_min_boll_width(mark_price)
        required_pct = required / mark_price if mark_price > 0 else 0.0
        log_check(
            f"{reason}: Bollinger width too narrow "
            f"width={width:.2f} < {required:.2f} "
            f"width_pct={width_pct:.2%} threshold={required_pct:.2%} "
            f"tp_space={self._tp_space_width_rule(mark_price):.2f}"
        )
        return
        _old_log_check_disabled(
            f"{reason}闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁炬儳缍婇弻锝夊箣閿濆憛鎾绘煕婵犲倹鍋ラ柡灞诲姂瀵噣宕奸悢鍛婎唶闂備胶顭堥鍡涘箰閸撗冨灊妞ゆ挾鍋愬Σ鍫熶繆椤栨繍鍤欐繛鍛囧洦鈷戞繛鑼额嚙楠炴鏌ｉ悢鍙夋珚鐎殿喖顭烽幃銏ゅ礂閻撳簶鍋撶紒妯诲弿婵°倐鍋撴俊顐ｇ懇閹箖鎮滈懞銉㈡嫼闂佺鍋愰崑娑㈠焵椤掍焦绀嬮柟顖氱焸瀹曟帡鎮欓懠鑸垫啺婵犵數鍋為崹顖炲垂閸︻厾涓嶇紓浣姑肩换鍡涙煏閸繂鈧憡绂嶆ィ鍐┾拺闁荤喐婢橀弳鍗炍旈悩铏€愰柟顕€绠栭幃婊堟寠婢光斂鍔戦弻宥夊传閸曨偀鍋撻悽绋跨闁惧繐婀辩壕浠嬫煕鐏炲墽鎳呴柛鏂跨Ч閺屻劌顫濋懜鐢靛幗闂婎偄娲﹀畝鎼佸传閸濆媱搴ㄥ炊瑜濋煬顒併亜閵忊剝绀嬪┑顔瑰亾闂佺粯鐟㈤崑鎾趁归悡搴℃殻闁诡喗顨堥幉鎾礋椤掑偆妲伴梻浣藉吹閸熷潡寮查悩鑽ゅ祦闁告劦鍠栭悡娑㈡煕濞戝崬骞樻い蟻鍥ㄢ拺闁告稑锕ｇ欢閬嶆煕閵婏箑鈻曢柟顔惧亾閵堬綁宕橀埞鐐濠电偞鎸婚崺鍐磻閹惧绠惧ù锝呭暱濞诧箓宕戠€ｎ喗鐓曢柍鈺佸暟閳藉鐥幆褏绉洪柡灞剧☉閳藉顫滈崼婵嗩潬婵＄偑鍊愰弲婊堟偂閿熺姴钃熼柣鏃傗拡閺佸秹鏌涢埄鍐炬畷婵犫偓娴煎瓨鍊甸梻鍫熺〒閻掑憡鎱ㄦ繝鍐┿仢婵☆偄鍟埥澶婎潩椤掑姣囧┑鐘殿暯濡插懘宕戦崨瀛樺仭闁冲搫鎳庨弰銉︾箾閹存瑥鐏╃紒鐘崇⊕閵囧嫰骞樼捄鐑樼€婚悗娈垮枟鐎笛呮崲濠靛鍋ㄩ梻鍫熷垁閵忋倖鐓曞┑鐘叉噹閸氬湱绱掗鑺ヮ棃闁诡喗绮岄～婊堝幢濮楀棗鏅梻鍌欒兌缁垶宕濋弴鐑嗗殨闁割偅娉欐径鎰潊闁冲灈鏅涙禍鐐箾閸繄浠㈡繛鍛耿閺屾盯鏁愯箛鏇炲煂闂佷紮缍侀弨杈╃紦娴犲宸濆┑鐘插€风花濠氭⒒娴ｅ憡鍟炵紒瀣灴閺佸啴鏁冩担鍏告睏闂佺硶鍓濈粙鎺楀煕閹烘嚚褰掓晲閸噥妫勯梺鍛婃皑閹虫捇鍩為幋锔绘晩闁兼祴鏅欑划鐢电磽娴ｇ鈧摜绮旈悽鐢电焿闁圭儤鏌￠崑鎾绘晲鎼存繄鏁栧銈忓瘜閸ｏ綁寮婚悢鐓庣闁兼祴鏅滃▓顒勬⒑閸涘﹥鐓ラ柣顒冨亹閸掓帗绻濆顓炰簻闂佺粯鍨堕敋闁哄鍨垮铏圭磼濮楀棙鐣堕梺缁橆殔濡稓鍒掗鐑嗘僵闁煎摜鏁搁崢?width={width:.2f} < {MIN_BOLL_WIDTH_USD:.2f} "
            f"width_pct={width_pct:.2%} threshold={MIN_BOLL_WIDTH_PCT:.2%}"
        )

    def _entry_max_boll_width_ok(self, last, mark_price: float) -> bool:
        """Return whether Bollinger width is not too wide for a first batch."""
        if not ENTRY_MAX_BOLL_WIDTH_FILTER_ENABLED:
            return True
        width = float(last["boll_width"])
        width_pct = width / mark_price if mark_price > 0 else 0.0
        pct_hit = ENTRY_MAX_BOLL_WIDTH_PCT > 0 and width_pct >= ENTRY_MAX_BOLL_WIDTH_PCT
        usd_hit = ENTRY_MAX_BOLL_WIDTH_USD > 0 and width >= ENTRY_MAX_BOLL_WIDTH_USD
        return not (pct_hit or usd_hit)

    def _log_entry_max_boll_width_skip(self, reason: str, last, mark_price: float) -> None:
        """Log why the maximum-width filter blocked a first-batch action."""
        width = float(last["boll_width"])
        width_pct = width / mark_price if mark_price > 0 else 0.0
        log_check(
            f"{reason}: Bollinger width too wide for first batch "
            f"width={width:.2f} max={ENTRY_MAX_BOLL_WIDTH_USD:.2f} "
            f"width_pct={width_pct:.2%} max_pct={ENTRY_MAX_BOLL_WIDTH_PCT:.2%}"
        )

    def _addon_max_boll_width_ok(self, last, mark_price: float) -> bool:
        """Return whether Bollinger width still allows add-on orders."""
        if not ADDON_MAX_BOLL_WIDTH_FILTER_ENABLED:
            return True
        width = float(last["boll_width"])
        width_pct = width / mark_price if mark_price > 0 else 0.0
        pct_hit = ADDON_MAX_BOLL_WIDTH_PCT > 0 and width_pct >= ADDON_MAX_BOLL_WIDTH_PCT
        usd_hit = ADDON_MAX_BOLL_WIDTH_USD > 0 and width >= ADDON_MAX_BOLL_WIDTH_USD
        return not (pct_hit or usd_hit)

    def _log_addon_max_boll_width_skip(self, reason: str, last, mark_price: float) -> None:
        """Log why the maximum-width filter blocked an add-on action."""
        width = float(last["boll_width"])
        width_pct = width / mark_price if mark_price > 0 else 0.0
        log_check(
            f"{reason}: Bollinger width too wide for add-on "
            f"width={width:.2f} max={ADDON_MAX_BOLL_WIDTH_USD:.2f} "
            f"width_pct={width_pct:.2%} max_pct={ADDON_MAX_BOLL_WIDTH_PCT:.2%}"
        )

    def _effective_boll_width_pct(self) -> float:
        """Return the active percentage width threshold."""
        base_pct = BOLL_WIDTH_BASE_USD / BOLL_WIDTH_BASE_PRICE if BOLL_WIDTH_BASE_PRICE > 0 else 0.0
        return max(MIN_BOLL_WIDTH_PCT, base_pct)

    def _head_entry_price(self, fallback_price: float | None = None) -> float:
        """Return the first filled batch price for dynamic risk calculations."""
        filled = sorted(self._state.filled_batches(), key=lambda b: b.batch_idx)
        if filled:
            return filled[0].price
        pending = self._state.pending_batch()
        if pending is not None and pending.batch_idx == 0:
            return pending.price
        if fallback_price is not None:
            return fallback_price
        return self._state.avg_entry

    def _min_ratio_ladder(self) -> list[float]:
        """Return the assumed minimum-size ladder up to the total-entry cap."""
        ratios = [FIRST_BATCH_RATIO, SECOND_BATCH_DYNAMIC_MIN_RATIO]
        while (
            sum(ratios) + DYNAMIC_MIN_ENTRY_RATIO <= MAX_TOTAL_ENTRY_RATIO + 1e-12
            and len(ratios) < MAX_ENTRY_BATCHES
        ):
            ratios.append(DYNAMIC_MIN_ENTRY_RATIO)
        if sum(ratios) < MAX_TOTAL_ENTRY_RATIO and len(ratios) < MAX_ENTRY_BATCHES:
            ratios.append(MAX_TOTAL_ENTRY_RATIO - sum(ratios))
        return ratios

    def _liq_price_for_ladder(self, head_price: float, gap: float) -> float | None:
        """Estimate OKX long liquidation price after minimum-ratio ladder fills."""
        ratios = self._min_ratio_ladder()
        prices = [head_price - idx * gap for idx in range(len(ratios))]
        if not prices or prices[-1] <= 0:
            return None

        sizes = []
        for ratio, price in zip(ratios, prices):
            raw_sz = TRADING_ACCOUNT_TARGET * ratio * LEVER / (price * CT_VAL)
            sz = math.floor(raw_sz / CONTRACT_STEP) * CONTRACT_STEP
            if sz <= 0:
                return None
            sizes.append(sz)

        qty = sum(sizes) * CT_VAL
        avg = sum(sz * CT_VAL * price for sz, price in zip(sizes, prices)) / qty
        entry_fee = sum(sz * CT_VAL * price * OKX_LIQ_FEE_RATE for sz, price in zip(sizes, prices))
        margin_balance = max(TRADING_ACCOUNT_TARGET - entry_fee, 0.0)
        denominator = qty * (OKX_MAINTENANCE_MARGIN_RATE + OKX_LIQ_FEE_RATE - 1)
        if denominator == 0:
            return None
        return (margin_balance - qty * avg) / denominator

    def _required_entry_gap_for_head_buffer(self, head_price: float) -> float:
        """Return minimum gap that keeps full-ladder liq distance above target."""
        if not DYNAMIC_ENTRY_GAP_ENABLED or head_price <= 0:
            return MIN_ENTRY_GAP_USD

        def buffer_pct(gap: float) -> float:
            liq = self._liq_price_for_ladder(head_price, gap)
            if liq is None:
                return 999.0
            return (head_price - liq) / head_price

        if buffer_pct(MIN_ENTRY_GAP_USD) >= MIN_HEAD_LIQ_BUFFER_PCT:
            return MIN_ENTRY_GAP_USD

        lo = MIN_ENTRY_GAP_USD
        hi = DYNAMIC_ENTRY_GAP_MAX_USD
        for _ in range(25):
            mid = (lo + hi) / 2
            if buffer_pct(mid) >= MIN_HEAD_LIQ_BUFFER_PCT:
                hi = mid
            else:
                lo = mid
        return round(hi, 2)

    def _effective_entry_gap(self, mark_price: float) -> float:
        """Return current entry spacing after head-price liquidation buffer."""
        head_price = self._head_entry_price(mark_price)
        base_gap = max(MIN_ENTRY_GAP_USD, self._required_entry_gap_for_head_buffer(head_price))
        mult = self._entry_extreme_gap_mult if ENTRY_EXTREME_GAP_ADJUST_ENABLED else 1.0
        mult *= self._addon_dynamic_gap_mult(mark_price)
        return round(base_gap * max(1.0, mult), 2)

    def _addon_dynamic_gap_mult(self, mark_price: float) -> float:
        """Return the add-on spacing multiplier from Bollinger and candle trend."""
        if not ADDON_DYNAMIC_GAP_ENABLED or not self._state.is_active():
            return 1.0

        mult = 1.0
        row = self._gap_context_row
        if row is not None and self._trend_entry_width > 0:
            width = float(row["boll_upper"] - row["boll_lower"])
            expand = width / self._trend_entry_width
            if expand >= ADDON_DYNAMIC_GAP_BOLL_STRONG:
                mult *= ADDON_DYNAMIC_GAP_BOLL_MAX_MULT
            elif expand >= ADDON_DYNAMIC_GAP_BOLL_START:
                mid_mult = 1.0 + (ADDON_DYNAMIC_GAP_BOLL_MAX_MULT - 1.0) * 0.5
                mult *= max(1.0, mid_mult)

        adverse_pct = self._head_adverse_move_pct(mark_price)
        if adverse_pct >= ADDON_DYNAMIC_GAP_HEAD_STRONG_PCT:
            mult *= ADDON_DYNAMIC_GAP_HEAD_MAX_MULT
        elif adverse_pct >= ADDON_DYNAMIC_GAP_HEAD_START_PCT:
            mid_mult = 1.0 + (ADDON_DYNAMIC_GAP_HEAD_MAX_MULT - 1.0) * 0.5
            mult *= max(1.0, mid_mult)

        mult *= self._addon_dynamic_gap_trend_mult()

        if ADDON_DYNAMIC_GAP_MAX_USD > 0 and MIN_ENTRY_GAP_USD > 0:
            mult = min(mult, ADDON_DYNAMIC_GAP_MAX_USD / MIN_ENTRY_GAP_USD)
        return max(1.0, mult)

    def _addon_dynamic_gap_trend_mult(self) -> float:
        """Return extra multiplier when completed candles keep moving against us."""
        if ADDON_DYNAMIC_GAP_TREND_MULT <= 1.0:
            return 1.0
        if self._state.direction not in ("long", "short"):
            return 1.0
        count = int(ADDON_DYNAMIC_GAP_TREND_KLINES)
        if count < 2:
            return 1.0
        df = self._gap_context_df
        if df is None or len(df) < count + 1:
            return 1.0
        completed = df.iloc[-(count + 1):-1]
        if len(completed) < count:
            return 1.0
        if self._state.direction == "long":
            lows = [float(value) for value in completed["low"].tail(count)]
            if all(lows[idx] > lows[idx + 1] for idx in range(len(lows) - 1)):
                return ADDON_DYNAMIC_GAP_TREND_MULT
            return 1.0
        highs = [float(value) for value in completed["high"].tail(count)]
        if all(highs[idx] < highs[idx + 1] for idx in range(len(highs) - 1)):
            return ADDON_DYNAMIC_GAP_TREND_MULT
        return 1.0

    def _effective_min_boll_width(self, mark_price: float) -> float:
        """Return the fixed-percent Bollinger-width threshold."""
        if mark_price <= 0:
            return MIN_BOLL_WIDTH_USD
        return max(MIN_BOLL_WIDTH_FLOOR_USD, mark_price * MIN_BOLL_WIDTH_PCT)

    def _tp_space_width_rule(self, mark_price: float) -> float:
        """Return the minimum Bollinger width implied by the take-profit target."""
        if not BOLL_WIDTH_TP_SPACE_ENABLED:
            return 0.0
        if mark_price <= 0 or LEVER <= 0 or TP_TARGET_MARGIN_RETURN <= 0 or BOLL_WIDTH_TP_SPACE_MULT <= 0:
            return 0.0
        tp_price_distance = mark_price * TP_TARGET_MARGIN_RETURN / LEVER
        return tp_price_distance * BOLL_WIDTH_TP_SPACE_MULT

    def _previous_kline_row(self, df, kline_ts=None):
        """Return the candle before ``kline_ts`` or before the current candle."""
        if df is None or len(df) < 2:
            return None
        if kline_ts is not None and "ts" in df:
            target = pd.to_datetime(kline_ts)
            matches = df.index[df["ts"] == target]
            if len(matches) and int(matches[0]) > 0:
                return df.iloc[int(matches[0]) - 1]
        return df.iloc[-2]

    def _start_addon_extreme_guard_from_fill(self, df, direction: str, batch_idx: int, fill_kline_ts=None) -> None:
        """Start tracking completed-candle extremes after the first fill."""
        if not ADDON_EXTREME_GUARD_ENABLED or direction not in ("long", "short"):
            return
        self._addon_extreme_guard_started = True
        self._addon_extreme_guard_batch_idx = batch_idx
        prev = self._previous_kline_row(df, fill_kline_ts)
        if prev is None:
            logger.warning("Add-on extreme guard start failed: not enough kline data")
            return
        old_guard = self._addon_extreme_guard_price
        if direction == "long":
            current_extreme = float(prev["low"])
            guard_price = (
                current_extreme
                if old_guard <= 0
                else min(old_guard, current_extreme)
            )
            label = "prev_low"
        else:
            current_extreme = float(prev["high"])
            guard_price = (
                current_extreme
                if old_guard <= 0
                else max(old_guard, current_extreme)
            )
            label = "prev_high"
        self._addon_extreme_guard_price = guard_price
        self._addon_extreme_guard_kline_ts = prev.get("ts")
        self._addon_extreme_guard_batch_idx = batch_idx
        action = "started" if old_guard <= 0 else "continued"
        log_check(
            f"Add-on extreme guard {action}: batch={batch_idx + 1} "
            f"direction={direction} {label}={guard_price:.2f} "
            f"kline={self._addon_extreme_guard_kline_ts}"
        )

    def _update_addon_extreme_guard_from_completed_kline(self, df, last) -> bool:
        """Update the tracked extreme with the latest completed candle."""
        if not ADDON_EXTREME_GUARD_ENABLED:
            return False
        if not self._state.is_active():
            return False
        if not self._addon_extreme_guard_started and self._state.filled_batches():
            self._addon_extreme_guard_started = True
        if not self._addon_extreme_guard_started:
            return False
        prev = self._previous_kline_row(df, last["ts"] if last is not None and "ts" in last else None)
        if prev is None:
            return False
        prev_ts = prev.get("ts")
        if self._addon_extreme_guard_kline_ts is not None and prev_ts == self._addon_extreme_guard_kline_ts:
            return False

        old = self._addon_extreme_guard_price
        if self._state.direction == "long":
            current = float(prev["low"])
            new_guard = current if old <= 0 else min(old, current)
            label = "low"
        elif self._state.direction == "short":
            current = float(prev["high"])
            new_guard = current if old <= 0 else max(old, current)
            label = "high"
        else:
            return False

        self._addon_extreme_guard_price = new_guard
        self._addon_extreme_guard_kline_ts = prev_ts
        last_batch = self._state.last_filled_batch()
        if last_batch is not None:
            self._addon_extreme_guard_batch_idx = last_batch.batch_idx
        log_check(
            f"Add-on extreme guard kline update: direction={self._state.direction} "
            f"kline={prev_ts} {label}={current:.2f} guard={new_guard:.2f}"
        )
        return True

    def _addon_extreme_guard_allows(self, order_price: float, batch_idx: int) -> bool:
        """Return whether an add-on order breaks the stored candle extreme."""
        if not ADDON_EXTREME_GUARD_ENABLED:
            return True
        if self._addon_extreme_guard_price <= 0:
            log_check("Add-on extreme guard missing; skip add-on until guard is initialized")
            return False
        if self._state.direction == "long":
            if order_price < self._addon_extreme_guard_price:
                return True
            log_check(
                f"Add-on skipped: long price has not broken previous guard low "
                f"batch={batch_idx + 1} next={order_price:.2f} "
                f"guard_low={self._addon_extreme_guard_price:.2f}"
            )
            return False
        if self._state.direction == "short":
            if order_price > self._addon_extreme_guard_price:
                return True
            log_check(
                f"Add-on skipped: short price has not broken previous guard high "
                f"batch={batch_idx + 1} next={order_price:.2f} "
                f"guard_high={self._addon_extreme_guard_price:.2f}"
            )
            return False
        return False

    async def _cancel_pending_batch_due_to_guard(self, client: OKXClient, pending_batch, reason: str) -> None:
        """Cancel a pending add-on when lifecycle guards no longer allow it."""
        log_check(
            f"Cancel pending batch {pending_batch.batch_idx + 1}: {reason} "
            f"ordId={pending_batch.ord_id}"
        )
        try:
            await client.cancel_order(INST_ID, pending_batch.ord_id)
        except Exception as exc:
            log_check(
                f"Cancel pending batch skipped, order may be filled/canceled/missing "
                f"ordId={pending_batch.ord_id}: {exc}"
            )
        self._state.remove_batch(pending_batch.ord_id)
        self._save_runtime_state()

    async def _cancel_pending_if_addon_guards_fail(self, client: OKXClient, df, last, pending_batch) -> bool:
        """Cancel pending add-on orders when add-on guards fail on a new candle."""
        if pending_batch.batch_idx <= 0:
            return False
        if not self._fixed_loss_head_buffer_allows(
            pending_batch.batch_idx,
            pending_batch.price,
            pending_batch.sz,
        ):
            await self._cancel_pending_batch_due_to_guard(client, pending_batch, "fixed-loss head buffer failed")
            return True
        return False

    async def _cancel_pending_if_addon_width_too_wide(
        self,
        client: OKXClient,
        last,
        mark_price: float,
        pending_batch,
    ) -> bool:
        """Cancel pending add-on orders when Bollinger width becomes too wide."""
        if pending_batch.batch_idx <= 0:
            return False
        if self._addon_max_boll_width_ok(last, mark_price):
            return False
        self._log_addon_max_boll_width_skip(
            f"cancel_pending_batch_{pending_batch.batch_idx + 1}",
            last,
            mark_price,
        )
        await self._cancel_pending_batch_due_to_guard(
            client,
            pending_batch,
            "add-on Bollinger width too wide",
        )
        return True

    async def _cancel_pending_if_trend_risk_freeze(self, client: OKXClient, pending_batch) -> bool:
        """Cancel pending add-on orders after the trend-risk guard freezes adds."""
        if not TREND_RISK_FREEZE_ADDON_ENABLED:
            return False
        if not self._trend_risk_guard_active:
            return False
        if pending_batch.batch_idx <= 0:
            return False
        log_check(
            f"Trend risk guard freeze; cancel pending add-on batch {pending_batch.batch_idx + 1}"
        )
        await self._cancel_pending_batch_due_to_guard(
            client,
            pending_batch,
            "trend risk freeze",
        )
        return True

    def _entry_extreme_multiplier(self, gap_pct: float) -> float:
        """Return entry-gap multiplier from first-entry 24h extreme distance."""
        if not ENTRY_EXTREME_GAP_ADJUST_ENABLED:
            return 1.0
        if gap_pct <= ENTRY_EXTREME_GAP_BASE_PCT:
            return 1.0
        full = ENTRY_EXTREME_GAP_FULL_PCT if ENTRY_EXTREME_GAP_FULL_PCT > 0 else 1e-9
        progress = (gap_pct - ENTRY_EXTREME_GAP_BASE_PCT) / full
        mult = 1.0 + progress * (ENTRY_EXTREME_GAP_MAX_MULT - 1.0)
        return round(max(1.0, min(ENTRY_EXTREME_GAP_MAX_MULT, mult)), 4)

    async def _get_24h_ticker_cached(self, client: OKXClient) -> dict | None:
        """Return cached 24h ticker high/low data when the feature is enabled."""
        if not ENTRY_EXTREME_GAP_ADJUST_ENABLED:
            return None
        now = time.time()
        if self._ticker_24h_cache and now - self._ticker_24h_cache_ts < ENTRY_24H_TICKER_CACHE_SEC:
            return self._ticker_24h_cache
        try:
            ticker = await client.get_ticker_24h(INST_ID)
        except Exception as e:
            logger.warning(f"Fetch 24h ticker failed: {e}")
            return None
        self._ticker_24h_cache = ticker
        self._ticker_24h_cache_ts = now
        return ticker

    async def _prepare_entry_extreme_adjustment(self, client: OKXClient, direction: str, entry_price: float) -> None:
        """Lock 24h extreme-distance adjustment for the current first-entry plan."""
        self._entry_extreme_gap_pct = 0.0
        self._entry_extreme_gap_mult = 1.0
        if not ENTRY_EXTREME_GAP_ADJUST_ENABLED or direction not in ("long", "short") or entry_price <= 0:
            return
        ticker = await self._get_24h_ticker_cached(client)
        if not ticker:
            return
        high24 = float(ticker.get("high24h") or 0)
        low24 = float(ticker.get("low24h") or 0)
        if direction == "long" and low24 > 0:
            gap_pct = max(0.0, (entry_price - low24) / entry_price)
        elif direction == "short" and high24 > 0:
            gap_pct = max(0.0, (high24 - entry_price) / entry_price)
        else:
            return
        self._entry_extreme_gap_pct = gap_pct
        self._entry_extreme_gap_mult = self._entry_extreme_multiplier(gap_pct)
        log_check(
            f"24h extreme gap input direction={direction} "
            f"gap={gap_pct:.2%} mult={self._entry_extreme_gap_mult:.2f}"
        )

    def _dynamic_tp_distance(self, avg_entry: float) -> float:
        """Return take-profit distance targeting a margin-return percentage."""
        if avg_entry <= 0:
            return TP_PROFIT_USD
        return round(avg_entry * TP_TARGET_MARGIN_RETURN / LEVER, 2)

    def _tp_price_from_avg(self, direction: str, avg_entry: float) -> float:
        """Return dynamic take-profit price from average entry."""
        distance = self._dynamic_tp_distance(avg_entry)
        return round(avg_entry + distance, 2) if direction == "long" else round(avg_entry - distance, 2)

    def _position_margin_return(self, mark_price: float) -> float:
        """Return current leveraged return from average entry."""
        if not self._state.is_active() or self._state.avg_entry <= 0:
            return 0.0
        if self._state.direction == "long":
            move = (mark_price - self._state.avg_entry) / self._state.avg_entry
        else:
            move = (self._state.avg_entry - mark_price) / self._state.avg_entry
        return move * LEVER

    async def _maybe_update_dynamic_tp(self, client: OKXClient, mark_price: float) -> None:
        """Switch take-profit to a live-price lock when profit momentum stalls."""
        if not DYNAMIC_TP_ENABLED or not self._state.is_active():
            return
        ret = self._position_margin_return(mark_price)
        target_tp = self._tp_price_from_avg(self._state.direction, self._state.avg_entry)

        if self._dynamic_tp_active:
            if ret < DYNAMIC_TP_RESTORE_RETURN:
                self._state.plan_tp_price = target_tp
                self._dynamic_tp_active = False
                log_check(
                    f"Dynamic TP restored: return={ret:.2%} tp={target_tp:.2f}"
                )
                await self._update_tp(client)
                self._save_runtime_state()
            return

        if ret < DYNAMIC_TP_ARM_RETURN or ret >= TP_TARGET_MARGIN_RETURN:
            return
        if self._state.direction == "long" and self._still_making_new_high():
            return
        if self._state.direction == "short" and self._still_making_new_low():
            return

        lock_price = round(mark_price, 2)
        if abs(lock_price - self._state.plan_tp_price) < DYNAMIC_TP_REPRICE_GAP_USD:
            return

        old_tp = self._state.plan_tp_price
        self._state.plan_tp_price = lock_price
        self._dynamic_tp_active = True
        log_action(
            f"Dynamic TP lock: return={ret:.2%} old_tp={old_tp:.2f} "
            f"new_tp={lock_price:.2f}"
        )
        await self._update_tp(client)
        self._save_runtime_state()

    async def _maybe_update_boll_tp_compression(self, client: OKXClient, mark_price: float, last) -> bool:
        """Replace default take-profit when the target band compresses inside it."""
        if not BOLL_TP_COMPRESSION_ENABLED or not self._state.is_active():
            return False
        if self._state.avg_entry <= 0 or self._state.plan_tp_price <= 0:
            return False

        ret = self._position_margin_return(mark_price)
        if ret < BOLL_TP_COMPRESSION_MIN_RETURN:
            return False

        direction = self._state.direction
        tp_price = self._state.plan_tp_price
        upper = float(last["boll_upper"])
        lower = float(last["boll_lower"])

        if direction == "long":
            if upper > tp_price or self._still_making_new_high():
                return False
            lock_price = round(mark_price - BOLL_TP_COMPRESSION_EXIT_OFFSET_USD, 2)
            if lock_price <= self._state.avg_entry:
                return False
        elif direction == "short":
            if lower < tp_price or self._still_making_new_low():
                return False
            lock_price = round(mark_price + BOLL_TP_COMPRESSION_EXIT_OFFSET_USD, 2)
            if lock_price >= self._state.avg_entry:
                return False
        else:
            return False

        if abs(lock_price - tp_price) < DYNAMIC_TP_REPRICE_GAP_USD:
            return False

        self._state.plan_tp_price = lock_price
        self._dynamic_tp_active = True
        log_action(
            f"Boll TP compression: direction={direction} return={ret:.2%} "
            f"old_tp={tp_price:.2f} new_tp={lock_price:.2f} "
            f"boll_lower={lower:.2f} boll_upper={upper:.2f}"
        )
        await self._update_tp(client)
        self._save_runtime_state()
        return True

    async def _maybe_notify_liq_warning(self, mark_price: float) -> None:
        """Send liquidation warning with throttling to avoid message spam."""
        if not self._state.is_active() or self._state.plan_liq_price <= 0:
            self._last_liq_warning_gap_usd = None
            return

        liq = self._state.plan_liq_price
        gap_usd = mark_price - liq if self._state.direction == "long" else liq - mark_price
        gap_pct = gap_usd / mark_price * 100

        if gap_usd <= 0 or gap_usd > LIQ_WARNING_DISTANCE_USD:
            self._last_liq_warning_gap_usd = None
            return

        now = time.time()
        first_warning = self._last_liq_warning_gap_usd is None
        repeat_due = now - self._last_liq_warning_ts >= LIQ_WARNING_REPEAT_SEC
        if not (first_warning or repeat_due):
            return

        self._last_liq_warning_ts = now
        self._last_liq_warning_gap_usd = gap_usd
        await notify_liq_warning(self._state.direction, mark_price, liq, gap_pct, gap_usd)

    def _remember_price(self, mark_price: float):
        """Store recent mark prices for no-new-extreme checks."""
        self._recent_prices.append(mark_price)
        keep = max(NO_NEW_EXTREME_TICKS + 1, 3)
        if len(self._recent_prices) > keep:
            self._recent_prices = self._recent_prices[-keep:]

    async def _maybe_place_probe_batch(self, client: OKXClient, df, last, mark_price: float, equity: float):
        """Place the first probe batch when the current signal qualifies."""
        direction = self._intrabar_probe_direction(df, last, mark_price)

        if direction == "none":
            return

        if not self._entry_max_boll_width_ok(last, mark_price):
            self._log_entry_max_boll_width_skip("probe_skip", last, mark_price)
            return
        if self._entry_trend_filter_blocks(direction, mark_price):
            return

        if direction == "long" and self._still_making_new_low():
            logger.info("Price is still making new lows; skip first long batch")
            return
        if direction == "short" and self._still_making_new_high():
            logger.info("Price is still making new highs; skip first short batch")
            return

        if not self._can_open_new_plan(last["ts"], mark_price):
            return

        if not self._prepare_dynamic_batch_size(0, mark_price):
            return

        boll_std_val = float(df["close"].tail(BOLL_PERIOD).std(ddof=0))
        plan = build_batch_plan(
            direction   = direction,
            first_price = mark_price,
            boll_width  = float(last["boll_width"]),
            boll_mid    = float(last["boll_mid"]),
            boll_lower  = float(last["boll_lower"]),
            boll_upper  = float(last["boll_upper"]),
            boll_std    = boll_std_val,
            equity      = equity,
            fixed_batch_sizes = self._fixed_batch_sizes,
        )
        if not plan.safe:
            logger.warning("Probe signal rejected by risk checks; skip this signal")
            return

        first_order = self._plan_order_at(plan, 0)
        if first_order is None:
            return

        log_check(f"Probe entry prepared direction={direction} first_ref_price={mark_price:.2f}")
        await self._prepare_entry_extreme_adjustment(client, direction, mark_price)
        self._log_plan(first_order, mark_price)
        if await self._place_batch_orders(client, first_order, remaining_batches_placed=False):
            self._probe_kline_ts = last["ts"]
            self._probe_direction = direction
            self._probe_entry_price = mark_price
            self._last_plan_kline_ts = last["ts"]
            self._last_plan_entry_price = mark_price
            self._last_batch_kline_ts = last["ts"]

    def _intrabar_probe_direction(self, df, last, mark_price: float) -> str:
        """Return signal side when mark price is outside the current band."""
        lower = float(last["boll_lower"])
        upper = float(last["boll_upper"])
        if mark_price < lower:
            return "long"
        if mark_price > upper:
            return "short"

        return "none"

    async def _cancel_pending_if_inside_too_long(self, client: OKXClient, last, mark_price: float) -> bool:
        """Cancel a pending entry after enough consecutive inside-band candles."""
        pending_batch = self._state.pending_batch()
        if pending_batch is None:
            self._inside_band_kline_count = 0
            self._last_inside_band_kline_ts = None
            return False

        direction = self._intrabar_probe_direction(None, last, mark_price)
        if direction == self._state.direction:
            self._inside_band_kline_count = 0
            self._last_inside_band_kline_ts = None
            return False

        kline_ts = last["ts"]
        if self._last_inside_band_kline_ts is not None and kline_ts == self._last_inside_band_kline_ts:
            return False
        self._last_inside_band_kline_ts = kline_ts
        self._inside_band_kline_count += 1
        self._save_runtime_state()
        if self._inside_band_kline_count < INSIDE_BAND_CANCEL_KLINES:
            log_check(
                f"Pending order inside band {self._inside_band_kline_count}/"
                f"{INSIDE_BAND_CANCEL_KLINES} klines; keep waiting"
            )
            return False

        log_check(
            f"Pending order stayed inside band for {self._inside_band_kline_count} klines; "
            f"cancel batch {pending_batch.batch_idx + 1}"
        )
        await self._cancel_entry_orders(client)
        self._inside_band_kline_count = 0
        self._last_inside_band_kline_ts = None
        if not self._state.is_active():
            self._last_plan_kline_ts = None
            self._last_batch_kline_ts = None
            self._last_plan_entry_price = 0.0
            self._reset_probe_state()
            self._state.reset()
            self._clear_runtime_state()
        else:
            self._save_runtime_state()
        return True

    async def _cancel_pending_if_width_too_narrow(self, client: OKXClient, last, mark_price: float) -> bool:
        """Cancel a pending entry immediately when Bollinger width is too narrow."""
        pending_batch = self._state.pending_batch()
        if pending_batch is None:
            return False
        if self._boll_width_ok(last, mark_price):
            return False

        self._log_boll_width_skip(
            f"cancel_pending_batch_{pending_batch.batch_idx + 1}",
            last,
            mark_price,
        )
        await self._cancel_entry_orders(client)
        self._inside_band_kline_count = 0
        self._last_inside_band_kline_ts = None
        if not self._state.is_active():
            self._last_plan_kline_ts = None
            self._last_batch_kline_ts = None
            self._last_plan_entry_price = 0.0
            self._reset_probe_state()
            self._state.reset()
            self._clear_runtime_state()
        else:
            self._save_runtime_state()
        return True

    async def _cancel_probe_if_width_too_wide(self, client: OKXClient, last, mark_price: float) -> bool:
        """Cancel an unfilled first batch when Bollinger width becomes too wide."""
        pending_batch = self._state.pending_batch()
        if pending_batch is None or pending_batch.batch_idx != 0 or self._state.is_active():
            return False
        if self._entry_max_boll_width_ok(last, mark_price):
            return False

        self._log_entry_max_boll_width_skip("cancel_pending_first_batch", last, mark_price)
        await self._cancel_entry_orders(client)
        self._inside_band_kline_count = 0
        self._last_inside_band_kline_ts = None
        self._last_plan_kline_ts = None
        self._last_batch_kline_ts = None
        self._last_plan_entry_price = 0.0
        self._reset_probe_state()
        self._state.reset()
        self._clear_runtime_state()
        return True

    def _still_making_new_low(self) -> bool:
        """Return whether recent mark prices are still making new lows."""
        if len(self._recent_prices) < NO_NEW_EXTREME_TICKS + 1:
            return True
        recent = self._recent_prices[-(NO_NEW_EXTREME_TICKS + 1):]
        return recent[-1] <= min(recent[:-1])

    def _still_making_new_high(self) -> bool:
        """Return whether recent mark prices are still making new highs."""
        if len(self._recent_prices) < NO_NEW_EXTREME_TICKS + 1:
            return True
        recent = self._recent_prices[-(NO_NEW_EXTREME_TICKS + 1):]
        return recent[-1] >= max(recent[:-1])

    def _can_open_new_plan(self, kline_ts, entry_price: float) -> bool:
        """Return whether a new first-batch plan can be opened."""
        if self._last_close_kline_ts is not None:
            if kline_ts == self._last_close_kline_ts:
                log_check(f"Close-cooldown active; skip opening on same candle ts={kline_ts}")
                return False
            self._last_close_kline_ts = None
            self._clear_close_cooldown()

        if self._last_plan_kline_ts is not None and kline_ts == self._last_plan_kline_ts:
            log_check(f"This kline already opened one plan; skip signal ts={kline_ts}")
            return False

        if self._last_plan_entry_price > 0:
            gap = abs(entry_price - self._last_plan_entry_price)
            required_gap = self._effective_entry_gap(entry_price)
            if gap < required_gap:
                log_check(
                    f"Entry plan gap below {required_gap:.2f} USDT: "
                    f"last={self._last_plan_entry_price:.2f} current={entry_price:.2f} gap={gap:.2f}"
                )
                return False

        return True

    async def _maybe_place_next_batch(self, client: OKXClient, df, last, equity: float, mark_price: float):
        """Place or maintain the next batch after the first batch has filled."""
        if not self._state.is_active():
            return

        pending_batch = self._state.pending_batch()
        if pending_batch is not None:
            if await self._cancel_pending_if_trend_risk_freeze(client, pending_batch):
                return
            if self._last_batch_kline_ts is not None and last["ts"] == self._last_batch_kline_ts:
                return
            if await self._cancel_pending_if_width_too_narrow(client, last, mark_price):
                return
            if await self._cancel_pending_if_addon_width_too_wide(client, last, mark_price, pending_batch):
                return
            if await self._cancel_pending_if_inside_too_long(client, last, mark_price):
                return
            if await self._cancel_pending_if_addon_guards_fail(client, df, last, pending_batch):
                return
            await self._maybe_reprice_pending_batch(client, df, last, equity, mark_price, pending_batch)
            return

        if not self._boll_width_ok(last, mark_price):
            self._log_boll_width_skip("addon_skip", last, mark_price)
            return

        if TREND_RISK_FREEZE_ADDON_ENABLED and self._trend_risk_guard_active:
            log_check("Trend risk guard freeze; skip new add-on batch")
            return

        trigger_direction = self._intrabar_probe_direction(df, last, mark_price)
        if trigger_direction != self._state.direction:
            return

        if self._state.direction == "long" and self._still_making_new_low():
            log_check("Price is still making new lows; delay next long batch")
            return
        if self._state.direction == "short" and self._still_making_new_high():
            log_check("Price is still making new highs; delay next short batch")
            return

        next_idx = self._state.next_batch_idx()
        if next_idx >= MAX_ENTRY_BATCHES:
            self._state.remaining_batches_placed = True
            return

        if not self._addon_max_boll_width_ok(last, mark_price):
            self._log_addon_max_boll_width_skip(f"addon_skip_batch_{next_idx + 1}", last, mark_price)
            return

        kline_ts = last["ts"]
        if self._last_batch_kline_ts is not None and kline_ts == self._last_batch_kline_ts:
            return

        last_batch = self._state.last_filled_batch()
        if last_batch is None:
            return
        if self._state.direction == "long" and self._still_making_new_low():
            log_check(f"Price is still making new lows; delay long batch {next_idx + 1}")
            return
        if self._state.direction == "short" and self._still_making_new_high():
            log_check(f"Price is still making new highs; delay short batch {next_idx + 1}")
            return

        if not self._prepare_dynamic_batch_size(next_idx, mark_price):
            self._save_runtime_state()
            return

        boll_std_val = float(df["close"].tail(BOLL_PERIOD).std(ddof=0))
        plan = build_batch_plan(
            direction   = self._state.direction,
            first_price = mark_price,
            boll_width  = float(last["boll_width"]),
            boll_mid    = float(last["boll_mid"]),
            boll_lower  = float(last["boll_lower"]),
            boll_upper  = float(last["boll_upper"]),
            boll_std    = boll_std_val,
            equity      = equity,
            max_batch_idx = next_idx,
            fixed_batch_sizes = self._fixed_batch_sizes,
        )
        if not plan.safe:
            logger.warning("Next add-on batch rejected by risk checks; skip")
            return

        next_plan = self._plan_order_at_price(plan, next_idx, mark_price)
        if next_plan is None:
            self._state.remaining_batches_placed = True
            return

        next_order = next_plan.orders[0]
        gap = abs(next_order.price - last_batch.price)
        required_gap = self._effective_entry_gap(mark_price)
        if gap < required_gap:
            log_check(
                f"Next batch gap below {required_gap:.2f} USDT: "
                f"last_fill={last_batch.price:.2f} next={next_order.price:.2f} gap={gap:.2f}"
            )
            return

        if self._state.direction == "long" and mark_price > last_batch.price:
            return
        if self._state.direction == "short" and mark_price < last_batch.price:
            return

        self._update_addon_extreme_guard_from_completed_kline(df, last)
        if not self._addon_extreme_guard_allows(next_order.price, next_idx):
            return
        if not self._addon_risk_budget_allows(next_idx, next_order.price, next_order.sz, mark_price):
            return

        log_check(f"Add-on batch triggered: batch={next_idx + 1} direction={self._state.direction}")
        self._log_plan(next_plan, mark_price)
        if await self._place_batch_orders(client, next_plan, remaining_batches_placed=False):
            self._last_batch_kline_ts = kline_ts

    async def _maybe_reprice_pending_batch(self, client: OKXClient, df, last, equity: float,
                                           mark_price: float, pending_batch):
        """Reprice one pending batch once per candle when plan price moves."""
        kline_ts = last["ts"]
        if self._last_entry_check_kline_ts is not None and kline_ts == self._last_entry_check_kline_ts:
            return
        self._last_entry_check_kline_ts = kline_ts

        last_filled = self._state.last_filled_batch()
        if last_filled is None:
            self._save_runtime_state()
            return

        if not self._boll_width_ok(last, mark_price):
            self._log_boll_width_skip("reprice_skip", last, mark_price)
            self._save_runtime_state()
            return
        if pending_batch.batch_idx > 0 and not self._addon_max_boll_width_ok(last, mark_price):
            self._log_addon_max_boll_width_skip(
                f"reprice_skip_batch_{pending_batch.batch_idx + 1}",
                last,
                mark_price,
            )
            self._save_runtime_state()
            return

        trigger_direction = self._intrabar_probe_direction(df, last, mark_price)
        if trigger_direction != self._state.direction:
            self._save_runtime_state()
            return

        if self._state.direction == "long" and self._still_making_new_low():
            self._save_runtime_state()
            return
        if self._state.direction == "short" and self._still_making_new_high():
            self._save_runtime_state()
            return

        pending_idx = pending_batch.batch_idx
        if pending_idx >= MAX_ENTRY_BATCHES:
            self._state.remaining_batches_placed = True
            return
        if not self._prepare_dynamic_batch_size(pending_idx, mark_price):
            self._save_runtime_state()
            return

        boll_std_val = float(df["close"].tail(BOLL_PERIOD).std(ddof=0))
        plan = build_batch_plan(
            direction   = self._state.direction,
            first_price = mark_price,
            boll_width  = float(last["boll_width"]),
            boll_mid    = float(last["boll_mid"]),
            boll_lower  = float(last["boll_lower"]),
            boll_upper  = float(last["boll_upper"]),
            boll_std    = boll_std_val,
            equity      = equity,
            max_batch_idx = pending_batch.batch_idx,
            fixed_batch_sizes = self._fixed_batch_sizes,
        )
        next_plan = self._plan_order_at_price(plan, pending_batch.batch_idx, mark_price)
        if next_plan is None:
            return

        next_order = next_plan.orders[0]
        gap_from_filled = abs(next_order.price - last_filled.price)
        if gap_from_filled < self._effective_entry_gap(mark_price):
            self._save_runtime_state()
            return

        if abs(next_order.price - pending_batch.price) < REPRICE_GAP_USD:
            self._save_runtime_state()
            return

        self._update_addon_extreme_guard_from_completed_kline(df, last)
        if not self._addon_extreme_guard_allows(next_order.price, pending_batch.batch_idx):
            self._save_runtime_state()
            return
        if not self._addon_risk_budget_allows(
            pending_batch.batch_idx,
            next_order.price,
            next_order.sz,
            mark_price,
        ):
            self._save_runtime_state()
            return

        log_check(
            f"Reprice pending batch {pending_batch.batch_idx + 1}: "
            f"old={pending_batch.price:.2f} new={next_order.price:.2f}"
        )
        await client.cancel_order(INST_ID, pending_batch.ord_id)
        self._state.remove_batch(pending_batch.ord_id)
        if await self._place_batch_orders(client, next_plan, remaining_batches_placed=False):
            self._last_batch_kline_ts = kline_ts
        self._save_runtime_state()

    async def _maybe_reprice_probe_batch(self, client: OKXClient, df, last, equity: float, mark_price: float):
        """Reprice an unfilled first batch once per candle when price moves."""
        pending_batch = self._state.pending_batch()
        if pending_batch is None or pending_batch.batch_idx != 0:
            return

        if await self._cancel_pending_if_width_too_narrow(client, last, mark_price):
            return
        if await self._cancel_probe_if_width_too_wide(client, last, mark_price):
            return

        kline_ts = last["ts"]
        if self._last_entry_check_kline_ts is not None and kline_ts == self._last_entry_check_kline_ts:
            return
        self._last_entry_check_kline_ts = kline_ts

        if await self._cancel_pending_if_inside_too_long(client, last, mark_price):
            return

        if not self._boll_width_ok(last, mark_price):
            self._log_boll_width_skip("probe_reprice_skip", last, mark_price)
            self._save_runtime_state()
            return

        direction = self._intrabar_probe_direction(df, last, mark_price)
        if direction != self._state.direction:
            self._save_runtime_state()
            return
        if self._entry_trend_filter_blocks(direction, mark_price):
            self._save_runtime_state()
            return
        if direction == "long" and self._still_making_new_low():
            self._save_runtime_state()
            return
        if direction == "short" and self._still_making_new_high():
            self._save_runtime_state()
            return

        if not self._prepare_dynamic_batch_size(0, mark_price):
            self._save_runtime_state()
            return

        boll_std_val = float(df["close"].tail(BOLL_PERIOD).std(ddof=0))
        plan = build_batch_plan(
            direction=direction,
            first_price=mark_price,
            boll_width=float(last["boll_width"]),
            boll_mid=float(last["boll_mid"]),
            boll_lower=float(last["boll_lower"]),
            boll_upper=float(last["boll_upper"]),
            boll_std=boll_std_val,
            equity=equity,
            fixed_batch_sizes=self._fixed_batch_sizes,
        )
        if not plan.safe:
            self._save_runtime_state()
            return

        first_plan = self._plan_order_at(plan, 0)
        if first_plan is None:
            self._save_runtime_state()
            return

        next_order = first_plan.orders[0]
        if abs(next_order.price - pending_batch.price) < REPRICE_GAP_USD:
            self._save_runtime_state()
            return

        log_check(
            f"Reprice first batch after kline update "
            f"old={pending_batch.price:.2f} new={next_order.price:.2f}"
        )
        await self._prepare_entry_extreme_adjustment(client, direction, mark_price)
        await client.cancel_order(INST_ID, pending_batch.ord_id)
        self._state.remove_batch(pending_batch.ord_id)
        if await self._place_batch_orders(client, first_plan, remaining_batches_placed=False):
            self._probe_kline_ts = kline_ts
            self._probe_direction = direction
            self._probe_entry_price = mark_price
            self._last_plan_kline_ts = kline_ts
            self._last_plan_entry_price = mark_price
            self._last_batch_kline_ts = kline_ts
        self._save_runtime_state()

    def _plan_order_at(self, plan, batch_idx: int):
        """Return a copy of ``plan`` containing only one batch order."""
        from dataclasses import replace

        orders = [o for o in plan.orders if o.batch_idx == batch_idx]
        if not orders:
            return None
        return replace(
            plan,
            orders=orders,
            total_margin=round(sum(o.margin for o in orders), 2),
        )

    def _plan_order_at_price(self, plan, batch_idx: int, price: float):
        """Return one batch order while using the current trigger price."""
        from dataclasses import replace

        one_order_plan = self._plan_order_at(plan, batch_idx)
        if one_order_plan is None:
            return None

        order = one_order_plan.orders[0]
        price = round(price, 2)
        notional = order.sz * CT_VAL * price
        margin = notional / LEVER
        updated_order = replace(
            order,
            price=price,
            notional=notional,
            margin=margin,
        )
        if plan.direction == "long":
            liq_price = price - ((margin * 0.9) / (order.sz * CT_VAL))
            tp_price = price + self._dynamic_tp_distance(price)
        else:
            liq_price = price + ((margin * 0.9) / (order.sz * CT_VAL))
            tp_price = price - self._dynamic_tp_distance(price)

        return replace(
            one_order_plan,
            orders=[updated_order],
            avg_entry=price,
            liq_price=round(liq_price, 2),
            sl_price=round(liq_price, 2),
            tp_price=round(tp_price, 2),
            total_margin=round(margin, 2),
        )

    # 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁惧墽鎳撻—鍐偓锝庝簼閹癸綁鏌ｉ鐐搭棞闁靛棙甯掗～婵嬫晲閸涱剙顥氶梺璇叉唉椤煤閿曞倸鍨傞悹楦挎閺嗭妇绱掔€ｎ収鍤﹂柡鍐ㄧ墕閻掑灚銇勯幒鎴濐仼缁炬儳銈搁弻鏇熺節韫囨搩娲紓浣叉閸嬫挸鈹戦悩鍨毄濠殿喗鎸冲畷鎰磼濡粯鐝烽梺鍝勬川婵澹曟總鍛婄厽婵せ鍋撴繛浣冲洤鐓濋柛顐犲劜閻撴盯鎮橀悙鎻掆挃婵炲弶娼欓埞鎴︽晬閸曨偄骞嬪銈冨灪閻熲晠骞冮埄鍐╁劅妞ゆ梹鍨濆锕傛⒒?婵犵數濮烽弫鍛婃叏閻戣棄鏋侀柛娑橈攻閸欏繘鏌ｉ幋锝嗩棄闁哄绶氶弻鐔兼⒒鐎靛壊妲紒鐐劤椤兘寮婚敐澶婄疀妞ゆ帊鐒﹂崕鎾绘⒑閹肩偛濡奸柛濠傛健瀵鈽夐姀鈺傛櫇闂佹寧绻傚Λ娑⑺囬妷褏纾藉ù锝呮惈灏忛梺鍛婎殕婵炲﹤顕ｆ繝姘亜闁稿繐鐨烽幏濠氭煟鎼达紕浠涢柣鈩冩礈缁絽螖閸涱喒鎷洪柡澶屽仦婢瑰棝藝閿曞倹鍊垫慨姗嗗亜瀹撳棛鈧鍠涢褔鍩ユ径鎰潊闁绘﹢娼ф慨鍫曟⒒娴ｅ憡鍟為柛鏃€娲橀弲鑸电鐎ｎ亞顦梺缁樺灱婵倝鍩涢幋鐘电＝濞达綀顕栭悞鐣岀磼閻樿櫕宕岄柡宀嬬秮瀵€燁槹闁稿鍨婚埀顒侇問閸犳牠鈥﹂悜钘夌畺闁靛繈鍊曠粈鍌炴煟閹惧磭宀搁柛瀣尵缁辨帒螣閸︻厾鐣炬俊鐐€栭悧妤冩崲閸愵噮鏁傞柣妯款梿閻熼偊鐓ラ柛鎰典簻閻撶喎鈹戦纭锋敾婵＄偠妫勯悾鐑筋敃閿曗偓缁€瀣⒒閸喓鈽夊鐟扮墦濮婂宕掑▎鎴М闁圭厧鐡ㄧ划搴ｆ閻愬瓨濯撮柛鎾村絻濞堛劍绻濋悽闈浶ｉ柤鍦亾閸庮偊姊绘担绋挎毐闁圭⒈鍋婂畷鎰板川婵犲嫷娲稿┑鐘诧工閻楀﹪鍩涢幋锔界厱婵炴垶锕崝鐔虹磼閻樿櫕宕岄柟顔筋殔椤繈顢楁担鍛婄暬闂備浇妗ㄩ悞锕傚礉濞嗗繒鏆﹂梺顒€绉寸粻娑欍亜韫囨挻顥犳繛澶婃健濮婄粯鎷呴悜妯烘畬闂佸憡鑹鹃澶愬箖妤ｅ喚鏁傞柛顐ｇ箓閻庮厽绻濋悽闈浶ｉ柤鐟板⒔缁鎮╃紒妯煎幈闂侀€涘嵆濞佳囧几閻旀悶浜滈柕澶堝劜椤ユ粍銇勯鐐村仴闁硅櫕绮撳Λ鍐ㄢ槈濮楀棙缍岄梻鍌欒兌缁垶骞愭ィ鍐ㄧ獥闁归偊鍘鹃埞宥呪攽閻樺弶绁╅柡浣哥У缁绘繈宕归銏狀潓濠电偛鐗滈崢鍓ф閹惧瓨濯撮柦妯侯槺濮ｃ垻绱撴笟鍥ф珮闁搞劏浜划姘綇閵娧呯槇闂佹悶鍎崝灞解枔閵堝鐓熼柣妯哄级閹牓鏌熺喊鍗炰喊妤犵偛妫濋幃娆撴倻濡攱瀚?闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁惧墽鎳撻—鍐偓锝庝簼閹癸綁鏌ｉ鐐搭棞闁靛棙甯掗～婵嬫晲閸涱剙顥氶梺璇叉唉椤煤閿曞倸鍨傞悹楦挎閺嗭妇绱掔€ｎ収鍤﹂柡鍐ㄧ墕閻掑灚銇勯幒鎴濐仼缁炬儳銈搁弻鏇熺節韫囨搩娲紓浣叉閸嬫挸鈹戦悩鍨毄濠殿喗鎸冲畷鎰磼濡粯鐝烽梺鍝勬川婵澹曟總鍛婄厽婵せ鍋撴繛浣冲洤鐓濋柛顐犲劜閻撴盯鎮橀悙鎻掆挃婵炲弶娼欓埞鎴︽晬閸曨偄骞嬪銈冨灪閻熲晠骞冮埄鍐╁劅妞ゆ梹鍨濆锕傛⒒閸屾艾鈧绮堟笟鈧獮鏍敃閿旂粯鏅為梺鍛婃处閸ㄩ亶宕愰崸妤佺叆闁哄洨鍋涢埀顒€鎽滅划濠氭倷閻戞鍘繝鐢靛仜閻忔繈宕濈€涙绠鹃柟鐐墯閻撳ジ鏌熼鑲╃Ш鐎规洖鐖奸、鏃堝礋椤撶儐妲辨繝鐢靛О閸ㄥジ锝炴径濞掓椽鎮㈡總澶嬬稁缂傚倷鐒﹁摫濠殿垱鎸抽弻褑绠涢幘鍓佹殯闂侀€炲苯澧柨鏇ㄤ邯瀵鏁撻悩鎻掔獩濡炪倖鏌ㄦ晶浠嬫偪閸曨垱鍊甸悷娆忓缁€鍐煕閵婏箑顕滃ǎ鍥э躬閹虫粓妫冨☉姘辩嵁濠电姷鏁告慨鎾疮椤栨績鍙㈠┑鐘垫暩婵挳鎯€婢舵劕绾ч幖瀛樻尭娴滈箖鏌￠崶銉ョ仼缂佺姷濞€楠炴牕菐椤掆偓婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻?

    async def _place_batch_orders(self, client: OKXClient, plan, remaining_batches_placed: bool = True) -> bool:
        """Submit all entry orders in a batch plan."""
        had_batches = bool(self._state.batches)
        if not had_batches and any(bo.batch_idx == 0 for bo in plan.orders):
            await self._record_cycle_start_account_value(client, reason="before_head_order")

        self._state.direction      = plan.direction
        self._state.plan_liq_price = plan.liq_price
        self._state.plan_sl_price  = plan.sl_price
        self._state.plan_tp_price  = plan.tp_price

        side     = "buy"  if plan.direction == "long"  else "sell"
        pos_side = plan.direction
        placed_any = False

        for bo in plan.orders:
            try:
                result = await client.place_order(
                    INST_ID, side, pos_side,
                    sz=str(bo.sz),
                    ord_type="limit",
                    px=str(bo.price),
                )
                ord_id = result.get("ordId", "")
                self._state.add_batch(OpenBatch(
                    batch_idx=bo.batch_idx,
                    ord_id=ord_id,
                    price=bo.price,
                    sz=bo.sz,
                ))
                placed_any = True
                await notify_entry_order(
                    direction=plan.direction,
                    price=bo.price,
                    sz=bo.sz,
                    batch=bo.batch_idx + 1,
                    total=MAX_ENTRY_BATCHES,
                    ord_id=ord_id,
                )
                log_action(f"Batch {bo.batch_idx + 1} order placed price={bo.price} sz={bo.sz} ordId={ord_id}")
            except Exception as e:
                logger.error(f"Batch {bo.batch_idx + 1} order failed: {e}")

        if not placed_any:
            if had_batches:
                logger.warning("New batch order failed; keep current position state")
                return False
            logger.warning("No batch order placed; reset strategy state")
            self._state.reset()
            self._last_batch_kline_ts = None
            self._last_entry_check_kline_ts = None
            self._clear_runtime_state()
            return False

        self._state.remaining_batches_placed = remaining_batches_placed
        self._save_runtime_state()
        return True

    async def _recover_missing_entry_orders(self, client: OKXClient, df, last, equity: float, mark_price: float):
        """Restore a missing next-batch entry order after restart or cancel."""
        if not self._state.is_active():
            return
        if not self._boll_width_ok(last, mark_price):
            return
        if any(not b.filled for b in self._state.batches):
            return
        if self._state.avg_entry <= 0:
            return
        if self._last_batch_kline_ts is not None and last["ts"] == self._last_batch_kline_ts:
            return
        if self._last_recovery_kline_ts is not None and last["ts"] == self._last_recovery_kline_ts:
            return

        trigger_direction = self._intrabar_probe_direction(df, last, mark_price)
        if trigger_direction != self._state.direction:
            return
        if self._state.direction == "long" and self._still_making_new_low():
            logger.info("Recovery add-on skipped: price is still making new lows")
            return
        if self._state.direction == "short" and self._still_making_new_high():
            logger.info("Recovery add-on skipped: price is still making new highs")
            return

        try:
            open_orders = await client.get_open_orders(INST_ID)
        except Exception as e:
            logger.warning(f"Query pending orders before recovery failed: {e}")
            return

        entry_side = "buy" if self._state.direction == "long" else "sell"
        has_exchange_entry = any(
            o.get("side") == entry_side
            and o.get("posSide") == self._state.direction
            and o.get("reduceOnly") != "true"
            for o in open_orders
        )
        if has_exchange_entry:
            self._state.remaining_batches_placed = True
            return

        next_idx = self._state.next_batch_idx()
        if next_idx >= MAX_ENTRY_BATCHES:
            self._state.remaining_batches_placed = True
            self._save_runtime_state()
            return

        if not self._prepare_dynamic_batch_size(next_idx, mark_price):
            self._save_runtime_state()
            return

        boll_std_val = float(df["close"].tail(BOLL_PERIOD).std(ddof=0))
        plan = build_batch_plan(
            direction   = self._state.direction,
            first_price = mark_price,
            boll_width  = float(last["boll_width"]),
            boll_mid    = float(last["boll_mid"]),
            boll_lower  = float(last["boll_lower"]),
            boll_upper  = float(last["boll_upper"]),
            boll_std    = boll_std_val,
            equity      = equity,
            max_batch_idx = next_idx,
            fixed_batch_sizes = self._fixed_batch_sizes,
        )
        if not plan.safe:
            logger.warning("Recovery add-on order rejected by risk checks; skip")
            self._last_recovery_kline_ts = last["ts"]
            return

        recovery_plan = self._plan_order_at_price(plan, next_idx, mark_price)
        if recovery_plan is None:
            self._state.remaining_batches_placed = True
            return

        last_batch = self._state.last_filled_batch()
        if last_batch is not None:
            next_order = recovery_plan.orders[0]
            gap = abs(next_order.price - last_batch.price)
            required_gap = self._effective_entry_gap(mark_price)
            if gap < required_gap:
                logger.info(
                    f"Recovery add-on gap below {required_gap:.2f} USDT: "
                    f"last_fill={last_batch.price:.2f} next={next_order.price:.2f} gap={gap:.2f}"
                )
                self._last_recovery_kline_ts = last["ts"]
                return
            if self._state.direction == "long" and mark_price > last_batch.price:
                self._last_recovery_kline_ts = last["ts"]
                return
            if self._state.direction == "short" and mark_price < last_batch.price:
                self._last_recovery_kline_ts = last["ts"]
                return
            self._update_addon_extreme_guard_from_completed_kline(df, last)
            if not self._addon_extreme_guard_allows(next_order.price, next_idx):
                self._last_recovery_kline_ts = last["ts"]
                return

        logger.info(
            f"Recovered missing add-on order: direction={self._state.direction} "
            f"avg_entry={self._state.avg_entry:.2f} batch={next_idx + 1}"
        )
        self._log_plan(recovery_plan, mark_price)
        if await self._place_batch_orders(client, recovery_plan, remaining_batches_placed=False):
            self._last_batch_kline_ts = last["ts"]
            self._last_recovery_kline_ts = last["ts"]

    # 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁惧墽鎳撻—鍐偓锝庝簼閹癸綁鏌ｉ鐐搭棞闁靛棙甯掗～婵嬫晲閸涱剙顥氶梺璇叉唉椤煤閿曞倸鍨傞悹楦挎閺嗭妇绱掔€ｎ収鍤﹂柡鍐ㄧ墕閻掑灚銇勯幒鎴濐仼缁炬儳銈搁弻鏇熺節韫囨搩娲紓浣叉閸嬫挸鈹戦悩鍨毄濠殿喗鎸冲畷鎰磼濡粯鐝烽梺鍝勬川婵澹曟總鍛婄厽婵せ鍋撴繛浣冲洤鐓濋柛顐犲劜閻撴盯鎮橀悙鎻掆挃婵炲弶娼欓埞鎴︽晬閸曨偄骞嬪銈冨灪閻熲晠骞冮埄鍐╁劅妞ゆ梹鍨濆锕傛⒒?濠电姷鏁告慨鐑藉极閸涘﹥鍙忛柣鎴ｆ閺嬩線鏌涘☉姗堟敾闁告瑥绻橀弻锝夊箣濠垫劖缍楅梺閫炲苯澧柛濠傛健楠炴劖绻濋崘顏嗗骄闂佸啿鎼鍥╃矓椤旈敮鍋撶憴鍕８闁告梹鍨甸锝夊醇閺囩偟顓洪梺缁樼懃閹虫劙鐛姀锛勭瘈闁汇垽娼ф禒锕傛煙缁嬫鐓肩€规洘妞藉畷姗€顢欓懖鈺嬬幢闂備浇顫夐崕鎶芥倶閸儱纾婚柟鎹愬煐閸犲棝鏌涢弴銊ュ妞わ富鍙冨铏规兜閸涱喚褰ч梺瑙勬倐缁犳牕鐣烽敐澶婂窛妞ゆ挆鍕槣闂備線娼ч悧鍡涘箠閹邦喚涓嶅ù鐓庣摠閻撴瑩鏌涢幇顓炵祷妞ゆ帇鍨荤槐鎺楀磼濮樻瘷銏ゆ懚閺嶎厽鐓曟繛鎴濆船閺嬫捇鏌熼柨瀣仢闁哄矉缍侀幃鈺呭礂閸涙澘鐒婚梻浣告啞閺屻劑鎯岄崒姘煎殨闁归棿绀佸Λ姗€骞栫€涙ɑ灏伴柡鍌楀亾濠碉紕鍋戦崐鏍ь潖婵犳艾鐓曢柛顐犲劚閸氬綊鏌ｉ弮鍥仩缁炬儳鍚嬮妵鍕棘閸喒鎸冮柣銏╁灡閻╊垶骞冨Δ浣瑰闁告劑鍔嬪Ч妤呮⒑闁偛鑻晶顖滅磼鐎ｎ偄娴柍銉畵瀹曞爼顢楅埀顒勫磼閵娾晜鈷戞い鎺嗗亾缂佸鏁婚幃锟犳偄閼测晛褰勯梺鎼炲劘閸斿秶绮堥埀顒佺箾鐎电顎撳┑鈥虫喘閸┾偓妞ゆ帒鍠氬鎰箾閸欏鐒鹃柟绛嬪亞缁辨挻鎷呯憴鍕ㄦ嫽闂佸摜濮村锕傛倶閹烘鈷戦柛蹇氬亹閵堟挳鏌￠崨顔剧疄闁轰礁绉撮…銊╁醇閻斿弶瀚奸梻鍌欑贰閸嬪棝宕戝☉銏″殣妞ゆ牗绋掑▍鐘裁归悡搴ｆ憼闁绘挸鍟伴幉绋款煥閸繄顦梺闈浤涢崨顖ｆЦ闂備胶纭堕崜婵堢矙閹寸姷鐜绘俊銈勬缁诲棝鏌曢崼婵囧櫤闁革絾妞介弻?闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁惧墽鎳撻—鍐偓锝庝簼閹癸綁鏌ｉ鐐搭棞闁靛棙甯掗～婵嬫晲閸涱剙顥氶梺璇叉唉椤煤閿曞倸鍨傞悹楦挎閺嗭妇绱掔€ｎ収鍤﹂柡鍐ㄧ墕閻掑灚銇勯幒鎴濐仼缁炬儳銈搁弻鏇熺節韫囨搩娲紓浣叉閸嬫挸鈹戦悩鍨毄濠殿喗鎸冲畷鎰磼濡粯鐝烽梺鍝勬川婵澹曟總鍛婄厽婵せ鍋撴繛浣冲洤鐓濋柛顐犲劜閻撴盯鎮橀悙鎻掆挃婵炲弶娼欓埞鎴︽晬閸曨偄骞嬪銈冨灪閻熲晠骞冮埄鍐╁劅妞ゆ梹鍨濆锕傛⒒閸屾艾鈧绮堟笟鈧獮鏍敃閿旂粯鏅為梺鍛婃处閸ㄩ亶宕愰崸妤佺叆闁哄洨鍋涢埀顒€鎽滅划濠氭倷閻戞鍘繝鐢靛仜閻忔繈宕濈€涙绠鹃柟鐐墯閻撳ジ鏌熼鑲╃Ш鐎规洖鐖奸、鏃堝礋椤撶儐妲辨繝鐢靛О閸ㄥジ锝炴径濞掓椽鎮㈡總澶嬬稁缂傚倷鐒﹁摫濠殿垱鎸抽弻褑绠涢幘鍓佹殯闂侀€炲苯澧柨鏇ㄤ邯瀵鏁撻悩鎻掔獩濡炪倖鏌ㄦ晶浠嬫偪閸曨垱鍊甸悷娆忓缁€鍐煕閵婏箑顕滃ǎ鍥э躬閹虫粓妫冨☉姘辩嵁濠电姷鏁告慨鎾疮椤栨績鍙㈠┑鐘垫暩婵挳鎯€婢舵劕绾ч幖瀛樻尭娴滈箖鏌￠崶銉ョ仼缂佺姷濞€楠炴牕菐椤掆偓婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻?

    def _kline_floor_freq(self) -> str:
        """Return a pandas floor frequency matching the configured bar size."""
        if BAR_15M.endswith("m"):
            return f"{BAR_15M[:-1]}min"
        if BAR_15M.endswith("H"):
            return f"{BAR_15M[:-1]}h"
        return BAR_15M

    def _extract_order_fill(self, order_info: dict, fallback_sz: float, fallback_price: float):
        """Extract real filled size, average fill price, and fill candle."""
        raw_sz = order_info.get("accFillSz") or order_info.get("fillSz") or fallback_sz
        raw_price = order_info.get("avgPx") or order_info.get("fillPx") or order_info.get("px") or fallback_price
        try:
            fill_sz = float(raw_sz or fallback_sz)
        except (TypeError, ValueError):
            fill_sz = fallback_sz
        try:
            fill_price = float(raw_price or fallback_price)
        except (TypeError, ValueError):
            fill_price = fallback_price

        fill_kline_ts = None
        raw_time = order_info.get("fillTime") or order_info.get("uTime")
        try:
            if raw_time:
                fill_kline_ts = pd.to_datetime(int(raw_time), unit="ms").floor(self._kline_floor_freq())
        except Exception:
            fill_kline_ts = None
        return fill_sz, fill_price, fill_kline_ts

    async def _sync_fills(self, client: OKXClient, mark_price: float, kline_ts=None, df=None):
        """Synchronize filled and canceled entry orders from OKX."""
        if not self._state.batches:
            return

        prev_sz = self._state.total_sz
        filled_this_tick = False
        filled_kline_ts = None
        newly_filled = []
        canceled_batches = []
        for batch in self._state.batches:
            if batch.filled:
                continue
            try:
                order_info = await client.get_order(INST_ID, batch.ord_id)
                if order_info.get("state") == "filled":
                    fill_sz, fill_price, fill_ts = self._extract_order_fill(order_info, batch.sz, batch.price)
                    batch_idx = batch.batch_idx
                    self._state.mark_filled(batch.ord_id, fill_sz, fill_price)
                    log_action(
                        f"Entry fill synced batch={batch_idx + 1} ordId={batch.ord_id} "
                        f"fill_px={fill_price:.2f} fill_sz={fill_sz:g} "
                        f"fill_kline={fill_ts or kline_ts or '--'}"
                    )
                    if fill_ts is not None:
                        filled_kline_ts = fill_ts
                    newly_filled.append((batch_idx, fill_ts or kline_ts))
                    filled_this_tick = True
                elif order_info.get("state") in ("canceled", "cancelled"):
                    canceled_batches.append(batch)
                    logger.info(f"Batch {batch.batch_idx + 1} canceled ordId={batch.ord_id}")
            except Exception as e:
                logger.warning(f"Query order {batch.ord_id} failed: {e}")

        for batch in canceled_batches:
            self._state.remove_batch(batch.ord_id)
        if canceled_batches:
            self._save_runtime_state()

        if filled_this_tick:
            self._last_batch_kline_ts = filled_kline_ts or kline_ts
            for batch_idx, fill_ts in newly_filled:
                self._start_addon_extreme_guard_from_fill(
                    df,
                    self._state.direction,
                    batch_idx,
                    fill_ts,
                )
            if kline_ts is not None and self._last_batch_kline_ts == kline_ts:
                logger.info(f"This kline already has an entry fill; wait for next kline ts={kline_ts}")
            elif filled_kline_ts is not None:
                logger.info(f"Synced historical fill at kline={filled_kline_ts}; current kline can continue")

        if self._state.total_sz != prev_sz:
            self._dynamic_tp_active = False
            avg = await self._sync_exchange_position(client)
            if avg <= 0:
                avg = self._recalc_tp()
            await self._replace_exit_orders(client)
            self._save_runtime_state()
            filled_count = len(self._state.filled_batches())
            await notify_open(
                direction=self._state.direction,
                avg_entry=avg,
                sz=self._state.total_sz,
                tp=self._state.plan_tp_price,
                liq=self._state.plan_liq_price,
                batch=filled_count,
                total=len(self._state.batches),
            )

        await self._reset_if_plan_has_no_live_orders(client)

    def _recalc_tp(self) -> float:
        """Recalculate local average entry and take-profit from filled batches."""
        filled = self._state.filled_batches()
        if not filled:
            return 0.0
        total_sz       = sum(b.sz for b in filled)
        weighted_price = sum(b.price * b.sz for b in filled)
        avg_entry      = weighted_price / total_sz
        self._state.avg_entry = avg_entry
        self._dynamic_tp_active = False
        self._state.plan_tp_price = self._tp_price_from_avg(self._state.direction, avg_entry)
        log_check(f"Average entry={avg_entry:.2f} new_tp={self._state.plan_tp_price}")
        return avg_entry

    async def _replace_exit_orders(self, client: OKXClient):
        """Cancel old exit orders and place fresh take-profit and stop orders."""
        await self._cancel_exchange_exit_orders(client)
        self._state.tp_ord_id = None
        self._state.sl_ord_id = None
        await self._update_tp(client)
        await self._update_sl(client)
        self._save_runtime_state()

    async def _sync_exchange_position(self, client: OKXClient) -> float:
        """Synchronize local position fields from the exchange position."""
        pos = await client.get_position(INST_ID)
        if pos is None or float(pos.get("pos", 0)) == 0:
            return 0.0

        avg_entry = float(pos.get("avgPx") or 0)
        liq_price = float(pos.get("liqPx") or 0)
        total_sz = float(pos.get("pos", 0))
        pos_side = pos.get("posSide") or self._state.direction

        self._state.direction = pos_side
        self._state.update_position(total_sz, avg_entry, liq_price)
        self._seed_existing_position_batch()

        if avg_entry > 0 and (not self._dynamic_tp_active or self._state.plan_tp_price <= 0):
            self._state.plan_tp_price = self._tp_price_from_avg(pos_side, avg_entry)

        log_check(
            f"Exchange position synced avg={avg_entry:.2f} sz={total_sz:g} "
            f"real_liq={liq_price:.2f} new_tp={self._state.plan_tp_price:.2f}"
        )
        self._log_runtime_state_summary("Post-exchange sync state")
        return avg_entry

    def _seed_existing_position_batch(self):
        """Create a synthetic filled batch for a pre-existing position."""
        if not self._state.is_active():
            return
        if self._state.batches:
            return
        if self._state.avg_entry <= 0:
            return

        self._state.add_batch(OpenBatch(
            batch_idx=0,
            ord_id="existing-position",
            price=self._state.avg_entry,
            sz=self._state.total_sz,
            filled=True,
        ))
        self._probe_entry_price = self._state.avg_entry
        self._last_plan_entry_price = self._state.avg_entry
        logger.info(
            f"Recovered local first batch from existing position "
            f"avg_entry={self._state.avg_entry:.2f} sz={self._state.total_sz}"
        )

    async def _update_tp(self, client: OKXClient):
        """Place the current reduce-only take-profit limit order."""
        pos_side   = self._state.direction
        close_side = "sell" if pos_side == "long" else "buy"

        if self._state.total_sz <= 0 or self._state.plan_tp_price <= 0:
            return

        if self._state.tp_ord_id:
            try:
                await client.cancel_order(INST_ID, self._state.tp_ord_id)
            except Exception as e:
                logger.warning(f"Cancel old take-profit order failed: {e}")
            self._state.tp_ord_id = None

        try:
            r = await client.place_order(
                INST_ID, close_side, pos_side,
                sz=str(self._state.total_sz),
                ord_type="limit",
                px=str(self._state.plan_tp_price),
                reduce_only=True,
            )
            self._state.tp_ord_id = r.get("ordId", "")
            log_action(
                f"止盈挂单 price={self._state.plan_tp_price} sz={self._state.total_sz}"
            )
        except Exception as e:
            logger.error(f"Place take-profit order failed: {e}")

        log_check(f"Current liquidation price={self._state.plan_liq_price} final risk boundary")

    async def _update_sl(self, client: OKXClient):
        """Place the current reduce-only stop order."""
        pos_side = self._state.direction
        if self._state.total_sz <= 0 or pos_side not in ("long", "short"):
            return

        close_side = "sell" if pos_side == "long" else "buy"
        sl_price = 0.0
        stop_mode = "liquidation_guard"
        target_loss = 0.0

        if COPY_FIXED_LOSS_STOP_ENABLED and self._state.avg_entry > 0:
            target_loss = self._fixed_loss_target_usdt()
            if target_loss > 0:
                sl_price = self._fixed_loss_stop_price(
                    pos_side,
                    self._state.avg_entry,
                    self._state.total_sz,
                )
                stop_mode = "fixed_loss"

        liq_guard_price = 0.0
        if self._state.plan_liq_price > 0:
            if pos_side == "long":
                liq_guard_price = self._state.plan_liq_price + LIQ_STOP_OFFSET_USD
            else:
                liq_guard_price = self._state.plan_liq_price - LIQ_STOP_OFFSET_USD

        if sl_price <= 0 and liq_guard_price > 0:
            sl_price = liq_guard_price

        if sl_price <= 0:
            logger.warning("Invalid stop-loss price; skip stop order")
            return

        if liq_guard_price > 0:
            if pos_side == "long" and sl_price < liq_guard_price:
                sl_price = liq_guard_price
                stop_mode = "fixed_loss_clamped_to_liq_guard"
            elif pos_side == "short" and sl_price > liq_guard_price:
                sl_price = liq_guard_price
                stop_mode = "fixed_loss_clamped_to_liq_guard"

        sl_price = round(sl_price, 2)
        if sl_price <= 0:
            logger.warning(f"Invalid stop-loss price; skip sl={sl_price}")
            return

        if pos_side == "long":
            estimated_loss = max((self._state.avg_entry - sl_price) * self._state.total_sz * CT_VAL, 0.0)
        else:
            estimated_loss = max((sl_price - self._state.avg_entry) * self._state.total_sz * CT_VAL, 0.0)

        if self._state.sl_ord_id:
            await client.cancel_algo_order(INST_ID, self._state.sl_ord_id)
            self._state.sl_ord_id = None

        try:
            r = await client.place_algo_order(
                INST_ID,
                close_side,
                pos_side,
                sz=str(self._state.total_sz),
                sl_trigger_px=str(sl_price),
            )
            self._state.sl_ord_id = r.get("algoId", "")
            self._state.plan_sl_price = sl_price
            if stop_mode.startswith("fixed_loss"):
                log_action(
                    f"Fixed-loss stop order trigger={sl_price} mode={stop_mode} "
                    f"target_loss={target_loss:.2f} est_loss={estimated_loss:.2f} "
                    f"avg={self._state.avg_entry:.2f} sz={self._state.total_sz}"
                )
            else:
                log_action(
                    f"Liquidation stop order trigger={sl_price} "
                    f"liq={self._state.plan_liq_price} sz={self._state.total_sz}"
                )
        except Exception as e:
            logger.error(f"Place stop-loss order failed: {e}")

    async def _fetch_actual_close_pnl(
        self,
        client: OKXClient,
        direction: str,
        total_sz: float,
        close_ord_id: str = "",
    ) -> dict | None:
        """Return realized close details from recent close fills when available."""
        if direction not in ("long", "short") or total_sz <= 0:
            return None
        close_side = "sell" if direction == "long" else "buy"
        end_ms = int(time.time() * 1000)
        begin_ms = end_ms - 10 * 60 * 1000
        try:
            fills = await client.get_fills_history(INST_ID, begin=begin_ms, end=end_ms, limit=100)
        except Exception as e:
            logger.warning(f"Fetch close fills failed; fallback to balance diff: {e}")
            return None

        matched = []
        if close_ord_id:
            matched = [fill for fill in fills if str(fill.get("ordId", "")) == str(close_ord_id)]

        if not matched:
            close_fills = [
                fill for fill in fills
                if fill.get("side") == close_side and fill.get("posSide") == direction
            ]
            close_fills.sort(key=lambda fill: int(fill.get("fillTime") or fill.get("ts") or 0), reverse=True)
            filled_sz = 0.0
            for fill in close_fills:
                matched.append(fill)
                try:
                    filled_sz += float(fill.get("fillSz") or fill.get("sz") or 0)
                except (TypeError, ValueError):
                    pass
                if filled_sz + CONTRACT_STEP >= total_sz:
                    break

        if not matched:
            logger.warning("No close fills found; fallback to balance diff for capital rebalance")
            return None

        gross_pnl = 0.0
        total_fill_sz = 0.0
        weighted_px = 0.0
        total_fee = 0.0
        for fill in matched:
            try:
                fill_pnl = float(fill.get("fillPnl") or 0)
            except (TypeError, ValueError):
                fill_pnl = 0.0
            try:
                fee = float(fill.get("fee") or 0)
            except (TypeError, ValueError):
                fee = 0.0
            try:
                fill_sz = float(fill.get("fillSz") or fill.get("sz") or 0)
            except (TypeError, ValueError):
                fill_sz = 0.0
            try:
                fill_px = float(fill.get("fillPx") or fill.get("px") or 0)
            except (TypeError, ValueError):
                fill_px = 0.0
            gross_pnl += fill_pnl
            total_fee += fee
            if fill_sz > 0 and fill_px > 0:
                total_fill_sz += fill_sz
                weighted_px += fill_sz * fill_px

        net_pnl = round(gross_pnl + total_fee, 4)
        avg_fill_px = round(weighted_px / total_fill_sz, 4) if total_fill_sz > 0 else 0.0
        log_action(
            f"Actual close PnL from fills net_pnl={net_pnl:+.4f} USDT "
            f"gross_pnl={gross_pnl:+.4f} fee={total_fee:+.4f} "
            f"avg_px={avg_fill_px or '--'} sz={total_fill_sz or '--'} "
            f"fills={len(matched)} ordId={close_ord_id or '--'}"
        )
        return {
            "pnl": net_pnl,
            "avg_price": avg_fill_px,
            "sz": round(total_fill_sz, 8),
            "fee": round(total_fee, 4),
            "gross_pnl": round(gross_pnl, 4),
            "fills": len(matched),
        }

    # 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁惧墽鎳撻—鍐偓锝庝簼閹癸綁鏌ｉ鐐搭棞闁靛棙甯掗～婵嬫晲閸涱剙顥氶梺璇叉唉椤煤閿曞倸鍨傞悹楦挎閺嗭妇绱掔€ｎ収鍤﹂柡鍐ㄧ墕閻掑灚銇勯幒鎴濐仼缁炬儳銈搁弻鏇熺節韫囨搩娲紓浣叉閸嬫挸鈹戦悩鍨毄濠殿喗鎸冲畷鎰磼濡粯鐝烽梺鍝勬川婵澹曟總鍛婄厽婵せ鍋撴繛浣冲洤鐓濋柛顐犲劜閻撴盯鎮橀悙鎻掆挃婵炲弶娼欓埞鎴︽晬閸曨偄骞嬪銈冨灪閻熲晠骞冮埄鍐╁劅妞ゆ梹鍨濆锕傛⒒?濠电姷鏁告慨鐑藉极閸涘﹥鍙忛柣鎴ｆ閺嬩線鏌涘☉姗堟敾闁告瑥绻橀弻锝夊箣濠垫劖缍楅梺閫炲苯澧柛濠傛健楠炴劖绻濋崘顏嗗骄闂佸啿鎼鍥╃矓椤旈敮鍋撶憴鍕８闁告梹鍨甸锝夊醇閺囩偟顓洪梺缁樼懃閹虫劙鐛姀锛勭瘈闁汇垽娼ф禒锕傛煙缁嬫鐓肩€规洘妞藉畷姗€顢欓懖鈺嬬幢闂備浇顫夐崕鎶芥倶閸儱纾婚柟鎹愬煐閸犲棝鏌涢弴銊ュ妞わ富鍙冨铏规兜閸涱喚褰ч梺瑙勬倐缁犳牕鐣烽敐澶婂窛妞ゆ挆鍕槣闂備線娼ч悧鍡涘箠閹邦喚涓嶅ù鐓庣摠閻撴瑩鏌涢幇顓炵祷妞ゆ帇鍨荤槐鎺楀磼濮樻瘷銏ゆ懚閺嶎厽鐓曟繛鎴濆船閺嬫捇鏌熼柨瀣仢闁哄矉缍侀幃鈺呭礂閸涙澘鐒婚梻浣告啞閺屻劑鎯岄崒姘煎殨闁归棿绀佸Λ姗€骞栫€涙ɑ灏伴柡鍌楀亾濠碉紕鍋戦崐鏍ь潖婵犳艾鐓曢柛顐犲劚閸氬綊鏌ｉ弮鍥仩缁炬儳鍚嬮妵鍕籍閸屾瀚涢梺缁樻崄閸嬫劙鍩€椤掍緡鍟忛柛鐘崇☉閳绘柨鈽夊鍛綍闂傚倸鍊搁崐鎼佹偋婵犲嫮鐭欓柟鎯у閻挻绻涘顔荤凹闁绘挻绋戦湁闁挎繂娲﹂崵鈧繝娈垮枛閻楀繘鍩€椤掆偓閻忔艾顭垮Ο灏栧亾濮樼厧澧查柣蹇斿笒閳规垿鎮欑捄铏规缂備緡鍣崹鎯版＂濠电偞鍨惰彜闁衡偓娴犲鐓熸俊顖濇娴犳盯鏌￠崱蹇旀珚闁哄本绋撻埀顒婄秵閸嬪棗煤閹绢喗瀵犳繝闈涙储娴滄粓鏌熼幆褍鑸归柣蹇婃櫊閺屾盯濡搁妷銉㈠亾閹间焦绠掗梻浣虹帛閿氭俊顖氾躬瀹曟洝绠涘☉娆戝弮闂佸憡鍔︽禍婊堝几濞戙垺鐓涢悘鐐额嚙婵倿鏌熼鍝勭伈鐎规洦鍋婂畷鐔煎箣濞嗗繒浼勭紓浣介哺鐢繝宕洪埀顒併亜閹烘垵鈧敻宕戦幘缁樻櫜閹肩补鍓濋悘宥夋⒑閹惰姤鏁遍悽顖ょ節瀵鈽夐姀鈺傛櫇闂侀潧鐗嗛幊蹇涙倶娓氣偓濮婃椽妫冨☉娆樻！闁汇埄鍨辩敮鈥筹耿娓氣偓濮婅櫣绱掑鍫滅返闂佺顑呴幊搴ㄥ煝瀹ュ棛绡€闁告劏鏅涘鎸庣節閻㈤潧孝闁瑰啿绻橀、鏃堟偐缂佹鍘垫俊鐐差儏妤犳悂鍩㈤崼銉︾厱闁靛绠戦崝銈夋煟閿濆洤鍘寸€规洖鐖奸弫鍌炴寠婢跺苯骞堢紓鍌氬€搁崐鎼佸磹閹间礁纾瑰瀣婵ジ鏌＄仦璇插姎缁炬儳顭烽弻鐔煎礈瑜嶆禒娲煃瑜滈崜姘辨暜閹烘缍栨繝闈涱儐閺呮煡鏌涘☉鍗炲妞ゃ儲鑹鹃埞?闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁惧墽鎳撻—鍐偓锝庝簼閹癸綁鏌ｉ鐐搭棞闁靛棙甯掗～婵嬫晲閸涱剙顥氶梺璇叉唉椤煤閿曞倸鍨傞悹楦挎閺嗭妇绱掔€ｎ収鍤﹂柡鍐ㄧ墕閻掑灚銇勯幒鎴濐仼缁炬儳銈搁弻鏇熺節韫囨搩娲紓浣叉閸嬫挸鈹戦悩鍨毄濠殿喗鎸冲畷鎰磼濡粯鐝烽梺鍝勬川婵澹曟總鍛婄厽婵せ鍋撴繛浣冲洤鐓濋柛顐犲劜閻撴盯鎮橀悙鎻掆挃婵炲弶娼欓埞鎴︽晬閸曨偄骞嬪銈冨灪閻熲晠骞冮埄鍐╁劅妞ゆ梹鍨濆锕傛⒒閸屾艾鈧绮堟笟鈧獮鏍敃閿旂粯鏅為梺鍛婃处閸ㄩ亶宕愰崸妤佺叆闁哄洨鍋涢埀顒€鎽滅划濠氭倷閻戞鍘繝鐢靛仜閻忔繈宕濈€涙绠鹃柟鐐墯閻撳ジ鏌熼鑲╃Ш鐎规洖鐖奸、鏃堝礋椤撶儐妲辨繝鐢靛О閸ㄥジ锝炴径濞掓椽鎮㈡總澶嬬稁缂傚倷鐒﹁摫濠殿垱鎸抽弻褑绠涢幘鍓佹殯闂侀€炲苯澧柨鏇ㄤ邯瀵鏁撻悩鎻掔獩濡炪倖鏌ㄦ晶浠嬫偪閸曨垱鍊甸悷娆忓缁€鍐煕閵婏箑顕滃ǎ鍥э躬閹虫粓妫冨☉姘辩嵁濠电姷鏁告慨鎾疮椤栨績鍙㈠┑鐘垫暩婵挳鎯€婢舵劕绾ч幖瀛樻尭娴滈箖鏌￠崶銉ョ仼缂佺姷濞€楠炴牕菐椤掆偓婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛?

    async def _check_position_closed(self, client: OKXClient, mark_price: float, kline_ts=None):
        """Detect external position close and reset local state."""
        pos = await client.get_position(INST_ID)
        if pos is None or float(pos.get("pos", 0)) == 0:
            self._last_close_kline_ts = kline_ts
            self._save_close_cooldown()
            # 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧湱鈧懓瀚崳纾嬨亹閹烘垹鍊為悷婊冪箻瀵娊鏁冮崒娑氬幈濡炪値鍘介崹鍨濠靛鐓曟繛鍡楃箳缁犳娊鏌嶈閸撴繈锝炴径濞掓椽寮介‖鈩冩そ閺佸啴宕掗妶鍡樻珖濠电偛顕慨鎾敄閸℃稒鍋傞煫鍥ㄧ⊕閻撴洘銇勯幇鍓佹偧缂佺姵鐗曢…璺ㄦ崉閸濆嫷浠鹃梺闈涙搐鐎氫即銆侀弴銏℃櫜闁糕剝鐟Σ褰掓⒒娴ｅ憡鎯堥柣顓烆槺閹广垹鈹戦崱娆愭闂佸壊鍋呭ú鏍ф暜闂備線娼ч敍蹇涘磼濠婂嫸绱￠梻鍌氬€搁崐鐑芥嚄閸洍鈧箓宕奸妷顔芥櫈闂佺鐬奸崑娑㈡偪閻愵剛绠鹃柟瀛樼懃閻忊晠鏌ｉ幘杈捐€块柡宀€鍠愬蹇斻偅閸愨晩鈧秹姊虹粙娆惧剱闁告梹鐟╅獮鍐ㄎ旈崨顓熷祶濡炪倖鎸鹃崐顐﹀鎺虫禍婊堟煏婢舵稑顩紒鐘靛仱閺屸€崇暆鐎ｎ剛蓱闂佽鍨卞Λ鍐╂叏閳ь剟鏌ㄥ┑鍡樺婵☆偆鍠愭穱濠囨倷椤忓嫧鍋撹缁辨挸顫濈捄铏诡攨闂佹儳娴氶崑鍌滄崲閸℃稒鐓熼柕蹇嬪焺閻掑墽绱掗埀顒傗偓锝庡亖娴滄粓鏌″鍐ㄥ闁汇劍鍨堕妵鍕棘閸柭ゅ惈闂佽鍠楅〃濠囨偘椤曗偓瀹曞綊顢欓悡搴經濠电姷鏁搁崑鐐电矈閹绢喖鐤炬繝闈涚墕閸ㄦ繈鏌ㄥ┑鍡樺晵闁哄啫鐗嗗婵囥亜閺冨浂娼愰悗姘冲亹缁辨捇宕掑▎鎴М濡炪倖鍨甸悧鍡涘煝閺冨牆鍗抽柕蹇曞Х閻ゅ洦绻濋姀锝呯厫闁告梹鐗犻幃锟犲即閵忥紕鍘繝銏ｅ煐缁嬫捇宕氶弶搴撴斀闁炽儴娅曢崑銉╂煛鐏炲墽鈽夐摶锝夋煕閿旇骞橀柣鎾存尭閳规垿顢欑涵鐑界反濠电偛鎷戠紞渚€宕洪埀顒併亜閹哄棗浜惧┑鐘亾閺夊牄鍔嶉崣蹇涙煟閵忋埄鐒炬潻婵嬫⒑閸涘﹤濮﹂柛鐘愁殜閹繝鎮╃紒妯衡偓鐢告煕閿旇骞栫€涙繈姊虹紒妯诲鞍闁告梹鐟╁濠氭晸閻樻彃绐涘銈嗘尵婵挳鎮￠悢鍏煎€垫繛鍫濈仢濞呮﹢鏌涢敐蹇曞埌闁伙絿鍏橀獮鎺楀箣椤撶姴寮抽梻浣告啞缁矂宕悧鍫熷劅?
            filled = self._state.filled_batches()
            avg_entry = self._state.avg_entry
            total_sz = self._state.total_sz
            direction = self._state.direction
            close_price = self._state.plan_tp_price if self._state.plan_tp_price > 0 else mark_price
            if filled and avg_entry <= 0:
                total_sz       = sum(b.sz for b in filled)
                avg_entry      = sum(b.price * b.sz for b in filled) / total_sz
            close_ord_id = self._state.tp_ord_id or ""
            actual_close = await self._fetch_actual_close_pnl(client, direction, total_sz, close_ord_id)
            equity_close = await self._fetch_account_equity_close_pnl(client)
            actual_pnl = equity_close["pnl"] if equity_close else (actual_close["pnl"] if actual_close else None)
            pnl_source = "account_equity_diff" if equity_close else ("fills" if actual_close else "estimate")
            if total_sz > 0 and avg_entry > 0:
                if actual_close:
                    display_close_price = actual_close["avg_price"] or close_price
                    display_sz = actual_close["sz"] or total_sz
                    pnl = actual_pnl
                    log_action(
                        f"Position closed {direction} avg_entry={avg_entry:.2f} "
                        f"close_avg={display_close_price:.2f} sz={display_sz:.2f} "
                        f"actual_pnl={pnl:+.4f} USDT fills={actual_close['fills']} "
                        f"fee={actual_close['fee']:+.4f} source={pnl_source}"
                    )
                elif equity_close:
                    display_close_price = close_price
                    display_sz = total_sz
                    pnl = actual_pnl
                    log_action(
                        f"Position closed {direction} avg_entry={avg_entry:.2f} "
                        f"close_ref={close_price:.2f} sz={total_sz:.2f} "
                        f"actual_pnl={pnl:+.4f} USDT source={pnl_source}"
                    )
                else:
                    from src.config import CT_VAL
                    display_close_price = close_price
                    display_sz = total_sz
                    if direction == "long":
                        pnl = (close_price - avg_entry) * total_sz * CT_VAL
                    else:
                        pnl = (avg_entry - close_price) * total_sz * CT_VAL
                    log_action(
                        f"Position closed {direction} avg_entry={avg_entry:.2f} "
                        f"close_ref={close_price:.2f} sz={total_sz:.2f} "
                        f"estimated_pnl={pnl:+.4f} USDT"
                    )
                dashboard.state.add_trade(
                    action="close_long" if direction == "long" else "close_short",
                    price=display_close_price,
                    sz=display_sz,
                    pnl=pnl,
                )
                await notify_close(direction, avg_entry, display_close_price, pnl, display_sz)

            await self._cancel_entry_orders(client)
            await self._cancel_exchange_exit_orders(client)
            await self._cancel_exit_orders(client)
            log_action("Position state reset")
            self._last_plan_kline_ts = None
            self._last_batch_kline_ts = None
            self._last_plan_entry_price = 0.0
            self._reset_probe_state()
            self._state.reset()
            self._clear_runtime_state()
            actual_profit = await self._rebalance_accounts(client, actual_pnl=actual_pnl)
            if actual_profit:
                log_action(f"Capital actual PnL confirmed {actual_profit:+.4f} USDT")
            await self._calibrate_capital_after_close(client)
            await self._init_fixed_batch_sizes(client)

    # 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁惧墽鎳撻—鍐偓锝庝簼閹癸綁鏌ｉ鐐搭棞闁靛棙甯掗～婵嬫晲閸涱剙顥氶梺璇叉唉椤煤閿曞倸鍨傞悹楦挎閺嗭妇绱掔€ｎ収鍤﹂柡鍐ㄧ墕閻掑灚銇勯幒鎴濐仼缁炬儳銈搁弻鏇熺節韫囨搩娲紓浣叉閸嬫挸鈹戦悩鍨毄濠殿喗鎸冲畷鎰磼濡粯鐝烽梺鍝勬川婵澹曟總鍛婄厽婵せ鍋撴繛浣冲洤鐓濋柛顐犲劜閻撴盯鎮橀悙鎻掆挃婵炲弶娼欓埞鎴︽晬閸曨偄骞嬪銈冨灪閻熲晠骞冮埄鍐╁劅妞ゆ梹鍨濆锕傛⒒?闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁炬儳婀遍埀顒傛嚀鐎氼參宕崇壕瀣ㄤ汗闁圭儤鍨归崐鐐差渻閵堝棗绗掓い锔垮嵆瀵煡顢旈崼鐔叉嫼闂佸憡绻傜€氼噣鍩㈡径鎰厱婵☆垱浜介崑銏⑩偓瑙勬礃鐢剝淇婂宀婃Ъ闂佸摜濮甸崝娆撳蓟閿濆憘鏃堝焵椤掑嫭鍋嬮柛鈩冪懅缁犳棃鏌熼悜姗嗘畷闁绘挻娲熼弻鏇熺箾閸喖濮庨梺閫炲苯澧い顓犲厴閻涱喗寰勯幇顒傤啇婵炶揪绲块幊鎾寸闁秵鈷戦柛鎾村絻娴滄繄绱掔拠鎻掆偓鍧楃嵁閸℃稒鍊烽柣鎴炃氶幏娲⒑绾懎浜归柛瀣洴瀹曟繈骞橀鐣屽幈闂佽鍎抽顓㈠箠閸ヮ剚鐓欓梺鍨儏閻忔挳鏌熼鍝勭伈鐎规洘顨婇幊鏍煛娴ｇ顩┑鐘垫暩婵即宕规總绋挎槬闁哄稁鍋嗛惌娆撴煙閸撗呭笡闁稿鏅犻弻娑樜旈崘褏闂?闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁惧墽鎳撻—鍐偓锝庝簼閹癸綁鏌ｉ鐐搭棞闁靛棙甯掗～婵嬫晲閸涱剙顥氶梺璇叉唉椤煤閿曞倸鍨傞悹楦挎閺嗭妇绱掔€ｎ収鍤﹂柡鍐ㄧ墕閻掑灚銇勯幒鎴濐仼缁炬儳銈搁弻鏇熺節韫囨搩娲紓浣叉閸嬫挸鈹戦悩鍨毄濠殿喗鎸冲畷鎰磼濡粯鐝烽梺鍝勬川婵澹曟總鍛婄厽婵せ鍋撴繛浣冲洤鐓濋柛顐犲劜閻撴盯鎮橀悙鎻掆挃婵炲弶娼欓埞鎴︽晬閸曨偄骞嬪銈冨灪閻熲晠骞冮埄鍐╁劅妞ゆ梹鍨濆锕傛⒒閸屾艾鈧绮堟笟鈧獮鏍敃閿旂粯鏅為梺鍛婃处閸ㄩ亶宕愰崸妤佺叆闁哄洨鍋涢埀顒€鎽滅划濠氭倷閻戞鍘繝鐢靛仜閻忔繈宕濈€涙绠鹃柟鐐墯閻撳ジ鏌熼鑲╃Ш鐎规洖鐖奸、鏃堝礋椤撶儐妲辨繝鐢靛О閸ㄥジ锝炴径濞掓椽鎮㈡總澶嬬稁缂傚倷鐒﹁摫濠殿垱鎸抽弻褑绠涢幘鍓佹殯闂侀€炲苯澧柨鏇ㄤ邯瀵鏁撻悩鎻掔獩濡炪倖鏌ㄦ晶浠嬫偪閸曨垱鍊甸悷娆忓缁€鍐煕閵婏箑顕滃ǎ鍥э躬閹虫粓妫冨☉姘辩嵁濠电姷鏁告慨鎾疮椤栨績鍙㈠┑鐘垫暩婵挳鎯€婢舵劕绾ч幖瀛樻尭娴滈箖鏌￠崶銉ョ仼缂佺姷濞€楠炴牕菐椤掆偓婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛?

    async def _sync_state(self, client: OKXClient):
        """Reconcile local state with the exchange on startup."""
        pos = await client.get_position(INST_ID)
        if pos and float(pos.get("pos", 0)) != 0:
            pos_side = pos.get("posSide", "")
            sz       = float(pos.get("pos", 0))
            logger.info(f"Existing position detected: {pos_side} {sz} contracts; continue monitoring")
            self._state.direction = pos_side
            await self._sync_exchange_position(client)
            await self._refresh_filled_batches_from_orders(client)
            self._repair_filled_batches_after_restart()
            if self._sync_known_batch_sizes():
                self._save_runtime_state()
            if self._state.cycle_start_account_value <= 0:
                await self._record_cycle_start_account_value(client, reason="restart_existing_position")
            await self._reconcile_entry_orders_after_restart(client)
            await self._replace_exit_orders(client)
        else:
            await self._cancel_exchange_open_orders(client)
            self._reset_probe_state()
            self._state.reset()
            self._clear_runtime_state()
            if not self._fixed_batch_sizes:
                await self._init_fixed_batch_sizes(client)
            logger.info("No existing position; strategy ready")

    # 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁惧墽鎳撻—鍐偓锝庝簼閹癸綁鏌ｉ鐐搭棞闁靛棙甯掗～婵嬫晲閸涱剙顥氶梺璇叉唉椤煤閿曞倸鍨傞悹楦挎閺嗭妇绱掔€ｎ収鍤﹂柡鍐ㄧ墕閻掑灚銇勯幒鎴濐仼缁炬儳銈搁弻鏇熺節韫囨搩娲紓浣叉閸嬫挸鈹戦悩鍨毄濠殿喗鎸冲畷鎰磼濡粯鐝烽梺鍝勬川婵澹曟總鍛婄厽婵せ鍋撴繛浣冲洤鐓濋柛顐犲劜閻撴盯鎮橀悙鎻掆挃婵炲弶娼欓埞鎴︽晬閸曨偄骞嬪銈冨灪閻熲晠骞冮埄鍐╁劅妞ゆ梹鍨濆锕傛⒒?缂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁炬儳缍婇弻鐔兼⒒鐎靛壊妲紒鐐劤缂嶅﹪寮婚敐澶婄闁挎繂鎲涢幘缁樼厱濠电姴鍊归崑銉╂煛鐏炶濮傜€殿喗鎸抽幃娆徝圭€ｎ亙澹曢悷婊呭鐢帞澹曢崹顔规斀闁绘ê寮舵径鍕煃闁垮鐏﹂柕鍥у楠炴帡宕卞鎯ь棜缂傚倸鍊风粈渚€藝椤栫儐鏁嬫い鎾跺Т閸ㄦ繈鏌涢鐘插姎缂佲偓閸愵喗鐓冮柣鐔诲焽娴犳帞绱掗妸銉吋婵﹥妞藉畷顐﹀礋椤愶絾顔勬繝鐢靛仩椤曟粎绮婚幘瑙勶紓濠电姰鍨奸崺鏍礉閺嶎厽鍋?闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁惧墽鎳撻—鍐偓锝庝簼閹癸綁鏌ｉ鐐搭棞闁靛棙甯掗～婵嬫晲閸涱剙顥氶梺璇叉唉椤煤閿曞倸鍨傞悹楦挎閺嗭妇绱掔€ｎ収鍤﹂柡鍐ㄧ墕閻掑灚銇勯幒鎴濐仼缁炬儳銈搁弻鏇熺節韫囨搩娲紓浣叉閸嬫挸鈹戦悩鍨毄濠殿喗鎸冲畷鎰磼濡粯鐝烽梺鍝勬川婵澹曟總鍛婄厽婵せ鍋撴繛浣冲洤鐓濋柛顐犲劜閻撴盯鎮橀悙鎻掆挃婵炲弶娼欓埞鎴︽晬閸曨偄骞嬪銈冨灪閻熲晠骞冮埄鍐╁劅妞ゆ梹鍨濆锕傛⒒閸屾艾鈧绮堟笟鈧獮鏍敃閿旂粯鏅為梺鍛婃处閸ㄩ亶宕愰崸妤佺叆闁哄洨鍋涢埀顒€鎽滅划濠氭倷閻戞鍘繝鐢靛仜閻忔繈宕濈€涙绠鹃柟鐐墯閻撳ジ鏌熼鑲╃Ш鐎规洖鐖奸、鏃堝礋椤撶儐妲辨繝鐢靛О閸ㄥジ锝炴径濞掓椽鎮㈡總澶嬬稁缂傚倷鐒﹁摫濠殿垱鎸抽弻褑绠涢幘鍓佹殯闂侀€炲苯澧柨鏇ㄤ邯瀵鏁撻悩鎻掔獩濡炪倖鏌ㄦ晶浠嬫偪閸曨垱鍊甸悷娆忓缁€鍐煕閵婏箑顕滃ǎ鍥э躬閹虫粓妫冨☉姘辩嵁濠电姷鏁告慨鎾疮椤栨績鍙㈠┑鐘垫暩婵挳鎯€婢舵劕绾ч幖瀛樻尭娴滈箖鏌￠崶銉ョ仼缂佺姷濞€楠炴牕菐椤掆偓婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛?

    async def _emergency_close(self, client: OKXClient, reason: str = "emergency_close"):
        """Close the active position and clear local state after drawdown stop."""
        if self._state.direction in ("long", "short"):
            try:
                direction = self._state.direction
                avg_entry = self._state.avg_entry
                total_sz = self._state.total_sz
                close_price = await client.get_mark_price(INST_ID)
                log_action(
                    f"Emergency close start reason={reason} direction={direction} "
                    f"mark={close_price:.2f} avg_entry={avg_entry:.2f} sz={total_sz}"
                )
                await self._cancel_entry_orders(client)
                await self._cancel_exchange_exit_orders(client)
                await self._cancel_exit_orders(client)
                await client.close_position(INST_ID, direction)
                await asyncio.sleep(1)
                equity_close = await self._fetch_account_equity_close_pnl(client)
                actual_pnl = equity_close["pnl"] if equity_close else None
                if avg_entry > 0 and total_sz > 0:
                    if direction == "long":
                        pnl = (close_price - avg_entry) * total_sz * CT_VAL
                    else:
                        pnl = (avg_entry - close_price) * total_sz * CT_VAL
                    if actual_pnl is not None:
                        pnl = actual_pnl
                        log_action(
                            f"Emergency close actual_pnl={pnl:+.4f} USDT "
                            f"source=account_equity_diff reason={reason}"
                        )
                    await notify_close(direction, avg_entry, close_price, pnl, total_sz)
                self._reset_probe_state()
                self._state.reset()
                self._clear_runtime_state()
                actual_profit = await self._rebalance_accounts(client, actual_pnl=actual_pnl)
                if actual_profit:
                    log_action(f"Capital actual PnL confirmed {actual_profit:+.4f} USDT")
                await self._calibrate_capital_after_close(client)
                await self._init_fixed_batch_sizes(client)
            except Exception as e:
                logger.error(f"Emergency close failed: {e}")

    async def _cancel_entry_orders(self, client: OKXClient):
        """Cancel all local unfilled entry orders."""
        kept_batches = []
        for batch in self._state.batches:
            if batch.filled:
                kept_batches.append(batch)
                continue
            try:
                await client.cancel_order(INST_ID, batch.ord_id)
            except Exception as e:
                logger.warning(f"Cancel remaining add-on order failed ordId={batch.ord_id}: {e}")
        self._state.batches = kept_batches
        self._save_runtime_state()

    async def _cancel_untriggered_entry_orders(self, client: OKXClient, last, mark_price: float):
        """Cancel pending add-on orders when price returns inside the band."""
        if not self._state.is_active():
            return

        trigger_direction = self._intrabar_probe_direction(None, last, mark_price)
        if trigger_direction == self._state.direction:
            return

        has_local_pending = any(not b.filled for b in self._state.batches)

        entry_side = "buy" if self._state.direction == "long" else "sell"
        try:
            orders = await client.get_open_orders(INST_ID)
        except Exception as e:
            logger.warning(f"Fetch open entry orders failed: {e}")
            return

        entry_orders = [
            o for o in orders
            if o.get("side") == entry_side
            and o.get("posSide") == self._state.direction
            and o.get("reduceOnly") != "true"
        ]
        if not has_local_pending and not entry_orders:
            return

        log_check("Price no longer breaks Bollinger band; cancel unfilled add-on order")
        await self._cancel_entry_orders(client)
        self._last_batch_kline_ts = None
        self._last_recovery_kline_ts = None

        for order in entry_orders:
            ord_id = order.get("ordId")
            if ord_id:
                await client.cancel_order(INST_ID, ord_id)

    async def _cancel_untriggered_probe_order(self, client: OKXClient, last, mark_price: float):
        """Cancel the first probe order when price returns inside the band."""
        if self._state.is_active():
            return
        if self._state.direction not in ("long", "short"):
            return

        trigger_direction = self._intrabar_probe_direction(None, last, mark_price)
        if trigger_direction == self._state.direction:
            return

        pending = self._state.pending_batch()
        if pending is None:
            return

        log_check("Price returned inside Bollinger band; cancel unfilled first batch")
        await client.cancel_order(INST_ID, pending.ord_id)
        self._state.remove_batch(pending.ord_id)
        if pending.batch_idx == 0 and self._last_plan_kline_ts == last["ts"]:
            self._last_plan_kline_ts = None
            self._last_batch_kline_ts = None
            self._last_plan_entry_price = 0.0
            logger.info("Unfilled first batch canceled; current kline entry lock released")
        self._reset_probe_state()
        self._state.reset()
        self._clear_runtime_state()

    async def _cancel_exchange_open_orders(self, client: OKXClient):
        """Cancel all exchange open orders for the instrument."""
        try:
            orders = await client.get_open_orders(INST_ID)
        except Exception as e:
            logger.warning(f"Query pending orders failed: {e}")
            return

        for order in orders:
            ord_id = order.get("ordId")
            if not ord_id:
                continue
            try:
                await client.cancel_order(INST_ID, ord_id)
            except Exception as e:
                logger.warning(f"Cancel stale exchange entry order failed ordId={ord_id}: {e}")

    async def _reconcile_entry_orders_after_restart(self, client: OKXClient):
        """Match local pending entry orders with exchange orders after restart."""
        entry_side = "buy" if self._state.direction == "long" else "sell"
        try:
            orders = await client.get_open_orders(INST_ID)
        except Exception as e:
            logger.warning(f"Restart reconciliation for exchange add-on orders failed: {e}")
            return

        entry_orders = [
            o for o in orders
            if o.get("side") == entry_side
            and o.get("posSide") == self._state.direction
            and o.get("reduceOnly") != "true"
        ]
        live_ids = {o.get("ordId") for o in entry_orders if o.get("ordId")}
        local_pending = [b for b in self._state.batches if not b.filled and b.ord_id]
        local_pending_ids = {b.ord_id for b in local_pending}

        removed = []
        known_filled_sz = sum(b.sz for b in self._state.batches if b.filled)
        unmatched_filled_sz = max(self._state.total_sz - known_filled_sz, 0.0)
        for batch in sorted(local_pending, key=lambda b: b.batch_idx):
            if batch.ord_id not in live_ids:
                if unmatched_filled_sz + 1e-8 >= batch.sz:
                    try:
                        order_info = await client.get_order(INST_ID, batch.ord_id)
                        fill_sz, fill_price, fill_kline_ts = self._extract_order_fill(
                            order_info, batch.sz, batch.price
                        )
                        batch.sz = fill_sz
                        batch.price = fill_price
                        if fill_kline_ts is not None:
                            self._last_batch_kline_ts = fill_kline_ts
                    except Exception:
                        pass
                    batch.filled = True
                    unmatched_filled_sz -= batch.sz
                    logger.info(f"Local batch {batch.batch_idx + 1} filled while offline ordId={batch.ord_id}")
                else:
                    removed.append(batch)
                    logger.info(f"Local batch {batch.batch_idx + 1} no longer on exchange; remove ordId={batch.ord_id}")
        for batch in removed:
            self._state.remove_batch(batch.ord_id)
        local_pending_ids = {b.ord_id for b in self._state.batches if not b.filled and b.ord_id}

        for order in entry_orders:
            ord_id = order.get("ordId")
            if not ord_id or ord_id in local_pending_ids:
                continue
            logger.info(f"Cancel unmatched exchange entry order ordId={ord_id}")
            await client.cancel_order(INST_ID, ord_id)

        if local_pending_ids:
            logger.info(f"Restart kept exchange pending add-on orders ordId={sorted(local_pending_ids)}")
        self._save_runtime_state()

    async def _refresh_filled_batches_from_orders(self, client: OKXClient):
        """Refresh filled batch prices and sizes from OKX order details."""
        changed = False
        for batch in self._state.filled_batches():
            if not batch.ord_id or batch.ord_id == "existing-position":
                continue
            try:
                order_info = await client.get_order(INST_ID, batch.ord_id)
                fill_sz, fill_price, fill_kline_ts = self._extract_order_fill(order_info, batch.sz, batch.price)
            except Exception as e:
                logger.warning(f"Restart refresh batch {batch.batch_idx + 1} fill failed ordId={batch.ord_id}: {e}")
                continue
            if fill_kline_ts is not None:
                self._last_batch_kline_ts = fill_kline_ts
                changed = True
            if fill_sz > 0 and abs(fill_sz - batch.sz) > 1e-8:
                logger.info(
                    f"Restart refreshed batch {batch.batch_idx + 1} fill size "
                    f"{batch.sz:g} -> {fill_sz:g}"
                )
                batch.sz = fill_sz
                changed = True
            if fill_price > 0 and abs(fill_price - batch.price) > 1e-8:
                logger.info(
                    f"Restart refreshed batch {batch.batch_idx + 1} fill price "
                    f"{batch.price:.2f} -> {fill_price:.2f}"
                )
                batch.price = fill_price
                changed = True
        if changed:
            self._save_runtime_state()

    def _repair_filled_batches_after_restart(self):
        """Replace stale filled batches when they do not match the exchange position."""
        if not self._state.is_active():
            return

        filled = self._state.filled_batches()
        pending = [batch for batch in self._state.batches if not batch.filled]
        filled_sz = sum(batch.sz for batch in filled)
        has_invalid_order_id = any(
            batch.ord_id
            and batch.ord_id != "existing-position"
            and not str(batch.ord_id).isdigit()
            for batch in filled
        )
        size_mismatch = abs(filled_sz - self._state.total_sz) > CONTRACT_STEP
        if not has_invalid_order_id and not size_mismatch:
            return

        logger.warning(
            "Local filled batches mismatch exchange position; "
            f"rebuild from exchange avg filled_sz={filled_sz:g} real_sz={self._state.total_sz:g}. "
            "Future add-on gap checks will use the exchange average until real fill history is available."
        )
        self._state.batches = [
            OpenBatch(
                batch_idx=0,
                ord_id="existing-position",
                price=self._state.avg_entry,
                sz=self._state.total_sz,
                filled=True,
            )
        ] + pending
        self._probe_entry_price = self._state.avg_entry
        self._last_plan_entry_price = self._state.avg_entry
        self._save_runtime_state()
        self._log_runtime_state_summary("Rebuilt runtime state from exchange position")

    async def _cancel_exchange_entry_orders(self, client: OKXClient):
        """Cancel exchange entry orders for the current side.

        Reserved helper. It is not called by the current live path.
        """
        entry_side = "buy" if self._state.direction == "long" else "sell"
        try:
            orders = await client.get_open_orders(INST_ID)
        except Exception as e:
            logger.warning(f"Query exchange add-on orders failed: {e}")
            return

        for order in orders:
            if order.get("side") != entry_side:
                continue
            if order.get("posSide") != self._state.direction:
                continue
            if order.get("reduceOnly") == "true":
                continue
            ord_id = order.get("ordId")
            if ord_id:
                logger.info(f"Cancel stale close order ordId={ord_id}")
                await client.cancel_order(INST_ID, ord_id)

    async def _cancel_exchange_exit_orders(self, client: OKXClient):
        """Cancel exchange take-profit and stop-loss orders for the position."""
        try:
            orders = await client.get_open_orders(INST_ID)
        except Exception as e:
            logger.warning(f"Fetch open close orders failed: {e}")
            orders = []

        close_side = "sell" if self._state.direction == "long" else "buy"
        for order in orders:
            if order.get("reduceOnly") != "true":
                continue
            if order.get("side") != close_side:
                continue
            ord_id = order.get("ordId")
            if ord_id:
                await client.cancel_order(INST_ID, ord_id)

        try:
            algos = await client.get_open_algo_orders(INST_ID)
        except Exception as e:
            logger.warning(f"Fetch open algo orders failed: {e}")
            return

        for algo in algos:
            algo_id = algo.get("algoId")
            if algo_id:
                await client.cancel_algo_order(INST_ID, algo_id)

    async def _cancel_invalid_entry_orders(self, client: OKXClient):
        """Cancel entry orders that violate spacing or side rules.

        Reserved helper. It is not called by the current live path.
        """
        last_batch = self._state.last_filled_batch()
        if last_batch is None:
            return

        entry_side = "buy" if self._state.direction == "long" else "sell"
        try:
            orders = await client.get_open_orders(INST_ID)
        except Exception as e:
            logger.warning(f"Query add-on orders failed: {e}")
            return

        for order in orders:
            if order.get("side") != entry_side or order.get("posSide") != self._state.direction:
                continue
            if order.get("reduceOnly") == "true":
                continue

            try:
                px = float(order.get("px") or 0)
            except ValueError:
                continue

            gap = abs(px - last_batch.price)
            invalid = gap < self._effective_entry_gap(px)
            if self._state.direction == "long" and px >= last_batch.price:
                invalid = True
            if self._state.direction == "short" and px <= last_batch.price:
                invalid = True

            if invalid:
                ord_id = order.get("ordId")
                if ord_id:
                    logger.info(
                        f"Cancel invalid add-on order ordId={ord_id} price={px:.2f} "
                        f"last_fill={last_batch.price:.2f} gap={gap:.2f}"
                    )
                    await client.cancel_order(INST_ID, ord_id)

    async def _cancel_exit_orders(self, client: OKXClient):
        """Cancel locally tracked exit orders."""
        if self._state.tp_ord_id:
            await client.cancel_order(INST_ID, self._state.tp_ord_id)
            self._state.tp_ord_id = None
        if self._state.sl_ord_id:
            await client.cancel_algo_order(INST_ID, self._state.sl_ord_id)
            self._state.sl_ord_id = None

    async def _reset_if_plan_has_no_live_orders(self, client: OKXClient):
        """Reset local plan state when no live position or entry order exists."""
        if self._state.is_active() or not self._state.batches:
            return

        try:
            orders = await client.get_open_orders(INST_ID)
        except Exception as e:
            logger.warning(f"Fetch open orders for dashboard failed: {e}")
            return

        live_ids = {o.get("ordId") for o in orders}
        has_live_entry = any(
            (not b.filled) and b.ord_id in live_ids
            for b in self._state.batches
        )
        if not has_live_entry:
            self._last_plan_kline_ts = None
            self._last_batch_kline_ts = None
            self._last_plan_entry_price = 0.0
            logger.info("No position or pending entry orders; reset strategy state")
            self._reset_probe_state()
            self._state.reset()
            self._clear_runtime_state()

    def _reset_probe_state(self):
        """Clear first-probe metadata."""
        self._probe_kline_ts = None
        self._probe_direction = "none"
        self._probe_entry_price = 0.0

    # 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁惧墽鎳撻—鍐偓锝庝簼閹癸綁鏌ｉ鐐搭棞闁靛棙甯掗～婵嬫晲閸涱剙顥氶梺璇叉唉椤煤閿曞倸鍨傞悹楦挎閺嗭妇绱掔€ｎ収鍤﹂柡鍐ㄧ墕閻掑灚銇勯幒鎴濐仼缁炬儳銈搁弻鏇熺節韫囨搩娲紓浣叉閸嬫挸鈹戦悩鍨毄濠殿喗鎸冲畷鎰磼濡粯鐝烽梺鍝勬川婵澹曟總鍛婄厽婵せ鍋撴繛浣冲洤鐓濋柛顐犲劜閻撴盯鎮橀悙鎻掆挃婵炲弶娼欓埞鎴︽晬閸曨偄骞嬪銈冨灪閻熲晠骞冮埄鍐╁劅妞ゆ梹鍨濆锕傛⒒?闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁惧墽鎳撻—鍐偓锝庝簼閹癸綁鏌ｉ鐐搭棞闁靛棙甯掗～婵嬫晲閸涱剙顥氬┑掳鍊楁慨鐑藉磻閻愮儤鍋嬮柣妯荤湽閳ь兛绶氬鎾閻橀潧骞堟繝娈垮枟閿曗晠宕㈡禒瀣︽繝闈涙閺€浠嬫⒔閸ヮ剙鏄ラ柡宓苯娈梺鍛婃处閸樻悂宕戦幘缁樻櫜閹煎瓨绻勯懗鍝勨攽閳ュ啿绾ч柛鏃€鐟ラ～蹇曠磼濡偐鎳濋梺閫炲苯澧い顓炴穿椤﹁泛顭胯缁诲牆顫忓ú顏勪紶闁告洦鍓欏▍銈夋⒑閻戔晜娅撻柛銊ョ埣閻涱喛绠涘☉妯碱吅闂佹寧妫佸Λ鍕濠婂牊鐓熼煫鍥ㄦ尵缁狅綁鏌ｉ幒鐐电暤鐎殿噮鍓熼崺鈧い鎺戝閳锋帒霉閿濆牊顏犻悽顖涚洴閺屻劌顫濋幍浣镐壕婵炲牆鐏濋弸锕傛煕閳哄倻澧い鏇樺劦瀹曠喖顢涘槌栨Ч婵＄偑鍊栭悧妤冪矙閹捐鍌ㄩ梺顒€绉甸悡娆撴煕韫囨艾浜归柡鍡橈耿閺屾盯濡搁妷褏楔闂佽鍠楅敃銏ょ嵁濮椻偓椤㈡瑩鎮剧仦钘夌濠碉紕鍋戦崐鏍ь潖婵犳碍鍋ら柡鍌氱氨閺嬫梹绻濇繝鍌滃闁绘挻绋戦湁闁挎繂娲﹂崵鈧繝娈垮枛閻楁捇寮婚悢纰辨晬婵炴垶鐟Λ鍐⒑閹肩偛濮傜紒鐘崇墵楠炲﹪鎮╁ú缁樻櫌闂侀€炲苯澧寸€规洜鏁婚、妤呭磼濠婂拑绱抽梻浣侯焾閺堫剟鎳濇ィ鍐ㄧ劦妞ゆ巻鍋撻柛銏＄叀濠€渚€姊洪幖鐐插姶闁告挻鐟╁浼村Ψ閳哄倻鍘搁悗骞垮劚妤犳悂鐛Δ鍛厱閻庯綆浜滈埀顒€娼″濠氬Ω閳哄倸浜滈梺鍛婄箓鐎氬懘濮€閵堝棛鍙?闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁惧墽鎳撻—鍐偓锝庝簼閹癸綁鏌ｉ鐐搭棞闁靛棙甯掗～婵嬫晲閸涱剙顥氶梺璇叉唉椤煤閿曞倸鍨傞悹楦挎閺嗭妇绱掔€ｎ収鍤﹂柡鍐ㄧ墕閻掑灚銇勯幒鎴濐仼缁炬儳銈搁弻鏇熺節韫囨搩娲紓浣叉閸嬫挸鈹戦悩鍨毄濠殿喗鎸冲畷鎰磼濡粯鐝烽梺鍝勬川婵澹曟總鍛婄厽婵せ鍋撴繛浣冲洤鐓濋柛顐犲劜閻撴盯鎮橀悙鎻掆挃婵炲弶娼欓埞鎴︽晬閸曨偄骞嬪銈冨灪閻熲晠骞冮埄鍐╁劅妞ゆ梹鍨濆锕傛⒒閸屾艾鈧绮堟笟鈧獮鏍敃閿旂粯鏅為梺鍛婃处閸ㄩ亶宕愰崸妤佺叆闁哄洨鍋涢埀顒€鎽滅划濠氭倷閻戞鍘繝鐢靛仜閻忔繈宕濈€涙绠鹃柟鐐墯閻撳ジ鏌熼鑲╃Ш鐎规洖鐖奸、鏃堝礋椤撶儐妲辨繝鐢靛О閸ㄥジ锝炴径濞掓椽鎮㈡總澶嬬稁缂傚倷鐒﹁摫濠殿垱鎸抽弻褑绠涢幘鍓佹殯闂侀€炲苯澧柨鏇ㄤ邯瀵鏁撻悩鎻掔獩濡炪倖鏌ㄦ晶浠嬫偪閸曨垱鍊甸悷娆忓缁€鍐煕閵婏箑顕滃ǎ鍥э躬閹虫粓妫冨☉姘辩嵁濠电姷鏁告慨鎾疮椤栨績鍙㈠┑鐘垫暩婵挳鎯€婢舵劕绾ч幖瀛樻尭娴滈箖鏌￠崶銉ョ仼缂佺姷濞€楠炴牕菐椤掆偓婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻?

    def _update_dashboard(self, mark_price: float, last, equity: float):
        """Copy the current strategy snapshot into dashboard state."""
        s = dashboard.state
        s.mark_price  = mark_price
        s.boll_lower  = float(last["boll_lower"])
        s.boll_mid    = float(last["boll_mid"])
        s.boll_upper  = float(last["boll_upper"])
        s.equity      = equity
        s.peak_equity = self._peak_eq
        s.direction   = self._state.direction
        s.total_sz    = self._state.total_sz
        s.tp_price    = self._state.plan_tp_price
        s.liq_price   = self._state.plan_liq_price
        s.batches     = [
            {"batch_idx": b.batch_idx, "price": b.price, "sz": b.sz, "filled": b.filled}
            for b in self._state.batches
        ]
        # 濠电姷鏁告慨鐑藉极閸涘﹥鍙忛柣鎴ｆ閺嬩線鏌涘☉姗堟敾闁告瑥绻橀弻锝夊箣閿濆棭妫勯梺鍝勵儎缁舵岸寮婚悢鍏尖拻閻庨潧澹婂Σ顔剧磼閻愵剙鍔ゆい顓犲厴瀵鏁愭径濠勭杸濡炪倖甯婇悞锕傚磿閹惧墎纾藉ù锝呮惈鏍″┑顔角滈崝鎴﹀春閳ь剚銇勯幒鍡椾壕闂佸憡蓱缁挸鐣烽幋锕€绠荤紓浣诡焽閸橀亶姊洪崫鍕偍闁告柨鏈粋宥夋倷閻戞鍘藉┑鐐村灦閻楁洟宕濋敂鑺ュ弿濠电姴鍟妵婵堚偓瑙勬处閸嬪﹤鐣烽悢纰辨晣濠㈣泛鑻埢鍫ユ煛鐏炲墽娲寸€殿噮鍓涢幑鍕Ω閹板苯鎳夐崑鎾舵喆閸曨剛鈹涚紓鍌氱М閸嬫挾绱撴担鍝勑ｇ紒瀣灴閸┿儲寰勬繛鐐€婚梺鐟扮摠缁诲倻绮鑸碘拻闁稿本鐟чˇ锕傛煙鐠囇呯？闁瑰箍鍨藉畷鎺戔攦閹傚濠殿喗顭囬崢褍顕ｉ閿亾鐟欏嫭绀€闁哥喕娉曢崣鍛渻閵堝懐绠伴柟铏姍瀹曘垽鏌嗗鍡欏幍闂佺厧婀辨晶妤勩亹瑜忕槐鎺旂磼濡偐鐤勯悗瑙勬穿缂嶄礁鐣烽悢纰辨晬婵炴垶鑹鹃獮鎰版⒑鐠囪尙绠抽柛瀣⊕閺呭爼鎸婃竟婵婃閳ь剚绋掕彠濞存粍绮撻弻鏇＄疀婵犲倸鈷夌紓浣插亾閻庯綆鍠楅悡娑樏归敐鍥ㄥ殌濠殿喖绉堕埀顒冾潐濞叉牠鎮ユ總绋挎槬闁跨喓濮寸粈鍐煏婵炲灝鍔ら柣鐔叉櫇缁?
        filled = self._state.filled_batches()
        if self._state.avg_entry > 0 and self._state.total_sz > 0 and mark_price > 0:
            from src.config import CT_VAL
            total_sz   = self._state.total_sz
            avg_entry  = self._state.avg_entry
            s.avg_entry = avg_entry
            if self._state.direction == "long":
                s.unrealized_pnl = (mark_price - avg_entry) * total_sz * CT_VAL
            else:
                s.unrealized_pnl = (avg_entry - mark_price) * total_sz * CT_VAL
        else:
            s.avg_entry      = 0.0
            s.unrealized_pnl = 0.0
        s.update_time()

    # 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁惧墽鎳撻—鍐偓锝庝簼閹癸綁鏌ｉ鐐搭棞闁靛棙甯掗～婵嬫晲閸涱剙顥氶梺璇叉唉椤煤閿曞倸鍨傞悹楦挎閺嗭妇绱掔€ｎ収鍤﹂柡鍐ㄧ墕閻掑灚銇勯幒鎴濐仼缁炬儳銈搁弻鏇熺節韫囨搩娲紓浣叉閸嬫挸鈹戦悩鍨毄濠殿喗鎸冲畷鎰磼濡粯鐝烽梺鍝勬川婵澹曟總鍛婄厽婵せ鍋撴繛浣冲洤鐓濋柛顐犲劜閻撴盯鎮橀悙鎻掆挃婵炲弶娼欓埞鎴︽晬閸曨偄骞嬪銈冨灪閻熲晠骞冮埄鍐╁劅妞ゆ梹鍨濆锕傛⒒?闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁惧墽鎳撻—鍐偓锝庝簼閹癸綁鏌ｉ鐐搭棞闁靛棙甯掗～婵嬫晲閸涱剙顥氬┑掳鍊楁慨鐑藉磻閻愮儤鍋嬮柣妯荤湽閳ь兛绶氬鎾閳╁啯鐝栭梻渚€鈧偛鑻晶鎾煙椤斿吋鍋ユい銏″哺閸┾偓妞ゆ帒瀚拑鐔兼煛閸モ晛鏋旂紒鐘荤畺閺岋綁骞囬棃娑橆潽闂佺粯绻冨鑽ゆ閹烘鍊锋い鎺嗗亾闁告柣鍊栫换娑氫焊閺嶃倕浜鹃柟棰佺濞堛劑姊洪崜鎻掍簼婵炲弶鐗犻弻瀣炊閵婏箑寮垮┑顔筋殔濡鐛Δ鍛厽婵犻潧娲︾粈瀣煙椤旂瓔娈橀柟鍙夋尦瀹曠喖顢楅崒銈喰為梻鍌欒兌缁垶骞栭銈嗗床婵犻潧妫崵鏇炩攽閻樺磭顣查柡鍛倐閺岋絽螣閸喚姣㈠銈忚礋閸旀垵顫忓ú顏勭閹艰揪绲块悾鐢告⒑閻熸澘鏆辩紒澶屾暩缁晠鎮㈤悡搴″祮闂佺粯妫佸▍锝夋儊閸儲鈷戞慨鐟版搐閻忓弶绻涙担鍐插椤╅鎲搁弮鍫濊摕婵炴垶顭傞悢鍏煎亹闁告瑥顦▍銈夋倵鐟欏嫭绀€鐎殿喖鐖奸獮鍫ュΩ閵夘喗瀵岄柣鐘叉穿瀵挻绔熼弴銏♀拻濞达綀娅ｇ敮娑樸€掑顓ф疁鐎规洑鍗冲浠嬵敇閵娧呪棨婵犵數濮撮敃銈夋偋閸℃稒鍊块柛顭戝亖娴滄粓鏌熼崫鍕棞濞存粓绠栧鐑樺濞嗘垵鍩岄梺鎼炲灪閻擄繝鏁愰悙宸叆闁割偅绻勯崝锕€顪冮妶鍡楀潑闁稿鎹囬弻锝夋晲閸パ冨箣闂佽桨鐒﹂崝娆忕暦閸楃倣鏃堝礃椤忓棗绁﹂梻鍌欐祰椤曆呮崲閹烘纾婚柣鏂垮悑閹偤骞栫划瑙勵潑闁绘帊绮欓弻宥夊传閸曨剙娅ら梺鎶芥敱鐢帡婀侀梺鎸庣箓濞层劑骞楅崒鐐寸厱闁靛牆妫涢幊鍕磼缂佹銆掗柍褜鍓涢弫鎼佲€﹂崼銉ュ偍閻庣數纭堕崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妼濞尖€愁嚕椤愶箑绠涙い鎾跺仧缁愮偞绻濋悽闈浶㈤悗姘憸濡叉劙顢欓悾宀€鐦堥梺姹囧灲濞佳勭閿曞倹鐓曢柕濞垮劤閸╋綁鏌℃担绋挎殻闁糕斁鍓濈€靛ジ宕￠悙鐢敌ㄩ悗娈垮枙缁瑩銆佸鈧幃銏ゅ传閸曨偄顩┑鐘垫暩婵兘寮幖浣哥９妞ゆ牜鍋涚粈鍐煏婵犲繘妾い鏂垮濮婄粯鎷呯粵瀣缂備胶绮崝娆撳极閸愵噮鏁傞柛鏇炵仛缂嶅骸鈹戦悙鍙夆枙濞存粍绻堣棢闁割偆鍠撶粻鐐箾閿濆骸澧柍褜鍓氶悧鐘诲箚閸曨垼鏁嶆慨妯块哺濞堥箖姊虹紒妯烩拻闁冲嘲鐗撳顐﹀礃閳瑰じ绨婚梺褰掑亰閸犳牠寮告惔鈭剁懓顭ㄩ崨顓濆缂備胶绮惄顖炵嵁鐎ｎ亖鏋庨煫鍥ㄦ磻閻ヮ亪姊绘担鐟扳枙闁衡偓闁秴鍨傞柛褎顨呴拑鐔兼煥濠靛棭妲哥紒鐘崇⊕閵囧嫰寮介妸銈囩箒濠碘槅鍋勭€氭澘顫忓ú顏勫窛濠电姴鍟犻幏褰掓倵閸忓浜鹃梺褰掓？缁€渚€鎷戦悢鍝ョ闁瑰瓨鐟ラ悘顏堟煕鐎ｎ亜顏慨濠冩そ瀹曞綊顢氶崨顓炲濠电偛顕慨鐢稿箰閸愬樊娼?闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁惧墽鎳撻—鍐偓锝庝簼閹癸綁鏌ｉ鐐搭棞闁靛棙甯掗～婵嬫晲閸涱剙顥氶梺璇叉唉椤煤閿曞倸鍨傞悹楦挎閺嗭妇绱掔€ｎ収鍤﹂柡鍐ㄧ墕閻掑灚銇勯幒鎴濐仼缁炬儳銈搁弻鏇熺節韫囨搩娲紓浣叉閸嬫挸鈹戦悩鍨毄濠殿喗鎸冲畷鎰磼濡粯鐝烽梺鍝勬川婵澹曟總鍛婄厽婵せ鍋撴繛浣冲洤鐓濋柛顐犲劜閻撴盯鎮橀悙鎻掆挃婵炲弶娼欓埞鎴︽晬閸曨偄骞嬪銈冨灪閻熲晠骞冮埄鍐╁劅妞ゆ梹鍨濆锕傛⒒閸屾艾鈧绮堟笟鈧獮鏍敃閿旂粯鏅為梺鍛婃处閸ㄩ亶宕愰崸妤佺叆闁哄洨鍋涢埀顒€鎽滅划濠氭倷閻戞鍘繝鐢靛仜閻忔繈宕濈€涙绠鹃柟鐐墯閻撳ジ鏌熼鑲╃Ш鐎规洖鐖奸、鏃堝礋椤撶儐妲辨繝鐢靛О閸ㄥジ锝炴径濞掓椽鎮㈡總澶嬬稁缂傚倷鐒﹁摫濠殿垱鎸抽弻褑绠涢幘鍓佹殯闂侀€炲苯澧柨鏇ㄤ邯瀵鏁撻悩鎻掔獩濡炪倖鏌ㄦ晶浠嬫偪閸曨垱鍊甸悷娆忓缁€鍐煕閵婏箑顕滃ǎ鍥э躬閹虫粓妫冨☉姘辩嵁濠电姷鏁告慨鎾疮椤栨績鍙㈠┑鐘垫暩婵挳鎯€婢舵劕绾ч幖瀛樻尭娴滈箖鏌￠崶銉ョ仼缂佺姷濞€楠炴牕菐椤掆偓婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛?

    def _log_plan(self, plan, mark_price: float):
        """Log a human-readable batch-entry plan."""
        log_check("=" * 60)
        log_check(f"Entry plan direction={plan.direction} mark_price={mark_price:.2f}")
        log_check(f"  tp={plan.tp_price} estimated_liq={plan.liq_price}")
        log_check(f"  total_margin={plan.total_margin:.2f} USDT")
        for bo in plan.orders:
            log_check(
                f"  batch={bo.batch_idx + 1} price={bo.price} sz={bo.sz}"
                f" notional={bo.notional:.2f} margin={bo.margin:.2f}"
            )
        log_check("=" * 60)

    # 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁惧墽鎳撻—鍐偓锝庝簼閹癸綁鏌ｉ鐐搭棞闁靛棙甯掗～婵嬫晲閸涱剙顥氶梺璇叉唉椤煤閿曞倸鍨傞悹楦挎閺嗭妇绱掔€ｎ収鍤﹂柡鍐ㄧ墕閻掑灚銇勯幒鎴濐仼缁炬儳銈搁弻鏇熺節韫囨搩娲紓浣叉閸嬫挸鈹戦悩鍨毄濠殿喗鎸冲畷鎰磼濡粯鐝烽梺鍝勬川婵澹曟總鍛婄厽婵せ鍋撴繛浣冲洤鐓濋柛顐犲劜閻撴盯鎮橀悙鎻掆挃婵炲弶娼欓埞鎴︽晬閸曨偄骞嬪銈冨灪閻熲晠骞冮埄鍐╁劅妞ゆ梹鍨濆锕傛⒒?闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧湱鈧懓瀚崳纾嬨亹閹烘垹鍊為悷婊冪箻瀵娊鏁冮崒娑氬幈濡炪値鍘介崹鍨濠靛鐓曟繛鍡楃箳缁犳娊鏌嶈閸撴瑧绮诲澶婄？闂侇剙鍗曢崶顒夋晬婵犲﹤鎳愰悞濂告⒑閸涘﹤濮€闁哄倸鍊圭粋宥咁煥閸曗晙绨婚梺瑙勫礃濞夋盯寮告惔銊︾厽闊洢鍎抽幃鑲╃磼鏉堛劌绗氭繛鐓庣箻婵℃悂鏁傜紒姗嗘濠电姵顔栭崰鏍晝閿旀儳鍨濇い鏍ㄧ矌缁犳棃鏌ｉ弮鍌氬付闁绘帒鐏氶妵鍕箳閸℃ぞ澹曟俊鐐€ら崢鐓幟洪銏犵疇闁跨喓濮村洿闂佸憡渚楅崹鎶芥偤濮椻偓濮婄粯鎷呯粙鎸庡€繛瀛樼矆缁瑥鐣烽弴銏犵闁瑰搫妫欓悗娲⒑缂佹﹩鐒炬い銉ユ閵囨劙骞掗幘鍏呮睏闁诲海鎳撴竟濠囧窗閺嶎厼绀堥柟娈垮枤绾捐棄霉閿濆懏鎯堟い搴＄焸閺屾盯濡搁敃鈧埢鏇燁殽閻愬樊妯€妤犵偞鐗楅幏鍛存偡妫颁胶缍嶉梻鍌欑婢瑰﹪宕戦崨顖涘床闁告洦鍨遍崑锟犳煛鐏炶鍔滈柍閿嬪灴閺屾稑鈹戦崱妤婁痪闂侀潻缍€濞咃絿妲愰幒妤婃晩闁兼亽鍎辩壕鎶芥倵濞堝灝鏋涙い顓犲厴瀵偊宕橀鑲╁姦濡炪倖甯掗崯鐗堢閽樺鏀介柣鎰摠鐏忎即鏌涢幋婵堢Ш鐎规洝顫夊蹇涒€﹂幋鐑嗗敳闂傚倸鍊搁崐鎼佸磹妞嬪海鐭嗗〒姘ｅ亾妤犵偛顦甸弫鎾绘偐閾忣偅鐝栭梻渚€娼ф蹇曟閺囶潿鈧懘寮婚妷锕€浠柡澶屽仦婵粙顢楅悢鍝ョ闁稿繗鍋愭晶顒傜磼缂佹鈽夋い鏂跨箻椤㈡瑩鎳￠妶鍥ㄦ櫒闂佽娴烽幊鎾诲箟閿涘嫭宕查柛宀€鍋涢悡姗€鏌熸潏楣冩闁稿鍔欓弻鐔虹磼濡搫娼戦梺绋款儐閹瑰洭鐛弽銊х懝濠电姴瀚埣銈夋煟鎼粹€冲辅闁稿鎹囬弻宥堫檨闁告挻绋戝嵄闁圭増婢樼粻濠氭倵濞戞顏堫敁閹剧粯鈷戦柛娑橈攻鐏忣厾鈧鍠涢崺鏍疾閵夆晜鈷掗柛灞剧懆閸忓矂鏌熼搹顐ｅ磳妤犵偛顦甸崺鍕礃椤忓棭鍟庡┑鐘垫暩婵挳宕戦崱娑樺惞?闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁惧墽鎳撻—鍐偓锝庝簼閹癸綁鏌ｉ鐐搭棞闁靛棙甯掗～婵嬫晲閸涱剙顥氶梺璇叉唉椤煤閿曞倸鍨傞悹楦挎閺嗭妇绱掔€ｎ収鍤﹂柡鍐ㄧ墕閻掑灚銇勯幒鎴濐仼缁炬儳銈搁弻鏇熺節韫囨搩娲紓浣叉閸嬫挸鈹戦悩鍨毄濠殿喗鎸冲畷鎰磼濡粯鐝烽梺鍝勬川婵澹曟總鍛婄厽婵せ鍋撴繛浣冲洤鐓濋柛顐犲劜閻撴盯鎮橀悙鎻掆挃婵炲弶娼欓埞鎴︽晬閸曨偄骞嬪銈冨灪閻熲晠骞冮埄鍐╁劅妞ゆ梹鍨濆锕傛⒒閸屾艾鈧绮堟笟鈧獮鏍敃閿旂粯鏅為梺鍛婃处閸ㄩ亶宕愰崸妤佺叆闁哄洨鍋涢埀顒€鎽滅划濠氭倷閻戞鍘繝鐢靛仜閻忔繈宕濈€涙绠鹃柟鐐墯閻撳ジ鏌熼鑲╃Ш鐎规洖鐖奸、鏃堝礋椤撶儐妲辨繝鐢靛О閸ㄥジ锝炴径濞掓椽鎮㈡總澶嬬稁缂傚倷鐒﹁摫濠殿垱鎸抽弻褑绠涢幘鍓佹殯闂侀€炲苯澧柨鏇ㄤ邯瀵鏁撻悩鎻掔獩濡炪倖鏌ㄦ晶浠嬫偪閸曨垱鍊甸悷娆忓缁€鍐煕閵婏箑顕滃ǎ鍥э躬閹虫粓妫冨☉姘辩嵁濠电姷鏁告慨鎾疮椤栨績鍙㈠┑鐘垫暩婵挳鎯€婢舵劕绾ч幖瀛樻尭娴滈箖鏌￠崶銉ョ仼缂佺姷濞€楠炴牕菐椤掆偓婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑锛勬暬瀹曠喖顢欓崜褎婢戦梻渚€娼ч敍蹇涘川椤旂瓔鍟屾繝鐢靛У椤旀牠宕伴弽顓熸櫇闁挎梹鍎抽崹婵囥亜閺嶎偄浠滅紒鐙€鍨堕弻娑樷槈閸楃偟浠梺娲诲幗閻熲晠寮婚悢铏圭煓闁割煈鍣崝澶愭⒑娴兼瑨顓虹紓宥勭椤繒绱掑Ο璇差€撻梺鍛婄☉閿曘倝寮抽崼銉︾參闁告劦鍘洪崥顐ょ磼鏉堛劌娴い銏＄懇閹崇偤濡烽妷褝绱﹂梻鍌欒兌缁垱绗熷Δ鍛獥婵°倕鍟畷鍙夌節闂堟侗鍎忛柣鎰功閹叉瓕绠涘☉妯硷紮闂佺粯鍔曞Ο濠傘€掓繝姘厪闁割偅绻冮ˉ婊堟煕瑜嶉敃顏堝蓟濞戞埃鍋撻敐搴′簼鐎规洖鐭傞弻鈥崇暆鐎ｎ剛锛熼梺閫炲苯澧剧紒鍙夋そ瀵彃鈹戠€ｎ亞顦悗骞垮劚椤︿即鎮″▎蹇嬧偓鎺戭潩椤掑倷铏庢繝纰樷偓鍐叉倯闁靛洤瀚伴、鏃堝礋椤愶絾顔嶉梻浣哥秺椤ユ挻绻涢埀顒勬煟閹垮啫浜版い銏★耿閹粓宕卞鍡橈紖闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢幘鑼妽闁稿繑绮撻弻娑㈩敃閿濆棛顦ラ梺姹囧€楅崑鎾舵崲濠靛顥堟繛鎴濆船閸撴壆绱掗悙顒€鍔ゆい顓犲厴瀵寮撮姀鐘诲敹濠电姴鐏氶崝鏍懅闂傚倷绀侀幖顐も偓姘ュ姂瀹曟洟宕ｆ径灞告敵婵犵數濮村ú銈呮纯闂備礁鎲℃笟妤呭窗閺嵮€鏋嶉柨婵嗩槹閳锋垹绱撴担鑲℃垹浜告导瀛樼厽闁冲搫锕ら悘锔锯偓娈垮枦椤曆囧煡婢跺娼╂い鎰剁到婵即姊绘担鍛婂暈闁圭妫濋崺鈧い鎺嶇劍閸欏繘鏌涢妷鎴濇湰鐎靛矂姊洪棃娑氬闁硅櫕锕㈤幃鏉款煥閸涱垳锛滈梺缁橆焾濞呮洜浜搁鐔翠簻妞ゆ劑鍨荤粻宕囩磼鏉堛劌绗掗摶锝夋偣閸パ勨枙闁逞屽墯閹稿墽妲愰幘瀛樺闁告挻褰冮崜閬嶆煟鎼达絿鎳楅柛鎰暞鐢繝鐛崶顒佸亱闁割偅纰嶇€氬ジ姊绘担鍛婂暈缂佸鍨块垾锕傚焵椤掑嫭鐓欓悗鐢殿焾瀛濋梺鎼炲妽缁诲牓寮婚悢鐓庣闁归偊鍓欓幆鐐烘倵鐟欏嫭灏紒鈧笟鈧崺鐐哄箣閿旇棄浜归梺鍛婄懃椤︿即骞冨▎鎾粹拺缂備焦锚婵洭鏌ㄩ弴妯哄姦濠碉紕鏁诲畷鐔碱敍濮橀硸鍞洪梻浣虹《閸撴繈濡甸悙瀵哥彾闁哄洢鍨洪埛鎺懨归敐鍛暈闁诡垰鐗婇妵鍕箣濠靛棭浼冮梺璇″枤閸嬨倝寮崘顔肩＜婵浜弶浠嬫⒒娓氣偓濞佳団€﹂崼銉ョ？闁告繂瀚ㄩ埀顒佸笚缁绘繂顫濋鐐板寲闂備焦鎮堕崕鐑樼濠婂懏顫曢柨鏇楀亾妞ゎ叀娉曢幉鎾礋椤掑偆妲规繝娈垮枛閿曪妇鍒掗鐐茬闁告稑鐡ㄩ幆鐐搭殽閻愯尙姘ㄩ柛瀣尰缁绘繈宕堕妸褍骞愰梻浣告啞娓氭宕板杈╀笉闁绘劗顣介崑鎾舵喆閸曨剛顦ㄩ梺鎼炲妽鐎笛勭┍婵犲洦鍤嬮梻鍫熺〒缁愮偞绻濋悽闈浶㈤柛鐕佸灥閳弶绻濋悽闈浶㈤柣銉ヮ樀瀵埖鎯旈幘鍏呭闂佸搫娲㈤崹鍦不閻樿绠规繛锝庡墮婵′粙鏌涚€ｎ亜鈧湱鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缂佹ɑ灏版繛鑼枛楠炲啫顫滈埀顒勫箖濞嗘挸绠甸柟鍝勬鐎垫牠姊绘担鐟板闁搞劌宕叅婵せ鍋撳┑?

    def _capital_target_equity(self) -> float:
        """Return the trading-account equity target after a flat position."""
        if TRADING_ACCOUNT_TARGET <= 0:
            return 0.0
        if CROSS_COPY_PROTECT_ENABLED:
            return TRADING_ACCOUNT_TARGET + max(CROSS_COPY_PROTECT_EQUITY_USDT, 0.0)
        return TRADING_ACCOUNT_TARGET

    async def _capital_account_value(self, client: OKXClient) -> float:
        """Return the value used for capital restoration checks."""
        if CROSS_COPY_PROTECT_ENABLED:
            return await client.get_equity("USDT")
        return await client.get_balance("USDT")

    async def _cycle_account_value(self, client: OKXClient) -> float:
        """Return account equity used as the per-cycle PnL baseline."""
        return await client.get_equity("USDT")

    async def _record_cycle_start_account_value(
        self,
        client: OKXClient,
        reason: str,
        force: bool = False,
    ) -> float:
        """Persist the trading-account equity baseline for the current cycle."""
        if not force and self._state.cycle_start_account_value > 0:
            return self._state.cycle_start_account_value
        try:
            account_value = await self._cycle_account_value(client)
        except Exception as e:
            logger.warning(f"Record cycle start account value failed reason={reason}: {e}")
            return 0.0

        self._state.cycle_start_account_value = round(account_value, 4)
        self._state.cycle_start_ts = pd.Timestamp.utcnow().isoformat()
        log_check(
            f"Cycle start account value recorded value={self._state.cycle_start_account_value:.4f} "
            f"reason={reason}"
        )
        self._save_runtime_state()
        return self._state.cycle_start_account_value

    async def _fetch_account_equity_close_pnl(self, client: OKXClient) -> dict | None:
        """Return realized cycle PnL from start/end account equity when available."""
        start_value = self._state.cycle_start_account_value
        if start_value <= 0:
            return None
        try:
            end_value = await self._cycle_account_value(client)
        except Exception as e:
            logger.warning(f"Fetch account-equity close PnL failed; fallback to fills: {e}")
            return None

        pnl = round(end_value - start_value, 4)
        log_action(
            f"Actual close PnL from account equity diff actual_pnl={pnl:+.4f} USDT "
            f"start={start_value:.4f} end={end_value:.4f}"
        )
        return {
            "pnl": pnl,
            "start": round(start_value, 4),
            "end": round(end_value, 4),
        }

    async def _calibrate_capital_after_close(self, client: OKXClient) -> float:
        """Align trading account capital after realized-PnL transfer."""
        target = self._capital_target_equity()
        if target <= 0:
            return 0.0
        if self._state.is_active():
            logger.debug("[Capital] Skip post-close calibration while state is active")
            return 0.0

        if CAPITAL_REBALANCE_DELAY_SEC > 0:
            await asyncio.sleep(CAPITAL_REBALANCE_DELAY_SEC)

        try:
            account_value = await self._capital_account_value(client)
            diff = round(account_value - target, 4)
            tolerance = max(CAPITAL_REBALANCE_TOLERANCE_USDT, 0.0)
            if abs(diff) <= tolerance:
                logger.info(
                    f"[Capital] Trading account aligned value={account_value:.4f} "
                    f"target={target:.4f} tolerance={tolerance:.4f}"
                )
                if self._capital_shortage_active:
                    self._capital_shortage_active = False
                    self._save_runtime_state()
                    await notify_capital_restored(account_value, target)
                return 0.0

            if diff > tolerance:
                trading_bal = await client.get_balance("USDT")
                transfer_amt = round(min(diff, trading_bal), 4)
                if transfer_amt < 0.01:
                    logger.warning(
                        f"[Capital] Equity above target but no transferable balance "
                        f"value={account_value:.4f} target={target:.4f} avail={trading_bal:.4f}"
                    )
                    return 0.0
                logger.info(
                    f"[Capital] Post-close excess value={account_value:.4f} "
                    f"target={target:.4f}; transfer {transfer_amt:.4f} USDT to funding"
                )
                await client.transfer(amt=transfer_amt, from_acct="18", to_acct="6")
                return transfer_amt

            needed = round(abs(diff), 4)
            funding_bal = await client.get_funding_balance("USDT")
            top_up = round(min(needed, funding_bal), 4)
            shortage = top_up + 0.0001 < needed
            if top_up >= 0.01:
                partial = " (partial top-up; funding insufficient)" if shortage else ""
                logger.info(
                    f"[Capital] Post-close below target value={account_value:.4f} "
                    f"target={target:.4f}; top up {top_up:.4f} USDT{partial}"
                )
                await client.transfer(amt=top_up, from_acct="6", to_acct="18")

            if shortage:
                if not self._capital_shortage_active:
                    self._capital_shortage_active = True
                    self._save_runtime_state()
                await notify_capital_shortage(account_value + top_up, target, funding_bal, top_up)
            elif self._capital_shortage_active:
                self._capital_shortage_active = False
                self._save_runtime_state()
                await notify_capital_restored(account_value + top_up, target)
            return -top_up
        except Exception as e:
            logger.warning(f"[Capital] Post-close calibration failed; strategy continues: {e}")
            return 0.0

    async def _rebalance_accounts(self, client: OKXClient, actual_pnl: float | None = None) -> float:
        """Keep capital by transferring the latest realized PnL when known."""
        """
        濠电姷鏁告慨鐑藉极閸涘﹥鍙忛柣鎴ｆ閺嬩線鏌涘☉姗堟敾闁告瑥绻橀弻锝夊閻樺樊妫岄梺杞扮閿曨亪寮婚垾鎰佸悑閹肩补鈧磭顔愰梻鍌氬€搁崑鍡涘垂闁秴桅闁告洦鍨伴崘鈧梺闈浤涢崨顖氬笌缂傚倸鍊峰ù鍥╃礄娴兼潙纾规繝闈涱儏閽冪喖鏌ㄥ┑鍡╂Ч闁哄懏鐓￠弻娑樷槈閸楃偞鐏嶉梺鍦厴娴滃爼骞冨Δ鍐╁枂闁告洦鍓涢ˇ銊╂⒑缂佹ɑ鎯堢紒缁樼箓椤曪絾绻濆顓炰簻闂佸憡绋戦敃锔剧矓閸洘鈷戦悹鍥ｂ偓宕団偓濠氭煕濞戝崬鏆曢柟鐑橆殕閳锋帒霉閿濆牆袚闁靛棗鍟扮槐鎺楀焵椤掍胶鐟归柍褜鍓欓悾鐑筋敍閻戝棙鏅濋梺鎸庢磵閸嬫捇鏌ｉ幘杈捐€块柡宀€鍠愬蹇涘礈瑜忛弳鐘电磼閻愵剙鍔ら柛姘儔楠炲牓濡搁妷顔藉缓闂佺硶鍓濋〃鍛不濞差亝鈷戦柛婵勫劚鏍￠梺鍛婃⒐閻熲晠鐛崘銊庢棃宕橀埡浣圭€梻浣告啞濞诧箓宕滃▎鎾村剹妞ゆ柨澧界壕钘壝归敐鍫燁棄闁绘挻鍔楅埀顒€鍘滈崑鎾剁磼鐎ｎ偒鍎ラ柛銈嗘礋閹綊宕堕妸褋鍋炲┑鈩冨絻閻楀﹦鎹㈠┑鍥╃瘈闁稿本绋戝▍褔姊洪崫鍕垫Ч闁诡喖鍊搁～蹇撁洪鍕祶濡炪倖鎸鹃崑妯荤珶閺囩偐鏀介柣鎰絻閹垿鏌ｉ悢鏉戝姦闁糕斂鍨归濂稿醇椤愶及鈺呮⒒娴ｅ摜鏋冩い顐㈩樀瀹曞綊宕稿Δ鈧粻鏍ㄧ箾閸℃绂嬮柛鐔锋噺閵囧嫰寮崹顔肩紦闂侀潧鐗嗛ˇ浼村煕閹达附鐓欓柤娴嬫櫅娴犳粌鈹戦檱濞咃絿妲愰幒妤婃晩闁伙絽鏈崳褍顪冮妶鍡樼┛缂佹彃娼￠獮蹇涙偐鐟佷礁婀遍埀顒婄秵閸嬫帒顭囬弮鈧换婵嗏枔閸喗鐏嶅銈冨妼閹冲氦鐏嬪┑鐐叉閹稿摜澹曟禒瀣拻闁割偆鍠嶇欢杈ㄧ箾閹炬剚鐓奸柡灞炬礋瀹曠厧鈹戦崶鑸殿棧缂傚倷鐒﹂〃鍛村磹閼姐倖顫曢柟鐑橆殔閻掑灚銇勯幒宥堝厡闁荤喎缍婇弻宥堫檨闁告挻鐟╅、姘舵晲婢跺á鈺呮煃閸濆嫸鏀婚柡鍜冪秮濮婃椽妫冨☉姘辩暰濠碉紕瀚忛崶褏顔嗛梺鍛婄箓鐎氀囧绩閼恒儯浜滈柡鍐ㄥ€告禍楣冩煕閵堝倸浜惧┑鐘垫暩閸嬫盯鎯囨导鏉戠９闁哄秲鍔庨埞宥呪攽閻樺弶鎼愰梺瑁ゅ€栨穱濠囧Χ閸曨喖鍘￠梺鍛婏耿娴滆泛顫忛搹鍦＜婵☆垰娴氭禍鐐寸珶閺囥埄鏁囬柣鏂挎啞閻濈兘姊洪崜鑼帥闁稿甯掗埢鎾寸鐎ｎ偆鍘介梺褰掑亰閸樿偐寰婃繝姘厸闁糕剝鐟ユ禒褏绱掓潏銊ユ诞鐎规洘甯掗埥澶娢熺喊鍗炲壃闂傚倷绶氶埀顒傚仜閼活垱鏅舵ィ鍐╃厵妞ゆ垶鍎抽崝锕傛煕閳规儳浜炬俊鐐€栫敮鎺斺偓姘煎墰婢规洘绻濆顓犲幍闂佸憡鎸嗛崨顓狀偧闂備礁鎼幊蹇曟崲閸儱钃熸繛鎴炵煯濞岊亪鏌ｉ幇闈涘婵炲牄鍊曢—鍐Χ閸℃鍙嗛梺鎸庢处娴滄粓锝炶箛鎾佹椽顢旈崟顏嗙倞闂備礁鎲″ú锕傚磻閸涱厸鏋旀い鎾卞灪閳锋垹绱掔€ｎ亜鐨″顐ｇ閵囧嫰寮撮崱妤佸闁?
          闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁炬儳缍婇弻锝夊箣閿濆憛鎾绘煕閵堝懎顏柡灞剧洴椤㈡洟鏁愰崱娆樻К缂備胶鍋撻崕鍐差焽閿熺姴钃熼柨婵嗩槸椤懘鏌曡箛濠冩珖闁告梹鎮傚鍝勑ч崶褉鍋撳Δ鍛；闁规崘鍩栧畷鍙夌箾閹存瑥鐏╃紒鐙呯稻缁绘盯宕卞Δ鍐唺婵炲濞€缁犳牕顫忛搹鍦煓婵炲棙鍎抽崜鏉款渻閵堝棗鐏ユい锕傛涧椤曪絿鎷犲ù瀣潔闂侀潧绻掓慨鐑藉礉閹绢喗鈷戦柛娑橈工婵箑霉濠婂嫮澧崡閬嶆煕椤愮姴鍔滈柍?-> 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧湱鈧懓瀚崳纾嬨亹閹烘垹鍊炲銈嗗笒椤︿即寮查鍫熷仭婵犲﹤鍟扮粻缁橆殽閻愭潙鐏村┑顔瑰亾闂侀潧鐗嗛幊鎰版偪閳ь剚淇婇悙顏勨偓鏍涙担鑲濇盯宕熼浣稿妳婵犵數濮村ú锕傚煕閹寸姵鍠愰柣妤€鐗嗙粭鎺懨瑰鈧崡鎶藉蓟濞戞瑦鍎熼柕濠忛檮闁款參姊虹€圭姵顥夋い锔诲灦閿濈偛顭ㄩ崼婵嗚€垮┑鐐叉缁绘劕效閺屻儲鈷掗柛灞剧懅椤︼箓鏌熷ù瀣⒉缂佹鍠庤灃闁告侗鍘奸悗顓㈡⒑鐟欏嫬鍔跺┑顔哄€濆畷?TRADING_ACCOUNT_TARGET 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁炬儳缍婇弻锝夊箣閿濆憛鎾绘煕婵犲倹鍋ラ柡灞诲姂瀵挳鎮欏ù瀣壕鐟滅増甯掔壕鍧楁煙鐎电校闁哥姵鍔欓弻锝呂旈埀顒勬偋閸℃瑧绠旈柟鐑樻⒒绾惧ジ鏌嶈閸撴艾顕ラ崟顓濇勃缂佸銇樻竟鏇㈡⒑缁嬭法绠版い锔诲灡缁傚秵銈ｉ崘鈹炬嫼闂備緡鍋嗛崑娑㈡嚐椤栨稒娅犻柟缁㈠枟閻撴洟鏌嶇憴鍕姢濞存粎鍋撴穱濠囨倷椤忓嫧鍋撻弽顐ｆ殰濠电姴娲﹂崵鍕煕椤愶絾绀冮柛瀣€归妵鍕冀椤愵澀娌梺缁樻尰濞叉ɑ绌辨繝鍥ч柛灞剧煯婢规洟姊绘担鍛婃喐闁哥姴楠搁…鍥晸閻樿尙鐣烘俊銈忕到閸燁垶宕愭繝姘參婵☆垯璀﹀Σ鍝モ偓瑙勬尭濡繂顫忓ú顏咁棃婵炴垶姘ㄩ濠冪節濞堝灝鏋ら柛蹇旓耿閵嗕礁鈻庨幘宕囶唺闂佽鎯岄崢鐣屸偓闈涚焸濮婃椽妫冨☉姘暫濠碘槅鍋呴悷鈺勬＂闂佺硶鍓濈粙鎺楁偂閺囥垺鍊甸柨婵嗛娴滄粌鈹戦鑲┬ч柟绋匡工閳规垿宕堕妷銈囩泿闂備浇顫夊姗€鎮ラ悡搴€舵い鏇楀亾闁哄苯绉归弻銊р偓锝庝簽娴犺偐绱撴担浠嬪摵閻㈩垽绻濋妴浣糕槈濡攱顫嶅┑鐐叉閸╁牆危椤掆偓閳规垿鎮╅锝咁€忛梺鍛婂姂閸斿孩顨ラ崶顒佲拺闂傚牊绋掗幖鎰版倵濮樺崬顣煎ǎ鍥э躬楠炴牗鎷呴懖婵勫姂閺屻劑寮村Δ鈧禍鎯р攽閻愯尙澧涙い銊ワ工椤繘鎼圭憴鍕彴闂佺偨鍎村▍鏇㈡倶瀹ュ鐓㈤柛鎰靛幒閸氼偆绱掓潏銊ョ缂佽鲸甯掕灒闁兼祴鏅濋弶浠嬫煟鎼淬値娼愭繛鍙夛耿瀹曟洘绺介弶鍡楁喘瀵爼骞婇搹顐ｎ棃闁诡喒鏅犲畷锝嗗緞婵犲啰浜峰┑鐘愁問閸犳牠鏁冮妸銉㈡瀺闁挎繂娲﹂～鏇㈡煙閻戞ê娈鹃柣鏂垮悑閹偤姊洪锝団姇婵☆垰锕ら埞鎴︽偐濞堟寧姣屽┑鈩冨絻閹虫ê鐣烽幋锕€宸濇繛锝庡厴閸嬫捇宕橀鐓庤€垮┑鐐村灦椤洭寮昏濮婃椽宕滈幓鎺嶇凹濠电偠顕滅粻鎾愁嚕椤愩倐鍋撻敐搴℃灍闁绘挸鍟伴幉绋款煥閸繄顦梺缁樻煥椤ㄥ酣宕甸弴銏″仯闁搞儯鍔庨妶瀛樹繆閹绘帞澧﹂柡灞炬礉缁犳盯寮撮悙鎰╁劦閺屾稑螣閻戞ê鏆堢紓浣虹帛缁嬫帒顭囪箛娑樼鐟滃繗鈪插┑鐘垫暩閸嬫盯骞忛幋鐘茬筏闁告挆鈧崑鎾绘濞戞牕浠悗瑙勬礀閻栧ジ銆佸Δ浣瑰闁告瑥顦褰掓⒒閸屾瑧绐旀繛鑹板吹閳ь剟娼ч惌鍌氱暦閿濆棙鍎熼柍钘夋閸旂兘姊洪崷顓炲妺妞ゃ劌妫濋幃锟犲即閵忥紕鍘繝銏ｆ硾椤戝懘鎮橀妷鈺傜厵鐎瑰嫰鍋婇崕蹇涘础闁秵鍋℃繛鍡楃箰椤徰呯磼鐠囧弶顥㈤柡宀嬬秮楠炲洭宕楅崫銉ф晨闂備線鈧偛鑻晶顖炴煟濡ゅ啫鈻堟鐐插暢椤︽煡鎽堕弽顓熺厱闁规壋鏅涙俊浠嬫煟鎼淬垹鈻曢柟顔筋殜閻涱噣宕归鐓庮潛婵＄偑鍊х紓姘卞垝濞嗗浚鍤曟い鎰剁畱绾惧ジ鏌ｉ幇顒夊殶闁告﹩浜濈换婵嬪閿濆棛銆愬┑鈽嗗亝閻熴儵鍩㈠澶婂耿婵炴垶鐟ч崢浠嬫⒑鐟欏嫬绀冩繛澶嬬洴瀵鈽夐姀锛勫幐閻庡厜鍋撻悗锝庡墮閸╁矂鏌ф导娆戠М闁哄本绋戦埞鎴﹀幢濡ゅ﹣鐥繝娈垮櫙缁查箖宕濋弽顐ｅ床婵犻潧妫岄弸鏃堟煕椤垵鏋熸繛鍫弮濮婃椽骞栭悙鎻掑闂佸憡鏌ㄩˇ鐢哥嵁韫囨梻绡€婵﹩鍘搁幏娲⒑閸涘﹦鈽夐柨鏇樺劜瀵板嫰宕熼娑氬幈闁诲函缍嗘禍婊堫敂椤撶喆浜滈柕蹇婂墲椤ュ牊銇勯姀鈽呰€块柟顔规櫊椤㈡洟锝為鐑嗘?
          婵犵數濮烽弫鍛婃叏閻戣棄鏋侀柛娑橈攻閸欏繘鏌ｉ幋锝嗩棄闁哄绶氶弻鐔兼⒒鐎靛壊妲紒鎯у⒔缁垳鎹㈠☉銏犵婵炲棗绻掓禒楣冩⒑缁嬫鍎嶉柛濠冪箞瀵寮撮悢铏诡啎閻熸粌绉瑰畷顖烆敃閿旇棄鈧泛鈹戦悩鍙夊闁稿﹦鏁婚弻娑滅疀閹垮啯笑婵炲瓨绮撶粻鏍ь潖濞差亝鐒婚柣鎰蔼鐎氭澘顭胯閻°劑銆冮妷鈺傚€烽柡澶嬪灩娴煎本绻濆▓鍨灀闁稿鎹囧娲濞戞艾顣洪梺纭呮珪閸旀瑩銆佸鈧畷顐﹀礋閵婏附鏉搁梻浣虹帛椤洨鍒掗姘ｆ鐟滄棃寮婚敐鍛傛棃鍩€椤掑嫭鏅濋柕鍫濐槸閻?-> 婵犵數濮烽弫鍛婃叏閻戣棄鏋侀柛娑橈攻閸欏繘鏌ｉ幋锝嗩棄闁哄绶氶弻鐔兼⒒鐎靛壊妲紒鎯у⒔缁垳鎹㈠☉銏犵婵炲棗绻掗崝鎾⒑鏉炴壆顦︽い鎴濇婵＄敻宕熼姘鳖啋闁荤姾娅ｉ崕銈夋倵妤ｅ啯鈷戦柛娑橈功閹冲啰绱掔紒姗堣€跨€殿喖顭烽崺鍕礃閵娧呯嵁闂佽鍑界紞鍡樼閻愬搫绀夌紒瀣氨閺€浠嬫煟閹邦垰鐨烘慨锝囧仱閺岋繝宕ㄩ姘ｆ瀰閻庢鍠栭…閿嬩繆閹间礁鐓涘ù锝堫嚃濡喖姊绘担绋款棌闁绘挸鐗撳畷浼村箳濡も偓缁犱即鏌涢幇闈涙灍闁抽攱鍨剁换娑㈡嚑妫版繂娈Δ鐘靛仦閻楃娀寮婚敐鍛傛棃鍩€椤掑嫭鏅濇い蹇撴噸缁诲棝鏌ｉ姀銏╃劸缂佲偓鐎ｎ偁浜滈柡宥冨妽閻ㄦ垿鏌ｉ妶鍌氫壕闂傚倸鍊风粈浣圭珶婵犲洤纾婚柛娑卞姸閸濆嫷娼ㄩ柍褜鍓熼妴渚€骞橀幇浣告倯婵犮垼娉涢鍌炲箯閾忓湱纾介柛灞剧懅閸斿秹鎷戦柆宥嗗€堕煫鍥风到瀵噣鏌＄仦鍓р姇缂佺粯绻堝畷鎺戔槈濡妯婇梺璇插椤旀牠宕伴弽顓涒偓锕傛倻閽樺鐣洪梺闈涚箞閸婃洖顔忓┑鍡忔斀闁绘ɑ褰冮顐︽嚃閺嶎厽鈷掗柛灞剧懅椤︼箓鏌熺拠褏绡€妤犵偞鍔欏畷鍗炩槈濡⒈妲伴梻渚€娼ц噹闁告劑鍔庨悷鈺傜節閻㈤潧啸妞わ綆鍠氬Σ鎰板即閵忕姷锛涢梺纭呮彧缁犳垹澹曢崷顓熷枑闁哄啫鐗嗙粻鐘崇箾閸℃ê绔惧ù婊冪秺閺岀喖鎮滃Ο铏逛患闂佸搫鐫欓崶銊㈡嫼缂傚倷鐒﹂敃鈺呮倿閻愵兙浜滈柡鍥ф濞诧妇鈧艾鎳橀弻锝夊棘閸喗鍊梺缁樻尵閸犳牠寮婚敓鐘茬闁靛绠戦崑宥夋⒑闁偛鑻崢鎾煕鐎ｎ偅宕屾慨濠呮缁瑥鈻庨幆褍澹夐梻浣告贡閹虫挸煤閵堝牏浜遍梻浣告啞閸旀垿宕濇径濞綁宕奸悢铏诡啎闂佺硶鍓濋〃鍫㈢不閸欏绠鹃柛顐ゅ櫐閼版寧鎱ㄦ繝鍕笡闁瑰嘲鎳橀幖褰掓偡閹殿噮鍋ч梻浣圭湽閸╁嫰宕规潏鈺傛殰闁圭儤銇涢埀顒婄畵瀹曞爼顢楁担瑙勵仧闂備胶绮…鍫焊濞嗘搩鏁婇柛銉ｅ妿缁♀偓闂佹眹鍨藉褍鐡繝鐢靛仩椤曟粎绮婚幘宕囨殾闁硅揪绠戠粈瀣亜閺嶃劎銆掗柛姗€浜跺Λ鍛搭敃閵忊€愁槱闂佺厧婀遍崑鎾剁矉瀹ュ牄浜归柟鐑樻尵閸樺崬鈹戦濮愪粶闁稿鎸搁湁婵犲﹤妫欑涵鐐亜椤愩垻绠伴悡銈嗐亜韫囨挻濯兼俊顐㈠暙閳规垿鎮欓弶鎴犱淮缂佸墽铏庨崢鎯р槈閻㈢鐒垫い鎺戝閳锋帒霉閿濆懏鍟為柛鐔哄仱閺屾盯骞欓崘銊モ拫閻庤娲忛崝搴ㄥ焵椤掍胶鈯曢柨姘舵倵閻熼偊妲搁柍瑙勫灴閹晠宕归锝嗙槑濠电姵顔栭崰姘跺极婵犳哎鈧礁鈻庨幘鍐插敤濡炪倖鎸鹃崑鐔兼偘閵夈儮鏀介幒鎶藉磹閺囥垹绠犻煫鍥ㄧ☉閻ょ偓绻濋棃娑欏偍濞存粍绮撻弻鐔煎箥閾忣偅鐝旈梺閫炲苯澧い銊ユ缁瑦寰勬繝搴℃倯婵犮垼娉涢鍥储閻㈠憡鈷戠紓浣姑慨锕傛煕閹惧鎳勯柡鍛埣椤㈡盯鎮欑€电骞堥梻渚€娼ч悧鍡椢涘▎鎴犵焼闁逞屽墴濮婂宕惰濡偓闂佸搫鏈粙鏍不濞戙垹绠婚柧蹇ｅ亜閳ь剦鍨崇槐鎾诲磼濮樻瘷锝夋煕閵娿儲璐℃俊鍙夊姍楠炴帒螖娴ｉ晲姹楅柣搴ｆ嚀婢瑰﹪宕板璺鸿Е閻庯綆鍠楅埛鎴︽煕濠靛棗顏柣鎺曟硶缁辨挸顓奸崟顓犵崲濡ょ姷鍋涢崯瀛樻叏閳ь剟鏌曢崼婵囶棞濞存粍顨婇弻鐔兼偂鎼达絾鎲肩紓浣筋嚙閸婂灝顕ｉ幖浣搁唶闁绘棁娅ｉ惁鍫㈢磼閸撗冾暭闁挎艾顭胯閻擄繝寮婚敐澶婄妞ゆ牗鑹鹃埛澶岀磽娴ｇ鈧摜绮旈崼鏇炵闁告洦鍓涢悷瑙勩亜閺嶃劎銆掔紒瀣╃窔濮婄粯鎷呴悷鏉垮Б缂備胶绮〃濠囨晲閻愭潙绶為柟閭﹀墰閻涖儵姊虹化鏇炲⒉缂佸甯￠幃锟犲即閵忥紕鍘搁梺鎼炲劘閸庤鲸淇婇悡搴唵鐟滃孩绔熼崱娆愵潟闁圭儤鏌￠崑鎾绘晲鎼存繄鍑归梺鍝ュУ钃遍柟渚垮妽缁绘繈宕橀埞澶歌檸婵°倗濮烽崑鐐烘偋閻樿绠栨繛鍡楃贩閸︻厸鍋撻敐搴濈胺濠㈣娲熷娲传閸曨厸鏋嗛梺鍛娒肩划娆忕暦閹寸姭鍋撻敐搴′簽缂佲檧鍋撳┑鐘垫暩婵挳宕愰幖浣哥畺闁冲搫鎳忛悡銉︾節闂堟稒锛嶆俊鎻掓憸缁辨帡鎮╁畷鍥ｅ闂侀潧娲ょ€氫即鐛€ｎ喗鏅查柛娑樻噺閹瑰洭寮婚敓鐘茬＜婵°倐鍋撳ù婊堢畺濮婂宕掑顑藉亾閻戣姤鍊块柨鏇炲€哥粻鏍煕椤愶絾绀€缁剧偓瀵ч妵鍕冀椤愵澀绮剁紓浣插亾濠㈣泛澶囬崑鎾诲礂婢跺﹣澹曢梻浣告啞濞叉牠鎮樺璺虹柧婵炴垯鍨洪埛鎴︽煟閻斿憡绶查柍閿嬫⒒缁辨帡顢氶崨顓犱桓闂佺硶鏅滈惄顖炵嵁鐎ｎ喗鏅滈柣锝呰嫰楠炲牓姊绘担鐑樺殌濠⒀傜矙楠炲﹪骞樼紒妯哄壄濠电偛妯婃禍婵嬫偂閺囩喍绻嗘い鏍ㄧ箓閸氳绻涢崣澶嬪唉闁哄矉绱曟禒锕傚礈瑜庨崚娑㈡⒑鐠団€虫灀闁哄懏鐩幃楣冩倻閽樺鍊為悷婊冾樀楠炲繘鏁撻悩鏂ユ嫽婵炶揪绲块崕銈夊吹閳ь剟姊洪幖鐐茬仾闁绘搫绻濋妴渚€寮介妸銉х獮婵犵數濮存绋库枔閵婏妇绡€闁汇垽娼ф牎闂佽壈顫夐崕鎶藉极椤曗偓濮婄粯鎷呴悷閭﹀殝濠殿喖锕ょ紞濠傜暦閺囥垹绠柣锝呰嫰缁侊箑鈹戞幊閸婃挸顪冮幒鏃€宕查柛鈩冪⊕閻撶喖鏌熼弶鍨倎缂併劌顭烽弻宥堫檨闁稿繑鐟╁畷鎰攽閸℃瑦娈鹃梺纭呮彧缁犳垹绮婚搹顐＄箚闁靛牆鍊告禍鍓х磽娴ｆ彃浜鹃梺鍓插亞閸犳挾绮绘ィ鍐╃厱闁斥晛鍘鹃鍛弿闁告劦浜炵壕濂告偣閸パ冪骇妞ゃ儯鍨介弻锛勪沪閻旈攱顥犻柣銈傚亾闂備胶鎳撴晶浠嬎夐幇顔藉厹闁逞屽墰缁辨挻鎷呴悷鏉垮Б婵犫拃鍌滅煓鐎殿喗鐓￠、鏇㈡晝閳ь剟鎮為崹顐犱簻闁瑰搫妫楁禍鍓х磽娴ｅ搫孝缂佸鎳撻悾鐑藉即閵忕姷顢呴梺缁樺姇缁夌數绮欓幋锝囦簷闂備礁鎲℃笟妤呭窗閺嶎厼鐒垫い鎺嗗亾闁绘牕銈稿濠氬Ω閳轰礁宓嗛梺缁樺姈缁佹挳宕戦幘骞夸汗闁圭儤鎸告禍妤呮⒑闂堟侗妾у┑鈥虫川缁粯銈ｉ崘鈺冨幍闁诲海鏁搁…鍫熺瑜旈弻鐔煎礃閹绘帗娈梺瀹狀潐閸ㄥ潡銆佸▎鎾崇闁绘挸瀛╅悘搴ㄦ⒒娴ｅ憡鎯堥柤娲诲灣缁棃宕奸弴鐐电枀闂佸湱铏庨崰鏍矆鐎ｎ偁浜滈柟鎵虫櫅閳ь剚鎸惧Σ鎰攽鐎ｎ偆鍘介柟鍏肩暘閸娿倕顭囬幇顓犵闁告瑥顦辨晶鐢告煙椤斿搫鍔滅紒铏规櫕缁瑩骞愭惔锝傚亾椤掑嫭鈷戦柛娑橈工婵箓鏌涢悩宕囧⒌闁诡喚鍋ゅ畷褰掝敃閻樿京鐩庨梻浣告贡閸庛倝宕归悽鍓叉晜闁冲搫鎳忛悡鏇㈡煏婵炲灝鍔氶柍褜鍓欏﹢閬嶅箲閵忕姭鏀介悗锝庡亽濡啫鈹戦悙鏉戠仸闁荤喆鍎崇划锝呪槈濮樿京锛濇繛杈剧到瀵爼顢撻崱娑欑厱閻庯綆鍋呭畷灞炬叏婵犲嫮甯涚紒妤冨枛閸┾偓妞ゆ巻鍋撴い顓炴穿椤﹁櫕銇勯妸锝呭姤缂佺姵鐩鎾倷閻㈢數鎽岄梻鍌欑閹诧繝骞愰崱娑樼妞ゆ劑鍨圭粻鏌ユ⒒閸屾瑧顦﹂柟璇х節楠炴劙宕卞☉妯虹獩濡炪倖鐗撻崐妤佹償婵犲啰绡€闁汇垽娼цⅷ闂佹悶鍔庨崢褔鍩㈤弬搴撴闁靛繆鏅滈弲鐐烘⒑缁洖澧查柣鐔村€濋幃鐐寸節閸ャ劎鍙嗛梺鍝勫暙閻楀﹪寮冲鍫熺厱?TRADING_ACCOUNT_TARGET
        TRADING_ACCOUNT_TARGET = 0 闂傚倸鍊搁崐鎼佸磹閹间礁纾归柟闂寸绾惧綊鏌熼梻瀵割槮缁惧墽鎳撻—鍐偓锝庝簼閹癸綁鏌ｉ鐐搭棞闁靛棙甯掗～婵嬫晲閸涱剙顥氬┑掳鍊楁慨鐑藉磻閻愮儤鍋嬮柣妯荤湽閳ь兛绶氬鎾閳╁啯鐝栭梻渚€鈧偛鑻晶鎾煙椤斿吋鍋ユい銏＄懄閹便劑骞囬鍡欐晨闂傚倷绀侀幖顐ょ矙娓氣偓瀹曟垿宕熼鍌ゆ祫濠电姴锕ら幊鎰涢鐐寸厵妞ゆ牕妫楅幊宥夋惞鎼淬劍鈷戦悗鍦濞兼劙鏌涢妸銉т虎闁伙絿鍏橀獮鎺楀箣椤撶喎鍏婃俊鐐€栭幐楣冨磻閻愭祴鏋旀慨妞诲亾婵﹦绮幏鍛瑹椤栨粌濮奸梻浣瑰濞插繘宕愬┑瀣伋闁挎洖鍊归崐濠氭煢濡警妲奸柟鑺ユ礋濮婃椽妫冨ù銉ョ墦瀵彃鈽夊鍗炴殫閻庡箍鍎遍ˇ浼存偂閺囥垺鐓忓鑸得弸銈吤归悩顔肩伈闁哄瞼鍠栭獮鎴﹀箛椤?
        """
        if TRADING_ACCOUNT_TARGET <= 0:
            return 0.0
        try:
            if actual_pnl is not None:
                actual_pnl = round(actual_pnl, 4)
                if abs(actual_pnl) <= 0.01:
                    logger.debug("[Capital] Actual PnL is within 0.01 USDT; skip transfer")
                    return 0.0

                if actual_pnl > 0:
                    trading_bal = await client.get_balance("USDT")
                    transfer_amt = round(min(actual_pnl, trading_bal), 4)
                    if transfer_amt < 0.01:
                        logger.warning("[Capital] Trading balance too low to transfer realized profit")
                        return 0.0
                    logger.info(
                        f"[Capital] Profit +{actual_pnl:.4f} USDT; realized; "
                        f"transfer {transfer_amt:.4f} USDT to funding"
                    )
                    await client.transfer(amt=transfer_amt, from_acct="18", to_acct="6")
                    return transfer_amt

                needed = round(abs(actual_pnl), 4)
                trading_bal = await client.get_balance("USDT")
                funding_bal = await client.get_funding_balance("USDT")
                top_up = round(min(needed, funding_bal), 4)
                shortage = top_up + 0.0001 < needed
                if shortage and not self._capital_shortage_active:
                    self._capital_shortage_active = True
                    await notify_capital_shortage(trading_bal + top_up, TRADING_ACCOUNT_TARGET, funding_bal, top_up)
                    self._save_runtime_state()
                if top_up < 0.01:
                    if not self._capital_shortage_active:
                        self._capital_shortage_active = True
                        await notify_capital_shortage(trading_bal, TRADING_ACCOUNT_TARGET, funding_bal, 0.0)
                        self._save_runtime_state()
                    logger.warning(
                        f"[Capital] Funding balance insufficient ({funding_bal:.4f} USDT); "
                        f"cannot top up realized loss"
                    )
                    return actual_pnl
                partial = " (partial top-up; funding insufficient)" if shortage else ""
                logger.info(
                    f"[Capital] Loss {actual_pnl:.4f} USDT; realized; "
                    f"transfer {top_up:.4f} USDT from funding to trading{partial}"
                )
                await client.transfer(amt=top_up, from_acct="6", to_acct="18")
                return actual_pnl

            if CROSS_COPY_PROTECT_ENABLED:
                logger.warning(
                    "[Capital] Actual PnL unavailable in cross-copy mode; "
                    "skip balance-diff rebalance to avoid moving protected trading equity"
                )
                return 0.0

            trading_bal = await client.get_balance("USDT")
            diff = round(trading_bal - TRADING_ACCOUNT_TARGET, 4)

            if diff > 0.01:
                logger.info(
                    f"[Capital] Profit +{diff:.4f} USDT; "
                    f"trading {trading_bal:.4f} -> {TRADING_ACCOUNT_TARGET:.4f}; transfer to funding"
                )
                await client.transfer(amt=diff, from_acct="18", to_acct="6")
                return diff

            elif diff < -0.01:
                needed = abs(diff)
                funding_bal = await client.get_funding_balance("USDT")
                top_up = round(min(needed, funding_bal), 4)
                shortage = top_up < needed
                if shortage and top_up >= 0.01 and not self._capital_shortage_active:
                    self._capital_shortage_active = True
                    await notify_capital_shortage(trading_bal + top_up, TRADING_ACCOUNT_TARGET, funding_bal, top_up)
                    self._save_runtime_state()
                if top_up < 0.01:
                    if not self._capital_shortage_active:
                        self._capital_shortage_active = True
                        await notify_capital_shortage(trading_bal, TRADING_ACCOUNT_TARGET, funding_bal, 0.0)
                        self._save_runtime_state()
                    logger.warning(
                        f"[Capital] Funding balance insufficient ({funding_bal:.4f} USDT); "
                        f"cannot top up trading account"
                    )
                    return diff
                partial = " (partial top-up; funding insufficient)" if top_up < needed else ""
                logger.info(
                    f"[Capital] Loss {diff:.4f} USDT; "
                    f"transfer {top_up:.4f} USDT from funding to trading{partial}"
                )
                await client.transfer(amt=top_up, from_acct="6", to_acct="18")
                return diff

            else:
                logger.debug("[Capital] Balance is within 0.01 USDT of target; skip transfer")

        except Exception as e:
            logger.warning(f"[Capital] Transfer failed; strategy continues: {e}")
        return 0.0

    async def _check_capital_restored(self, client: OKXClient, trading_balance: float | None = None) -> None:
        """Notify once when trading capital recovers after a shortage."""
        if not self._capital_shortage_active or TRADING_ACCOUNT_TARGET <= 0:
            return
        if self._state.is_active():
            return

        target = self._capital_target_equity()
        if target <= 0:
            return
        if CROSS_COPY_PROTECT_ENABLED:
            trading_balance = await self._capital_account_value(client)
        elif trading_balance is None:
            trading_balance = await client.get_balance("USDT")
        tolerance = max(CAPITAL_REBALANCE_TOLERANCE_USDT, 0.0)
        if trading_balance + tolerance < target:
            return

        excess = round(trading_balance - target, 4)
        if excess > tolerance:
            available = await client.get_balance("USDT")
            transfer_amt = round(min(excess, available), 4)
            if transfer_amt < 0.01:
                logger.warning(
                    f"[Capital] Account above target but no transferable balance "
                    f"value={trading_balance:.4f} target={target:.4f} avail={available:.4f}"
                )
                return
            logger.info(
                f"[Capital] Account above target after top-up; "
                f"{trading_balance:.4f} -> {target:.4f}; transfer {transfer_amt:.4f} USDT to funding"
            )
            await client.transfer(amt=transfer_amt, from_acct="18", to_acct="6")
            trading_balance = target

        self._capital_shortage_active = False
        self._save_runtime_state()
        logger.info(
            f"[Capital] Trading account restored to target "
            f"{trading_balance:.4f}/{target:.4f} USDT"
        )
        await notify_capital_restored(trading_balance, target)
        await self._ensure_fixed_batch_sizes(client)

    def stop(self):
        """Request the main strategy loop to stop."""
        self._running = False
        logger.info("Strategy stopped")
