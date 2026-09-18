# -*- coding: utf-8 -*-
"""干跑测试：断流 → 重订阅 → 回放去重/策略门控（不连 IB）

场景复现 2026-09-15 故障修复路径：
  T1 正常订阅，09:30~10:20 的 bar 陆续写入
  T2 10:21~10:35 断流丢失（bar 未推送）
  T3 10:36 恢复 → 重订阅（新 BarList 含全量历史）→
     断点续写(10:20~10:35) + 不重复(09:30~10:19) + 历史 bar 不重放策略
  T4 新 bar(≥10:36) 正常触发策略
"""
import asyncio
import sys
import tempfile
from pathlib import Path
from datetime import datetime, date as _date

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from models import StockInfo                      # noqa: E402
import realtime                                   # noqa: E402
from strategy.base import Bar                      # noqa: E402

SYM = 'TESTY'


class FakeEvent:
    def __init__(self):
        self.handlers = []
    def __iadd__(self, h):
        self.handlers.append(h)
        return self
    def __isub__(self, h):
        self.handlers.remove(h)
        return self
    def fire(*args):
        for h in self.handlers:
            h(*args)


class FakeIB:
    def __init__(self):
        self.errorEvent = FakeEvent()
        self.connectedEvent = FakeEvent()
        self.disconnectedEvent = FakeEvent()


class FakeBarsList(list):
    def __init__(self, items):
        super().__init__(items)
        self.updateEvent = FakeEvent()


class FakeBar:
    def __init__(self, dt, price=10.0):
        self.date = dt
        self.open = price
        self.high = price
        self.low = price
        self.close = price
        self.volume = 100


class RunnerStub:
    def __init__(self):
        self.calls = []

    async def on_bar(self, bar: Bar):
        self.calls.append((bar.symbol, bar.dt))

    def get_display_state(self, symbol):
        return {}


def main():
    est = realtime._EST
    today = datetime.now(est).date()

    def mk(h, m):
        return FakeBar(est.localize(datetime.combine(today, datetime.min.time()).replace(hour=h, minute=m)))

    tmp = Path(tempfile.mkdtemp())
    ib = FakeIB()
    runner = RunnerStub()
    rec = realtime.RealtimeDataRecorder(ib, tmp, runner)

    stock = StockInfo(code=SYM, exchange='', industry='', open=0, high=0, low=0,
                      close=0, volume=0, turnover_pct=0, y=0)

    def mk_from_minute(total):
        return mk(total // 60, total % 60)

    # ---------- T1: 初始订阅，bar 09:30..10:20（10:20 为未完成 bar 被 hold） ----------
    bars1 = FakeBarsList([mk_from_minute(t) for t in range(9 * 60 + 30, 10 * 60 + 21)])
    csv = rec._init_csv(SYM)
    rec._create_bar_handler(SYM, bars1, csv)

    async def fire(blist):
        # 模拟 eventkit：依次调用监听器
        for h in list(blist.updateEvent.handlers):
            h(blist, False)
        await asyncio.sleep(0.05)  # 让 create_task 的策略任务跑完

    async def run():
        await fire(bars1)
        # 断言1: 09:30..10:20 全部已写（51 行；现行语义=每根 bar 出现即处理，与线上一致）
        import pandas as pd
        df = pd.read_csv(csv)
        assert len(df) == 51, f"T1 rows={len(df)}"
        assert len(runner.calls) == 51, f"T1 strategy={len(runner.calls)}"

        # ---------- T2/T3: 10:21..10:35 断流丢失；10:36 恢复 → 重订阅全量回放 ----------
        boundary = est.localize(datetime.combine(today, datetime.min.time()).replace(hour=10, minute=36))
        bars2 = FakeBarsList([mk_from_minute(t) for t in range(9 * 60 + 30, 10 * 60 + 37)])
        rec._strategy_resume_boundary[SYM] = boundary
        rec._create_bar_handler(SYM, bars2, csv)
        await fire(bars2)

        df = pd.read_csv(csv)
        # 断流补写: 10:20..10:36 全部补齐（T1 已处理到 10:20）；09:30..10:20 不得重复
        times = list(df['time'])
        assert len(df) == 67, f"T3 rows={len(df)} (expect 67, no dup)"
        assert len(times) == len(set(times)), "duplicate bars written!"

        # 策略门控: 10:21..10:35（断流期间 15 根）被拦截；10:36 恰在边界上→放行 1 次
        assert len(runner.calls) == 52, f"T3 strategy={len(runner.calls)} (expect 52: 51 + 10:36 only)"
        assert runner.calls[-1][1].minute == 36, "10:36 should be the only newly triggered bar"

        # ---------- T4: 新 bar 推进（10:36 完成 → 10:37 到达） ----------
        bars3 = FakeBarsList(bars2 + [mk_from_minute(10 * 60 + 37)])
        rec._create_bar_handler(SYM, bars3, csv)
        await fire(bars3)

        df = pd.read_csv(csv)
        assert len(df) == 68, f"T4 rows={len(df)} (expect 68)"
        # 10:37 为正常新 bar → 放行 1 次
        assert len(runner.calls) == 53, f"T4 strategy={len(runner.calls)} (expect 53)"
        last_dt = runner.calls[-1][1]
        assert last_dt.hour == 10 and last_dt.minute == 37, f"T4 last strategy bar={last_dt}"

        print("ALL ASSERTIONS PASSED")
        print(f"  CSV rows: {len(df)} (no dup, gap backfilled)")
        print(f"  strategy triggers: {len(runner.calls)} (replayed gap bars correctly gated out)")

    asyncio.run(run())

    # ==================== 看门狗行为测试 ====================
    import datetime as _dt
    realtime.FEED_CHECK_INTERVAL_SECONDS = 1  # 加速巡检（仅测试用）

    rec2 = realtime.RealtimeDataRecorder(ib, Path(tempfile.mkdtemp()))
    calls = []
    async def fake_guarded(reason):
        calls.append(reason)
    rec2._guarded_recovery = fake_guarded
    rec2.ib.isConnected = lambda: True

    async def run_watchdog():
        rec2.start_feed_watchdog(is_quiet=lambda: False)
        now = datetime.now()
        # 断流状态：静默 400s+ > 300s 阈值 → 持续自动重试（每次巡检触发一次修复，符合设计）
        rec2._last_bar_event = now - _dt.timedelta(seconds=400)
        await asyncio.sleep(3.5)  # 覆盖 ≥3 个巡检周期，容忍调度漂移
        assert len(calls) >= 2, f"watchdog stale recovery not fired: {calls}"
        # 心跳正常 → 不再触发
        rec2._last_bar_event = datetime.now()
        await asyncio.sleep(0.4)   # 先让仍在途的巡检周期落地
        stable = len(calls)
        rec2._last_bar_event = datetime.now()
        await asyncio.sleep(2.5)
        assert len(calls) == stable, f"watchdog fired on healthy feed: {calls}"
        # quiet（强制平仓窗口）→ 不再巡检/触发
        rec2._last_bar_event = datetime.now() - _dt.timedelta(seconds=400)
        rec2._watchdog_quiet = lambda: True
        await asyncio.sleep(2.5)
        assert len(calls) == stable, f"watchdog fired while quiet: {calls}"
        rec2.stop_feed_watchdog()
        print("WATCHDOG TESTS PASSED (stale->auto-retry, healthy->stable, quiet->silent)")

    asyncio.run(run_watchdog())


if __name__ == '__main__':
    main()
