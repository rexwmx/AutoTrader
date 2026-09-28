# -*- coding: utf-8 -*-
"""
回归测试：IB API ↔ TWS 断线自动重连（reconnect.py，2026-09-28 代码审查修复）

审查发现：程序只在启动时连接一次；断线事件只写日志，依赖"重新连上"的修复逻辑
永远不会触发（没有代码去重连）→ TWS 重启/崩溃后程序还在跑，但下不了单、取不到数、
强平失败。修复：断线后按递进间隔重连（同一 clientId）；连上后依次同步持仓与
在途订单、重订阅行情（realtime 钩子）、恢复策略（bar 驱动）。

覆盖：
  R0  递进间隔序列（15 → 15 → 30 → 60 → 120 → 240 → 300 封顶）
  R1  断线→自动重连（同一 host/port/clientId）+ 重连后回调恰好一次
  R2  stop() 之后断线不再拨号（防僵尸重连，清理顺序 stop→disconnect）
  R3  重连后同步：持仓列出 / 在途订单仅 CRITICAL + 人工判断，绝不自动撤销；
      持仓同步异常不阻断在途订单同步
  R4  纯心跳兜底：无 disconnectedEvent 也能检测断线并重连
  R5  连接正常时绝不拨号（不误伤健康连接）
  R6  重连成功后再断线 → 再次重连（循环常驻，直到 stop）
运行：python test/test_reconnect.py
"""
import asyncio
import logging
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE))

import reconnect as RC                                # noqa: E402
from logger import get_logger                          # noqa: E402

# ---- 日志捕获 ----
records = []
_log = get_logger()


class _Capture(logging.Handler):
    def emit(self, r):
        records.append(r)


_cap = _Capture()
_cap.setLevel(logging.DEBUG)
_log.addHandler(_cap)


def crit(*kw) -> bool:
    return any(r.levelno >= logging.CRITICAL and any(k in r.getMessage() for k in kw)
               for r in records)


# ---- Fake 对象 ----
class FakeEvent:
    def __init__(self):
        self.handlers = []

    def __iadd__(self, h):
        self.handlers.append(h)
        return self

    def __isub__(self, h):
        if h in self.handlers:
            self.handlers.remove(h)
        return self

    def emit(self, *args):
        for h in list(self.handlers):
            h(*args)


class FakeIB:
    def __init__(self, connected: bool = False):
        self.connected = connected
        self.disconnectedEvent = FakeEvent()
        self.connectedEvent = FakeEvent()
        self.connect_calls = []
        self.fail_first_n = 0          # 前 N 次 connectAsync 抛异常
        self.positions = []
        self.open_orders = []
        self.cancel_calls = 0
        self.pos_fail = False
        self.orders_fail = False

    def isConnected(self):
        return self.connected

    async def connectAsync(self, host, port, clientId=1, timeout=4):
        self.connect_calls.append((host, port, clientId))
        if self.fail_first_n > 0:
            self.fail_first_n -= 1
            raise ConnectionRefusedError('fake: TWS not up yet')
        self.connected = True
        self.connectedEvent.emit()

    async def reqPositionsAsync(self):
        if self.pos_fail:
            raise ConnectionError('fake: not ready')
        return list(self.positions)

    async def reqOpenOrdersAsync(self):
        if self.orders_fail:
            raise ConnectionError('fake: not ready')
        return list(self.open_orders)

    def cancelOrder(self, order):
        self.cancel_calls += 1


class FakePos:
    def __init__(self, symbol, position):
        self.contract = SimpleNamespace(symbol=symbol)
        self.position = position


class FakeTrade:
    def __init__(self, symbol, action, total, filled, status):
        self.order = SimpleNamespace(
            symbol=symbol, action=action, totalQuantity=total,
            contract=SimpleNamespace(symbol=symbol))
        self.orderStatus = SimpleNamespace(status=status, filled=filled)


def check(name, ok, detail=''):
    print(f'[{"ok" if ok else "FAIL"}] {name}' + (f' — {detail}' if detail else ''))
    return ok


async def until(cond, timeout=5.0):
    """条件为真即返回；不加速任何 sleep（本测试用真实毫秒级常量）"""
    deadline = asyncio.get_event_loop().time() + timeout
    while not cond():
        if asyncio.get_event_loop().time() > deadline:
            return False
        await asyncio.sleep(0.01)
    return True


async def main():
    failures = []

    # ---------- R0: 递进间隔序列（生产常量） ----------
    seq = [RC._next_delay(i) for i in range(1, 8)]
    ok0 = (seq == [15, 15, 30, 60, 120, 240, 300])
    print(f'   失败重试等待序列: {seq} → 之后 300s 封顶')
    if not check('R0 递进间隔 15→15→30→60→120→240→300 封顶', ok0, f'{seq}'):
        failures.append('R0')

    # ---- 测试加速：毫秒级常量（模块级读取，start() 前生效） ----
    RC.LOOP_TICK_SECONDS = 0.01
    RC.FIRST_ATTEMPT_GRACE_SECONDS = 0.02
    RC.BASE_DELAY_SECONDS = 0.02
    RC.MAX_DELAY_SECONDS = 0.06
    RC.ESCALATE_ATTEMPTS = 50

    # ---------- R1: 断线→自动重连（同一 clientId）+ 回调一次 ----------
    records.clear()
    cb_calls = []

    async def _cb1():
        cb_calls.append(len(cb_calls) + 1)

    ib = FakeIB(connected=False)
    ib.fail_first_n = 2          # 前两次拨号失败 → 第三次成功
    rec = RC.IBReconnector(ib, '127.0.0.1', 7497, 1001, '测试账户(空)', on_reconnected=_cb1)
    rec.start()
    done = await until(lambda: len(cb_calls) == 1)
    ok1 = (done
           and len(cb_calls) == 1
           and len(ib.connect_calls) == 3
           and all(c == ('127.0.0.1', 7497, 1001) for c in ib.connect_calls)
           and ib.connected is True
           and crit('连接断开'))
    print(f'   拨号={ib.connect_calls} 回调次数={len(cb_calls)} connected={ib.connected}')
    rec.stop()
    if not check('R1 断线→递进重连(同一clientId)+重连后回调恰好一次', ok1,
                 f'calls={ib.connect_calls} cb={len(cb_calls)}'):
        failures.append('R1')

    # ---------- R2: stop() 后断线不再拨号（防僵尸） ----------
    records.clear()
    ib2 = FakeIB(connected=False)
    rec2 = RC.IBReconnector(ib2, '127.0.0.1', 7487, 1002, '测试账户(多)')
    rec2.start()
    rec2.stop()                 # 清理顺序：先 stop（hedge_trade 清理段如此）
    ib2.disconnectedEvent.emit()  # 模拟程序退出时 ib.disconnect() 引发的断线事件
    await asyncio.sleep(0.15)
    ok2 = (len(ib2.connect_calls) == 0 and len(ib2.disconnectedEvent.handlers) == 0)
    print(f'   stop() 后断线事件 → 拨号={len(ib2.connect_calls)}/0 残留监听={len(ib2.disconnectedEvent.handlers)}/0')
    if not check('R2 stop() 后绝不僵尸重连', ok2):
        failures.append('R2')

    # ---------- R3: 重连后同步（持仓/在途订单：不自动撤 + 异常隔离） ----------
    records.clear()
    ib3 = FakeIB(connected=True)
    ib3.positions = [FakePos('AAPL', -100), FakePos('MSFT', +50), FakePos('XOM', 0)]
    ib3.open_orders = [
        FakeTrade('AAPL', 'BUY', 100, 0, 'Submitted'),
        FakeTrade('MSFT', 'SELL', 50, 50, 'Filled'),      # 终态 → 忽略
    ]
    await RC.sync_after_reconnect(ib3, '同步测试')
    ok3a = (ib3.cancel_calls == 0
            and crit('在途订单') and crit('未自动撤销')
            and any('AAPL' in r.getMessage() and '+50' in r.getMessage() for r in records
                    if r.levelno >= logging.INFO))
    print(f'   自动撤单={ib3.cancel_calls}/0 在途CRITICAL={crit("在途订单")} 持仓留痕 AAPL/MSFT 非零')
    if not check('R3a 在途订单仅CRITICAL+人工判断,绝不自动撤', ok3a,
                 f'cancel={ib3.cancel_calls} crit={crit("在途订单")}'):
        failures.append('R3a')

    records.clear()
    ib3b = FakeIB(connected=True)
    ib3b.pos_fail = True        # 持仓同步失败 → 不得阻断在途订单同步
    ib3b.open_orders = [FakeTrade('TSLA', 'BUY', 200, 100, 'PendingSubmit')]
    await RC.sync_after_reconnect(ib3b, '同步测试B')
    ok3b = (crit('持仓同步失败') and crit('TSLA'))
    print(f'   持仓失败告警={crit("持仓同步失败")} 在途仍同步={crit("TSLA")} 撤单={ib3b.cancel_calls}/0')
    if not check('R3b 持仓同步异常不阻断在途订单同步', ok3b):
        failures.append('R3b')

    # ---------- R4: 纯心跳兜底（无 disconnectedEvent 也能重连） ----------
    records.clear()
    ib4 = FakeIB(connected=False)
    rec4 = RC.IBReconnector(ib4, '127.0.0.1', 7497, 1001, '心跳兜底账户')
    rec4.start()
    ib4.disconnectedEvent.handlers.clear()   # 事件监听全丢 → 只剩 isConnected 心跳
    done = await until(lambda: ib4.connected is True)
    ok4 = (done and len(ib4.connect_calls) == 1 and crit('连接断开'))
    print(f'   无事件监听下拨号={len(ib4.connect_calls)}/1 connected={ib4.connected}')
    rec4.stop()
    if not check('R4 无断线事件时心跳兜底仍会重连', ok4):
        failures.append('R4')

    # ---------- R5: 连接正常时绝不拨号 ----------
    ib5 = FakeIB(connected=True)
    rec5 = RC.IBReconnector(ib5, '127.0.0.1', 7487, 1002, '健康账户')
    rec5.start()
    await asyncio.sleep(0.25)
    ok5 = (len(ib5.connect_calls) == 0 and ib5.connected is True)
    rec5.stop()
    print(f'   健康连接拨号={len(ib5.connect_calls)}/0')
    if not check('R5 连接正常时绝不拨号(不误伤)', ok5):
        failures.append('R5')

    # ---------- R6: 恢复后再断线 → 再次重连（回调两次） ----------
    records.clear()
    cb6 = []

    async def _cb6():
        cb6.append(1)

    ib6 = FakeIB(connected=False)
    ib6.fail_first_n = 1        # 第一轮：败一次成一次
    rec6 = RC.IBReconnector(ib6, '127.0.0.1', 7497, 1001, '反复断线账户', on_reconnected=_cb6)
    rec6.start()
    await until(lambda: ib6.connected is True)
    assert len(cb6) == 1, '第一次重连回调应恰好一次'
    # 二次断线（事件）
    ib6.connected = False
    ib6.disconnectedEvent.emit()
    n = len(ib6.connect_calls)
    done = await until(lambda: ib6.connected is True and len(ib6.connect_calls) > n)
    ok6 = (done and len(cb6) == 2)
    print(f'   二次断线后拨号继续={len(ib6.connect_calls)}/{n} 回调累计={len(cb6)}/2')
    rec6.stop()
    if not check('R6 恢复后再断线→再次自动重连(循环常驻)', ok6,
                 f'calls={len(ib6.connect_calls)} cb={len(cb6)}'):
        failures.append('R6')

    _log.removeHandler(_cap)

    if failures:
        print('❌ 失败:', ', '.join(failures))
        sys.exit(1)
    print('✅ 断线自动重连回归测试全部通过（递进间隔/同clientId/防僵尸/同步审计/心跳兜底）')


try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass

asyncio.run(main())
