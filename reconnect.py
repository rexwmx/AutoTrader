# -*- coding: utf-8 -*-
"""
IB API ↔ TWS 断线自动重连（2026-09-28 代码审查修复）

审查发现：程序只在启动时连接一次。断线事件（disconnectedEvent）只写日志、
依赖"重新连上"（connectedEvent）的修复逻辑永远不会触发 —— **没有任何代码去重连**。
看门狗日志里"恢复后会自动重订阅"对这种断线不成立。
影响：TWS 重启/崩溃后程序还在跑，但下不了单、取不到数，强平也会失败。

修复（按审查建议）：
1) 断线后按**递进间隔**重连（确认断线后 5s 宽限 → 首次拨号；失败后
   15s → 15s → 30s → 60s → 120s → 240s → 300s 封顶），**同一 clientId**
   （ib_async connectAsync 对同一 IB 实例可再次拨号；host/port/clientId
   按首次连接的参数传入）；失败持续告警升级，直到成功或 stop()（程序退出时）
   ——绝不放弃（有持仓在途）；
2) 连上后（ib_async connectAsync 自带 startup sync：持仓 + 在途订单已重取）
   再显式同步一次并留审计痕：
   - 持仓：非零残留列出（应由盯盘/强平循环管理）；
   - 在途订单：**绝不自动撤销**（断线前提交的可能是想让它执行的合法平仓单），
     仅 CRITICAL + 人工判断 放行/撤销；
   - 行情：realtime recorder 全量重订阅（connectedEvent 钩子亦会触发，
     _recovery_in_flight 并发保护，至多多跑一次，回放去重安全）；
   - 策略：bar 驱动，数据流恢复后 on_bar 决策自然恢复（策略状态在内存中未丢失）。
3) 生命周期：程序退出清理时**先 stop() 两个 reconnector 再 disconnect()**，
   防止"程序已决定退出"之后僵尸重连。

测试：python test/test_reconnect.py
"""
import asyncio
import time

from logger import get_logger

# ==================== 重连策略参数 ====================
LOOP_TICK_SECONDS = 5                 # 主循环步长（断线检测粒度）
FIRST_ATTEMPT_GRACE_SECONDS = 5       # 首次断线确认后，给 TWS 的回程时间，再拨第一次
BASE_DELAY_SECONDS = 15               # 失败重试基础间隔
MAX_DELAY_SECONDS = 300               # 重试间隔封顶（5 分钟）
DELAY_FACTOR = 2                      # 递进倍数
CONNECT_TIMEOUT_SECONDS = 20          # 单次连接超时
ESCALATE_ATTEMPTS = 5                 # 第 N 次失败起 CRITICAL 升级（请人尽快看 TWS）


def _next_delay(attempt: int) -> float:
    """第 attempt 次失败后的等待：15s → 15s → 30s → 60s → 120s → 240s → 300s 封顶"""
    d = max(BASE_DELAY_SECONDS, BASE_DELAY_SECONDS * (DELAY_FACTOR ** (attempt - 2)))
    return float(min(d, MAX_DELAY_SECONDS))


class IBReconnector:
    """单个 IB 实例的断线自动重连器（同一 clientId，递进间隔）

    - start() 后常驻：每 LOOP_TICK_SECONDS 检查 isConnected()；
      断线 → 按递进间隔拨号（首次 5s 宽限后）；
      disconnectedEvent 监听只用于即时告警（重连由主循环统一驱动，单一执行路径）。
    - 成功：connectedEvent（ib_async emit）触发 realtime 钩子 + on_reconnected 回调
      （持仓/在途订单同步、行情重订阅、策略恢复提示）。
    - stop()：立即停止（退出清理路径必须先 stop 再 ib.disconnect()）。
    """

    def __init__(self, ib, host: str, port: int, client_id: int, name: str,
                 on_reconnected=None):
        self.ib = ib
        self.host = host
        self.port = int(port)
        self.client_id = int(client_id)
        self.name = name
        self.on_reconnected = on_reconnected   # async zero-arg callable（可选）
        self.logger = get_logger()
        self._stop = False
        self._task = None
        self._down_notified = False
        self._attempt = 0
        self._next_attempt_at = 0.0
        self._event_bound = False

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    def start(self) -> None:
        if self._stop:
            return
        if self._task is None or self._task.done():
            self._task = asyncio.ensure_future(self._loop())
        # 断线事件：即时告警（重连本身由主循环驱动 —— 单一执行路径，无并发拨号）
        if hasattr(self.ib, 'disconnectedEvent') and not self._event_bound:
            try:
                self.ib.disconnectedEvent += self._on_disconnected_event
                self._event_bound = True
            except Exception:
                self._event_bound = False
        self.logger.info(
            f"🔁 {self.name}: 断线自动重连器已启动（递进间隔，首试宽限 {FIRST_ATTEMPT_GRACE_SECONDS}s，"
            f"重试上限 {MAX_DELAY_SECONDS}s / 次；同一 clientId={self.client_id}；"
            f"重连成功后：同步持仓/在途订单 → 重订阅行情 → 策略恢复）")

    def stop(self) -> None:
        """停止重连（退出清理路径：必须先 stop 再 ib.disconnect()，防僵尸重连）"""
        self._stop = True
        task, self._task = self._task, None
        if task is not None and not task.done():
            task.cancel()
        if self._event_bound:
            try:
                self.ib.disconnectedEvent -= self._on_disconnected_event
            except Exception:
                pass
            self._event_bound = False

    def _on_disconnected_event(self, *args) -> None:
        """disconnectedEvent 监听（同步上下文，绝不能抛异常）"""
        self._mark_down(None)

    def _mark_down(self, conn_state) -> None:
        """一次"新断线"的首次确认：告警 + 重置退避 + 首试宽限（幂等，
        失败拨号引发的二次 disconnectedEvent 不会打乱退避计时）"""
        if self._stop or self._down_notified:
            return
        self._down_notified = True
        self._attempt = 0
        self._next_attempt_at = time.monotonic() + FIRST_ATTEMPT_GRACE_SECONDS
        try:
            self.logger.critical(
                f"🚨 {self.name}: 确认 IB API ↔ TWS 连接断开"
                f"（isConnected={conn_state if conn_state is not None else '事件'}）"
                f"——订单/取数/行情全部不可用；"
                f"自动重连已按递进间隔启动（同一 clientId，首试 {FIRST_ATTEMPT_GRACE_SECONDS}s 内）")
        except Exception:
            pass

    def _connected(self):
        """True=就绪 / False=断开 / None=未知（isConnected 异常）"""
        try:
            return bool(self.ib.isConnected())
        except Exception:
            return None

    # ------------------------------------------------------------------
    # 重连主循环
    # ------------------------------------------------------------------
    async def _loop(self) -> None:
        while not self._stop:
            try:
                await asyncio.sleep(LOOP_TICK_SECONDS)
                if self._stop:
                    return

                conn = self._connected()
                if conn is True:
                    if self._down_notified:
                        # 已恢复（我们重连成功或外部自愈）→ 重置退避状态
                        self._down_notified = False
                        self._attempt = 0
                        self._next_attempt_at = 0.0
                    continue

                # conn is False 或 None（未知也按断线处理：拨号是唯一的补救动作，
                # 且仅在 isConnected() 非 True 时拨号 → 不会打到"明明还连着"的 TWS 脸上）
                if not self._down_notified:
                    self._mark_down(conn)
                    continue  # 宽限期内不动作，宽限到期后下一轮 tick 起拨号

                if time.monotonic() < self._next_attempt_at:
                    continue

                self._attempt += 1
                ok = await self._attempt_connect()
                if ok:
                    self._down_notified = False
                    self._attempt = 0
                    self._next_attempt_at = 0.0
                    await self._fire_on_reconnected()
                    continue

                delay = _next_delay(self._attempt)
                self._next_attempt_at = time.monotonic() + delay
                if self._attempt >= ESCALATE_ATTEMPTS:
                    self.logger.critical(
                        f"🚨 {self.name}: 已重连失败 {self._attempt} 次（下次等待 {delay:.0f}s）——"
                        f"下单/取数/强平全部不可用，请尽快检查 TWS 进程与网络!")
                else:
                    self.logger.warning(
                        f"🔁 {self.name}: 重连 第{self._attempt}次 未成功，{delay:.0f}s 后再次尝试...")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # 重连器自身异常不允许杀死重连器
                self.logger.error(f"❌ {self.name}: 重连巡检异常（继续运行）: {e}", exc_info=True)

    async def _attempt_connect(self) -> bool:
        """一次拨号（同一 host/port/clientId）。ib_async 2.1.0 无自动重连，
        同一 IB 实例可再次 connectAsync —— 成功后其 startup sync 已重取持仓+在途订单，
        并 emit connectedEvent（realtime 钩子据此重订阅行情）。"""
        self.logger.info(
            f"🔌 {self.name}: 尝试重连 第 {self._attempt} 次 → "
            f"{self.host}:{self.port} (clientId={self.client_id})...")
        try:
            await self.ib.connectAsync(
                self.host, self.port,
                clientId=self.client_id,
                timeout=CONNECT_TIMEOUT_SECONDS)
        except Exception as e:
            self.logger.error(f"❌ {self.name}: 重连 第{self._attempt}次 失败: {e} —— 按递进间隔重试")
            return False

        conn = self._connected()
        if conn is not True:
            self.logger.error(
                f"❌ {self.name}: 重连握手返回但未就绪（isConnected={conn}）—— 按递进间隔重试")
            return False

        self.logger.info(
            f"🎉 {self.name}: 重连成功（clientId={self.client_id}）"
            f"—— 开始重连后同步（持仓/在途订单 → 行情重订阅 → 策略恢复）...")
        return True

    async def _fire_on_reconnected(self) -> None:
        if self.on_reconnected is None:
            return
        try:
            await self.on_reconnected()
        except Exception as e:
            self.logger.error(
                f"❌ {self.name}: 重连后回调异常（同步流程可能被中断）: {e}", exc_info=True)


# ----------------------------------------------------------------------
# 重连后状态同步（显式审计 + 恢复编排）
# ----------------------------------------------------------------------
async def sync_after_reconnect(ib, name: str) -> None:
    """重连成功后的状态同步与恢复（2026-09-28 审查建议的顺序）。

    1) **持仓同步**：重取持仓并列出非零残留
       （ib_async connectAsync 的 startup sync 已重取持仓/在途订单；此处是显式审计痕）；
    2) **在途订单同步**：列出断线期间仍挂在 TWS 的订单
       —— 它们可能继续成交，**绝不自动撤销**（可能是断线前提交、想让它执行的合法平仓单），
       仅 CRITICAL + 人工判断放行/撤销；
    3) **策略恢复**：策略/盯盘为 bar 驱动 + 轮询驱动，数据流恢复后自然恢复，
       状态在内存中未丢失（提示留痕）。

    说明（**行情重订阅**，审查建议的第 2 步中的"重新订阅行情"）：
       realtime recorder 已订阅 ib1.connectedEvent → reconnect 成功时 connectAsync
       在 emit connectedEvent 时由 recorder 的全量重订阅钩子（recover_feed，
       _recovery_in_flight 并发保护 + bar 时间戳去重 + 策略门控）自动完成，
       看门狗兜底。此处不重复调用，避免双跑。
    """
    logger = get_logger()

    # 1) 持仓同步
    try:
        positions = await ib.reqPositionsAsync()
    except Exception as e:
        logger.critical(f"🚨 {name}: 重连后持仓同步失败: {e} —— 请立即人工核对 TWS 持仓!")
        positions = []
    open_pos = []
    for p in (positions or []):
        try:
            if not p.position:
                continue
            sym = getattr(getattr(p, 'contract', None), 'symbol', '?')
            open_pos.append(f"{sym} {'+' if p.position > 0 else ''}{int(p.position)}股")
        except Exception:
            continue
    if open_pos:
        logger.info(
            f"🔄 {name}: 持仓同步: {len(open_pos)} 个标的有持仓（{', '.join(open_pos)}）"
            f"—— 继续由盯盘/强平循环管理")
    else:
        logger.info(f"🔄 {name}: 持仓同步: 全账户 0 持仓")

    # 2) 在途订单同步
    orders = None
    try:
        orders = await ib.reqOpenOrdersAsync()
    except Exception as e:
        logger.critical(f"🚨 {name}: 重连后在途订单同步失败: {e} —— 请人工核对 TWS API 挂单窗口!")
    active = []
    if orders:
        for t in orders:
            try:
                st = getattr(getattr(t, 'orderStatus', None), 'status', None)
                if st in ('Filled', 'Cancelled', 'Inactive'):
                    continue
                o = t.order
                sym = getattr(getattr(o, 'contract', None), 'symbol', '?')
                active.append(
                    f"{sym} {getattr(o, 'action', '?')} "
                    f"已成交 {getattr(getattr(t, 'orderStatus', None), 'filled', 0)}/"
                    f"{getattr(o, 'totalQuantity', '?')} ({st})")
            except Exception:
                continue
    if active:
        logger.critical(
            f"🚨 {name}: 重连后发现 {len(active)} 个在途订单（断线前提交、可能继续成交）: "
            f"{'; '.join(active)} —— 为避免误杀合法平仓单，未自动撤销，"
            f"请人工判断放行或撤销!")
    elif orders is not None:
        logger.info(f"🔄 {name}: 在途订单同步: 无未完结订单")

    # 3) 策略恢复提示（行情重订阅由 realtime connectedEvent 钩子完成，见 docstring）
    logger.info(
        f"🧠 {name}: 策略/盯盘自动恢复 —— 1分钟 bar 流恢复后 on_bar 决策继续；"
        f"强平监控循环按轮继续（策略状态在内存中未丢失）")
