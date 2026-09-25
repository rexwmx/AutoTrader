# -*- coding: utf-8 -*-
"""
交易数据存储：事件流明细 + 日汇总（支持多次加仓 / 部分减仓 / 跨批次平仓）

目录结构：
    runtime_dir/
    ├── account1/{SYMBOL}.csv   账户1（做空腿）该股票全天事件流
    ├── account2/{SYMBOL}.csv   账户2（做多腿）该股票全天事件流
    ├── sell.csv                账户1 日汇总（按股票一行，由事件流重建）
    └── buy.csv                 账户2 日汇总（按股票一行，由事件流重建）

事件流规则：
- 每笔 OPEN/ADD 生成独立 lot_id（批次）；该股票存在未平完批次时再开仓记 ADD，
  否则（首次开仓 / 全部清仓后重新开仓）记 OPEN —— 判定依据是"未完成批次"而非"是否有过开仓行"；
- 每笔平仓事件（REDUCE/CLOSE/FORCE_CLOSE/RECONCILE）按批次分配，一行一个批次：
  related_lot_id 指向被平批次，记录 allocated_vol / remaining_vol_after；
- 一笔平仓跨多个批次 → 拆成多行（FIFO，或 target_lot_id 指定批次优先）；
- 事件流只追加，不修改历史行；汇总由 rebuild_summary 幂等重建。

佣金口径（防止汇总双重计算）：
- 开仓行 commission = 该批次单向佣金估算（仅审计参考，不计入汇总合计）；
- 平仓行 commission = 本行分配量的双向佣金：
    优先使用实际佣金（actual_commission）按分配量比例拆分，
    否则按 股数 × COMMISSION_PER_SHARE × 2 估算；
- 汇总 commission / profit / gross_profit 只累加平仓行。

并发模型：
- 本类所有方法均为同步调用（asyncio 单线程内不会交错），
  threading.Lock 按 股票 粒度作 pd 文件 I/O（GIL 可释放窗口）与
  Windows 文件占用的保险；写入遇 PermissionError 自动重试。
"""
import datetime
import threading
import time as time_module
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

from constants import ACCOUNT_EVENT_COLUMNS, ACCOUNT_SUMMARY_COLUMNS
from config import ACCOUNT1_DIR, ACCOUNT2_DIR, SELL_SUMMARY_FILE, BUY_SUMMARY_FILE
from logger import get_logger

# ==================== 事件类型分组 ====================
OPEN_TYPES = ('OPEN', 'ADD')
CLOSE_TYPES = ('REDUCE', 'CLOSE', 'FORCE_CLOSE', 'RECONCILE')

# 每股佣金估算费率（与 close.py 的 ESTIMATED_COMMISSION_RATE 一致）
COMMISSION_PER_SHARE = 0.0035


def _fmt_dt(dt) -> str:
    if dt is None:
        return ''
    if isinstance(dt, str):
        return dt
    try:
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return str(dt)


def _to_num(value, default: float = 0.0) -> float:
    """宽容地把单元格值转 float（兼容空串 / NaN / 数字字符串）"""
    try:
        if value is None:
            return default
        s = str(value).strip()
        if s == '' or s.lower() == 'nan':
            return default
        return float(s)
    except Exception:
        return default


class TradeStore:
    """交易数据存储：账户级事件流明细 + 按股票日汇总"""

    def __init__(self, runtime_dir):
        self.runtime_dir = Path(runtime_dir)
        self.account_dirs: Dict[str, Path] = {
            'account1': self.runtime_dir / ACCOUNT1_DIR,
            'account2': self.runtime_dir / ACCOUNT2_DIR,
        }
        self.summary_paths: Dict[str, Path] = {
            'account1': self.runtime_dir / SELL_SUMMARY_FILE,
            'account2': self.runtime_dir / BUY_SUMMARY_FILE,
        }
        for d in self.account_dirs.values():
            d.mkdir(parents=True, exist_ok=True)
        # 按 账户:股票 粒度的锁（防御 GIL 释放窗口 / 多线程调用）
        self._locks: Dict[str, threading.Lock] = {}
        # 事件/批次序号（每个 账户+股票 独立，event 与 lot 共用计数器保证唯一）
        self._seq: Dict[str, int] = {}
        self.logger = get_logger()

    # ------------------------------------------------------------------
    # 路径与锁
    # ------------------------------------------------------------------
    def stock_csv(self, account: str, symbol: str) -> Path:
        return self.account_dirs[account] / f"{symbol}.csv"

    def summary_csv(self, account: str) -> Path:
        return self.summary_paths[account]

    def _lock_for(self, key: str) -> threading.Lock:
        return self._locks.setdefault(key, threading.Lock())

    def _next_seq(self, account: str, symbol: str) -> int:
        key = f"{account}:{symbol}"
        self._seq[key] = self._seq.get(key, 0) + 1
        return self._seq[key]

    def _new_event_id(self, account: str, symbol: str, date_str: str) -> str:
        n = self._next_seq(account, symbol)
        return f"{date_str}-{account}-{symbol}-{n:04d}"

    def _new_lot_id(self, account: str, symbol: str, date_str: str) -> str:
        n = self._next_seq(account, symbol)
        return f"{date_str}-{account}-{symbol}-L{n:04d}"

    # ------------------------------------------------------------------
    # 内部读写
    # ------------------------------------------------------------------
    def _read(self, path: Path) -> pd.DataFrame:
        if not Path(path).exists():
            return pd.DataFrame(columns=ACCOUNT_EVENT_COLUMNS)
        try:
            df = pd.read_csv(Path(path), dtype=str, keep_default_na=False)
            # 列对齐（历史文件列缺失/多余时保持规范列集）
            return df.reindex(columns=ACCOUNT_EVENT_COLUMNS, fill_value='')
        except Exception as e:
            self.logger.error(f"❌ 读取 {Path(path).name} 失败: {e}")
            return pd.DataFrame(columns=ACCOUNT_EVENT_COLUMNS)

    def _write(self, path: Path, df: pd.DataFrame) -> bool:
        try:
            df.to_csv(path, index=False, encoding='utf-8-sig')
            return True
        except PermissionError:
            for _ in range(5):
                time_module.sleep(0.2)
                try:
                    df.to_csv(path, index=False, encoding='utf-8-sig')
                    return True
                except PermissionError:
                    continue
            self.logger.error(f"❌ 写入 {Path(path).name} 失败: 文件持续被占用")
            return False
        except Exception as e:
            self.logger.error(f"❌ 写入 {Path(path).name} 失败: {e}")
            return False

    def _append_rows(self, path: Path, rows: List[dict]) -> bool:
        df_new = pd.DataFrame(rows, columns=ACCOUNT_EVENT_COLUMNS)
        if not Path(path).exists():
            return self._write(path, df_new)
        df_old = self._read(path)
        df_all = pd.concat([df_old, df_new], ignore_index=True)
        return self._write(path, df_all)

    # ------------------------------------------------------------------
    # 初始化
    # ------------------------------------------------------------------
    def init_summary_files(self) -> None:
        """启动时初始化两个汇总文件（表头空表），避免无事件期间文件不存在"""
        empty = pd.DataFrame(columns=ACCOUNT_SUMMARY_COLUMNS)
        for account in ('account1', 'account2'):
            self._write(self.summary_paths[account], empty)
        self.logger.debug("📁 交易数据存储汇总文件已初始化 (sell.csv / buy.csv)")

    # ------------------------------------------------------------------
    # 开仓 / 加仓
    # ------------------------------------------------------------------
    def append_open(self, account: str, symbol: str,
                    price: float, volume: int,
                    exchange: str = '', industry: str = '',
                    event_datetime=None,
                    strategy: str = '', reason: str = '',
                    commission: Optional[float] = None) -> Optional[str]:
        """写开仓/加仓事件，返回 lot_id（写盘失败返回 None）

        - 该股票当前存在未平完批次（remaining>0）→ ADD；
          否则（含首次开仓、全部清仓后重新开仓）→ OPEN；
        - 开仓行 commission = 本批次单向佣金估算（commission=None 时自动估算）或
          传入的实际值；仅审计参考，不计入汇总合计。
        """
        if account not in self.account_dirs:
            self.logger.error(f"❌ 未知账户: {account}")
            return None
        try:
            price = float(price)
            volume = int(volume)
        except Exception:
            return None
        if volume <= 0 or price <= 0:
            return None

        action = 'sell' if account == 'account1' else 'buy'
        dt = event_datetime if event_datetime is not None else datetime.datetime.now()
        dt_str = _fmt_dt(dt)
        date_str = dt_str[:10].replace('-', '')

        path = self.stock_csv(account, symbol)
        with self._lock_for(f"{account}:{symbol}"):
            df = self._read(path)
            # 判定依据：是否存在未平完批次（而非"是否有过开仓行"）
            event_type = ('ADD' if any(v > 0 for v in self._compute_lot_remaining(df).values())
                          else 'OPEN')
            lot_id = self._new_lot_id(account, symbol, date_str)
            event_id = self._new_event_id(account, symbol, date_str)
            try:
                comm = float(commission)
            except Exception:
                comm = 0.0
            if comm <= 0:
                comm = volume * COMMISSION_PER_SHARE
            row = {
                'event_id': event_id,
                'account': account,
                'code': symbol,
                'exchange': exchange or '',
                'industry': industry or '',
                'event_type': event_type,
                'action': action,
                'event_datetime': dt_str,
                'price': f"{price:.4f}",
                'volume': str(volume),
                'amount': f"{price * volume:.2f}",
                'lot_id': lot_id,
                'related_lot_id': '',
                'allocated_vol': '',
                'remaining_vol_after': '',
                'gross_profit': '',
                'commission': f"{comm:.4f}",
                'profit': '',
                'strategy': strategy or '',
                'reason': reason or '',
            }
            wrote = self._append_rows(path, [row])

        if not wrote:
            self.logger.critical(f"🚨 {symbol}: 开仓事件写盘失败（{event_type} {volume}股）—— 账本缺口，请人工登记")
            return None
        self.rebuild_summary(account)
        return lot_id

    # ------------------------------------------------------------------
    # 差额幂等补记（P0 地基：账本完整性统一口径）
    # ------------------------------------------------------------------
    def ensure_cover(self, account: str, symbol: str, target_vol: int,
                     price: float, exchange: str = '', industry: str = '',
                     event_datetime=None, strategy: str = 'reconcile',
                     reason: str = '') -> int:
        """差额幂等补记：保证该标的账上未平批次 ≥ target_vol，只补「实际 − 已入账」差额

        统一口径（2026-09-21 事故修正）：判重必须是「数量差额」而非「是否有过开仓记录」——
        - 曾有 lot 但被超额平仓、账户里仍留着延迟成交持仓（KEEL 型）→ 差额仍需补；
        - 已入账覆盖 target（调平已补写过 / 阶段8已补记过）→ 差额为 0，**绝不新增 lot**；
        - 重复调用完全幂等（同一 (account, symbol, target_vol) 多次调用结果一致）。

        阶段8延迟补齐、终态补平、收敛循环收尾全部经由本方法补记，杜绝多路径各记一遍。

        Returns:
            本次实际补记的股数（无差额可补 → 0）
        """
        try:
            target_vol = int(target_vol)
            price = float(price)
        except Exception:
            return 0
        if account not in self.account_dirs or target_vol <= 0 or price <= 0:
            return 0
        booked = sum(int(l['remaining']) for l in self.get_open_lots(account, symbol))
        gap = max(0, target_vol - booked)
        if gap <= 0:
            return 0
        self.append_open(
            account=account, symbol=symbol, price=price, volume=gap,
            exchange=exchange, industry=industry,
            event_datetime=event_datetime,
            strategy=strategy, reason=reason or '差额幂等补记'
        )
        return gap

    # ------------------------------------------------------------------
    # 减仓 / 平仓
    # ------------------------------------------------------------------
    def append_close(self, account: str, symbol: str,
                     close_action: str, volume: int, price: float,
                     event_datetime=None,
                     target_lot_id: Optional[str] = None,
                     strategy: str = '', reason: str = '',
                     force: bool = False, reconcile: bool = False,
                     commission_per_share: float = COMMISSION_PER_SHARE,
                     actual_commission: Optional[float] = None) -> bool:
        """写减仓/平仓事件

        - 未指定 target_lot_id → FIFO 从最早未平完批次扣减；
          指定后该批次排最前（不足部分自动溢出到其余批次）；
        - 一笔平仓跨多批次 → 拆多行，每行关联一个批次；
        - event_type：reconcile=True → RECONCILE；force=True → FORCE_CLOSE；
          否则批次被平完记 CLOSE、未平完记 REDUCE；
        - commission：actual_commission（整单实际佣金）按分配量比例拆分到每行；
          未提供时按 分配量 × commission_per_share × 2 估算双向佣金；
        - 平仓量超过全部未平批次时，只分配可平部分并 WARNING（账实差告警由调用方补做）。
        """
        if account not in self.account_dirs:
            self.logger.error(f"❌ 未知账户: {account}")
            return False
        try:
            volume = int(volume)
            price = float(price)
        except Exception:
            return False
        if volume <= 0 or price <= 0:
            return False

        dt = event_datetime if event_datetime is not None else datetime.datetime.now()
        dt_str = _fmt_dt(dt)
        date_str = dt_str[:10].replace('-', '')
        path = self.stock_csv(account, symbol)

        with self._lock_for(f"{account}:{symbol}"):
            df = self._read(path)
            if df.empty:
                self.logger.error(f"❌ {symbol}: 无开仓记录，无法平仓")
                return False

            lot_remaining = self._compute_lot_remaining(df)
            open_lots: List[Tuple[str, int]] = [
                (lid, rem) for lid, rem in lot_remaining.items() if rem > 0
            ]
            if not open_lots:
                self.logger.error(f"❌ {symbol}: 无未平仓批次，无法平仓")
                return False

            if target_lot_id:
                target = [x for x in open_lots if x[0] == target_lot_id]
                others = [x for x in open_lots if x[0] != target_lot_id]
                open_lots = target + others
                if not target:
                    self.logger.warning(
                        f"⚠️ {symbol}: 指定批次 {target_lot_id} 不存在或已平完，按 FIFO 处理"
                    )

            remaining_to_close = volume
            rows: List[dict] = []
            for lot_id, lot_rem in open_lots:
                if remaining_to_close <= 0:
                    break
                alloc = min(remaining_to_close, lot_rem)
                remaining_after = lot_rem - alloc

                if reconcile:
                    event_type = 'RECONCILE'
                elif force:
                    event_type = 'FORCE_CLOSE'
                else:
                    event_type = 'CLOSE' if remaining_after == 0 else 'REDUCE'

                entry_price = self._lot_entry_price(df, lot_id)
                gross_profit = self._gross_profit_for(
                    account, close_action, entry_price, price, alloc
                )

                # 佣金：实际佣金按分配比例拆分优先；否则双向估算
                try:
                    ac = float(actual_commission) if actual_commission is not None else 0.0
                except Exception:
                    ac = 0.0
                if ac > 0 and volume > 0:
                    comm = ac * alloc / volume
                else:
                    comm = alloc * commission_per_share * 2
                profit = gross_profit - comm

                event_id = self._new_event_id(account, symbol, date_str)
                row = {
                    'event_id': event_id,
                    'account': account,
                    'code': symbol,
                    'exchange': '',   # 平仓行可留空，汇总从开仓行取
                    'industry': '',
                    'event_type': event_type,
                    'action': close_action,
                    'event_datetime': dt_str,
                    'price': f"{price:.4f}",
                    'volume': str(alloc),
                    'amount': f"{price * alloc:.2f}",
                    'lot_id': '',
                    'related_lot_id': lot_id,
                    'allocated_vol': str(alloc),
                    'remaining_vol_after': str(remaining_after),
                    'gross_profit': f"{gross_profit:.2f}",
                    'commission': f"{comm:.4f}",
                    'profit': f"{profit:.2f}",
                    'strategy': strategy or '',
                    'reason': reason or '',
                }
                rows.append(row)
                remaining_to_close -= alloc

            if remaining_to_close > 0:
                self.logger.warning(
                    f"⚠️ {symbol}: 可平数量不足（请求 {volume}，实际分配 {volume - remaining_to_close}，"
                    f"超出 {remaining_to_close} 股 —— 疑似延迟开仓成交或账本缺口，请人工核对）"
                )

            if not rows:
                return False
            wrote = self._append_rows(path, rows)

        if not wrote:
            self.logger.critical(f"🚨 {symbol}: 平仓事件写盘失败（{len(rows)} 行）—— 账本缺口，请人工登记")
            return False
        self.rebuild_summary(account)
        return True

    # ------------------------------------------------------------------
    # 辅助：批次剩余 / 开仓价 / 盈亏
    # ------------------------------------------------------------------
    def _compute_lot_remaining(self, df: pd.DataFrame) -> Dict[str, int]:
        """从事件流计算每个 lot 的剩余数量（扣减不出现负数）"""
        result: Dict[str, int] = {}
        if df.empty:
            return result
        for _, row in df.iterrows():
            et = str(row.get('event_type') or '')
            if et in OPEN_TYPES:
                lid = str(row.get('lot_id') or '').strip()
                if not lid:
                    continue
                result[lid] = result.get(lid, 0) + int(_to_num(row.get('volume')))
            elif et in CLOSE_TYPES:
                lid = str(row.get('related_lot_id') or '').strip()
                if not lid:
                    continue
                alloc = int(_to_num(row.get('allocated_vol') or row.get('volume')))
                if lid in result:
                    result[lid] = max(0, result[lid] - alloc)
        return result

    def _lot_entry_price(self, df: pd.DataFrame, lot_id: str) -> float:
        if lot_id not in set(df['lot_id'].tolist()):
            return 0.0
        mask = (df['lot_id'] == lot_id) & (df['event_type'].isin(OPEN_TYPES))
        hit = df[mask]
        if hit.empty:
            return 0.0
        return _to_num(hit.iloc[0]['price'])

    @staticmethod
    def _gross_profit_for(account: str, close_action: str,
                          entry_price: float, close_price: float,
                          volume: int) -> float:
        """本行分配的毛盈亏。
        account1: 开仓 sell / 平仓 buy → 毛盈亏 = 开仓金额 - 平仓金额
        account2: 开仓 buy / 平仓 sell → 毛盈亏 = 平仓金额 - 开仓金额
        """
        open_fund = entry_price * volume
        close_fund = close_price * volume
        if account == 'account1':
            return open_fund - close_fund
        return close_fund - open_fund

    # ------------------------------------------------------------------
    # 日汇总重建
    # ------------------------------------------------------------------
    def rebuild_summary(self, account: str) -> bool:
        """遍历该账户事件流，按股票聚合，重建 sell.csv / buy.csv（幂等）"""
        if account not in self.account_dirs:
            return False
        account_dir = self.account_dirs[account]
        summary_path = self.summary_paths[account]

        rows: List[dict] = []
        for stock_csv in sorted(account_dir.glob('*.csv')):
            try:
                df = self._read(stock_csv)
                if df.empty:
                    continue
                summary = self._aggregate_one_stock(account, df)
                if summary:
                    rows.append(summary)
            except Exception as e:
                self.logger.error(f"❌ 汇总 {stock_csv.name} 失败: {e}")

        df_summary = pd.DataFrame(rows, columns=ACCOUNT_SUMMARY_COLUMNS)
        return self._write(summary_path, df_summary)

    @staticmethod
    def _num_sum(sub: pd.DataFrame, col: str) -> float:
        try:
            if sub is None or sub.empty:
                return 0.0
            s = sub[col]
            return float(pd.to_numeric(s.replace('', pd.NA), errors='coerce').fillna(0.0).sum())
        except Exception:
            return 0.0

    def _aggregate_one_stock(self, account: str, df: pd.DataFrame) -> Optional[dict]:
        """按股票聚合全天事件（汇总 commission/profit 只取平仓行，防双重计算）"""
        if df.empty:
            return None

        symbol = str(df.iloc[0]['code'])
        exchange = ''
        industry = ''
        open_rows = df[df['event_type'].isin(OPEN_TYPES)]
        if not open_rows.empty:
            exchange = str(open_rows.iloc[0].get('exchange') or '')
            industry = str(open_rows.iloc[0].get('industry') or '')

        close_rows = df[df['event_type'].isin(CLOSE_TYPES)]

        open_vol = self._num_sum(open_rows, 'volume')
        close_vol = self._num_sum(close_rows, 'volume')
        open_fund = self._num_sum(open_rows, 'amount')
        close_fund = self._num_sum(close_rows, 'amount')

        # 汇总盈亏/佣金只累加平仓行（平仓行已含本行分配量的双向佣金；
        # 开仓行的单向参考佣金不得累加，否则双重计算）
        gross_profit = self._num_sum(close_rows, 'gross_profit')
        commission = self._num_sum(close_rows, 'commission')
        profit = self._num_sum(close_rows, 'profit')

        lot_count = len({str(x).strip() for x in open_rows['lot_id'].tolist()
                         if str(x).strip()})
        event_count = len(df)

        times = [str(t) for t in df['event_datetime'].tolist() if str(t).strip()]
        first_time = min(times) if times else ''
        last_time = max(times) if times else ''

        remaining_vol = int(open_vol - close_vol)
        if remaining_vol <= 0:
            status = 'closed'
        elif close_vol > 0:
            status = 'partial'
        else:
            status = 'open'

        action = 'sell' if account == 'account1' else 'buy'
        date_str = times[0][:10] if times else ''

        return {
            'date': date_str,
            'account': account,
            'code': symbol,
            'exchange': exchange,
            'industry': industry,
            'action': action,
            'lot_count': lot_count,
            'event_count': event_count,
            'open_vol': int(open_vol),
            'close_vol': int(close_vol),
            'remaining_vol': remaining_vol,
            'open_fund': round(open_fund, 2),
            'close_fund': round(close_fund, 2),
            'gross_profit': round(gross_profit, 2),
            'commission': round(commission, 4),
            'profit': round(profit, 2),
            'first_event_time': first_time,
            'last_event_time': last_time,
            'status': status,
        }

    # ------------------------------------------------------------------
    # 查询接口（供策略 / 对账 / 审计）
    # ------------------------------------------------------------------
    def get_open_lots(self, account: str, symbol: str) -> List[dict]:
        """返回该股票当前所有未平完批次（按开仓时间升序）

        Returns:
            [{'lot_id': ..., 'entry_price': ..., 'remaining': ..., 'event_datetime': ...}, ...]
        """
        if account not in self.account_dirs:
            return []
        df = self._read(self.stock_csv(account, symbol))
        if df.empty:
            return []
        lot_remaining = self._compute_lot_remaining(df)
        result: List[dict] = []
        for lot_id, rem in lot_remaining.items():
            if rem <= 0:
                continue
            entry_price = self._lot_entry_price(df, lot_id)
            hit = df[(df['lot_id'] == lot_id) & (df['event_type'].isin(OPEN_TYPES))]
            entry_dt = str(hit.iloc[0]['event_datetime']) if not hit.empty else ''
            result.append({
                'lot_id': lot_id,
                'entry_price': entry_price,
                'remaining': int(rem),
                'event_datetime': entry_dt,
            })
        result.sort(key=lambda x: (x['event_datetime'], x['lot_id']))
        return result

    def open_codes(self, account: str) -> Set[str]:
        """该账户当前仍有未平完批次的股票代码集合（用于阶段8开仓对账）"""
        if account not in self.account_dirs:
            return set()
        codes: Set[str] = set()
        account_dir = self.account_dirs[account]
        if not Path(account_dir).exists():
            return codes
        for f in sorted(Path(account_dir).glob('*.csv')):
            try:
                df = self._read(f)
                if df.empty:
                    continue
                if any(v > 0 for v in self._compute_lot_remaining(df).values()):
                    codes.add(str(df.iloc[0]['code']))
            except Exception:
                continue
        return codes

    def has_open_events(self, account: str, symbol: str) -> bool:
        """该股票是否曾有任何开仓事件（OPEN/ADD）——调补缺口判重"""
        if account not in self.account_dirs:
            return False
        df = self._read(self.stock_csv(account, symbol))
        if df.empty:
            return False
        return bool((df['event_type'].isin(OPEN_TYPES) & (df['lot_id'] != '')).any())
