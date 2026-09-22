# -*- coding: utf-8 -*-
"""
回归测试：交易数据存储（事件流明细 + 日汇总，支持多次加仓/部分减仓）

覆盖矩阵：
  S1 一开一平（空腿）：OPEN+CLOSE，汇总 lot_count/open_vol/close_vol/remaining/status
  S2 一开一平（多腿）：盈亏方向相反仍为正
  S3 多次加仓 + 多次部分减仓：ADD 判定 / REDUCE / CLOSE 混用
  S4 一次减仓跨多批次：拆 2 行、行序 FIFO
  S5 全平后重新开仓 → 记 OPEN（而非 ADD）
  S6 target_lot_id 指定批次优先扣减
  S7 平仓量 > 未平批次：只分配可平部分 + 返回 True + 汇总剩余正确
  S8 汇总佣金只累加平仓行（防开仓行参考佣金双重计算）
  S9 actual_commission 按分配量比例拆分到各行
  S10 事件/批次 ID 格式与唯一性
  S11 open_codes / has_open_events / get_open_lots 查询语义
  S12 reconcile=True → event_type=RECONCILE；force=True → FORCE_CLOSE
  S13 旧策略一开一平兼容：全量平仓 → status=closed、剩余 0、盈亏与旧口径一致

运行：python test/test_trade_store.py   （AutoTrader 根目录下执行；任意 cwd 均可）
"""
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent  # AutoTrader 包目录
sys.path.insert(0, str(HERE))

import pandas as pd

from trade_store import TradeStore, COMMISSION_PER_SHARE

# Windows 控制台默认代码页（如 cp1252）无法输出中文/emoji，统一 UTF-8，避免测试进程崩溃
try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass

TMP = Path(tempfile.mkdtemp(prefix='trade_store_test_'))


def read_events(store, account, symbol):
    p = store.stock_csv(account, symbol)
    assert p.exists(), f"事件文件不存在: {p}"
    return pd.read_csv(p, dtype=str, keep_default_na=False)


def read_summary(store, account):
    p = store.summary_csv(account)
    assert p.exists(), f"汇总文件不存在: {p}"
    return pd.read_csv(p, dtype=str, keep_default_na=False)


def main():
    failures = []

    def check(name, ok, detail=''):
        print(f'[{"ok" if ok else "FAIL"}] {name}{(": " + str(detail)) if detail else ""}')
        if not ok:
            failures.append(name)

    # ==================== S1 一开一平（空腿 account1） ====================
    s1 = TradeStore(TMP / 's1')
    s1.init_summary_files()
    s1.append_open('account1', 'AAA', price=100.0, volume=10,
                   exchange='NASDAQ', industry='Tech',
                   strategy='hedge', reason='建空仓')
    ok_close = s1.append_close('account1', 'AAA', close_action='buy',
                               volume=10, price=99.0,
                               strategy='dynamic_tp', reason='触发')
    ev = read_events(s1, 'account1', 'AAA')
    su = read_summary(s1, 'account1')
    row = su[su['code'] == 'AAA'].iloc[0]
    gross = 10 * (100.0 - 99.0)                      # 空腿：开仓金额 - 平仓金额
    comm = 10 * COMMISSION_PER_SHARE * 2             # 双向估算
    ok1 = (ok_close
           and len(ev) == 2
           and ev.iloc[0]['event_type'] == 'OPEN'
           and ev.iloc[1]['event_type'] == 'CLOSE'
           and ev.iloc[1]['related_lot_id'] == ev.iloc[0]['lot_id']
           and int(row['open_vol']) == 10 and int(row['close_vol']) == 10
           and int(row['remaining_vol']) == 0
           and row['status'] == 'closed'
           and int(row['lot_count']) == 1
           and abs(float(row['gross_profit']) - gross) < 1e-6
           and abs(float(row['commission']) - comm) < 1e-9
           and abs(float(row['profit']) - (gross - comm)) < 1e-6)
    check('S1 一开一平(空腿): 事件/汇总/盈亏正确', ok1,
          f'rows={len(ev)} status={row["status"]} profit={row["profit"]}')

    # ==================== S2 一开一平（多腿 account2，盈亏反向） ====================
    s2 = TradeStore(TMP / 's2')
    s2.append_open('account2', 'BBB', price=50.0, volume=20,
                   exchange='NYSE', industry='Fin', strategy='hedge', reason='建多仓')
    s2.append_close('account2', 'BBB', close_action='sell',
                    volume=20, price=49.0, strategy='dynamic_tp', reason='触发')
    su2 = read_summary(s2, 'account2')
    row2 = su2[su2['code'] == 'BBB'].iloc[0]
    gross2 = 20 * (49.0 - 50.0)                       # 多腿：平仓-开仓 = 亏损
    ok2 = (abs(float(row2['gross_profit']) - gross2) < 1e-6
           and float(row2['profit']) < 0
           and row2['status'] == 'closed')
    check('S2 一开一平(多腿): 盈亏方向正确(亏损为负)', ok2,
          f'gross={row2["gross_profit"]} profit={row2["profit"]}')

    # ==================== S3 多次加仓 + 多次部分减仓 ====================
    s3 = TradeStore(TMP / 's3')
    s3.append_open('account1', 'CCC', price=10.0, volume=56, strategy='hedge', reason='开仓1')
    s3.append_open('account1', 'CCC', price=11.0, volume=50, strategy='hedge', reason='加仓')
    ev3 = read_events(s3, 'account1', 'CCC')
    ev3_types = ev3['event_type'].tolist()
    s3.append_close('account1', 'CCC', close_action='buy', volume=30, price=10.5,
                    strategy='dynamic_tp', reason='部分减仓')
    ev3b = read_events(s3, 'account1', 'CCC')
    r3 = ev3b.iloc[-1]
    lot1 = ev3.iloc[0]['lot_id']
    # 30 股全部落在 lot1（56→26），REDUCE
    s3.append_close('account1', 'CCC', close_action='buy', volume=40, price=10.8,
                    strategy='dynamic_tp', reason='平仓2')
    ev3c = read_events(s3, 'account1', 'CCC')
    # 40 = lot1 剩余 26(CLOSE) + lot2 14(REDUCE)，共 2 行
    ok3 = (ev3_types == ['OPEN', 'ADD']
           and r3['event_type'] == 'REDUCE'
           and r3['related_lot_id'] == lot1
           and r3['allocated_vol'] == '30'
           and r3['remaining_vol_after'] == '26'
           and ev3c['event_type'].tolist()[-2:] == ['CLOSE', 'REDUCE']
           and ev3c['volume'].tolist()[-2:] == ['26', '14'])
    su3 = read_summary(s3, 'account1')
    row3 = su3[su3['code'] == 'CCC'].iloc[0]
    ok3 = ok3 and (int(row3['lot_count']) == 2
                   and int(row3['open_vol']) == 106
                   and int(row3['close_vol']) == 70
                   and int(row3['remaining_vol']) == 36
                   and row3['status'] == 'partial')
    check('S3 多次加仓+部分减仓: ADD/REDUCE/CLOSE 判定与汇总正确', ok3,
          f'events={ev3_types}+... remaining={row3["remaining_vol"]} status={row3["status"]}')

    # ==================== S4 一次减仓跨多批次（拆行） ====================
    s4 = TradeStore(TMP / 's4')
    s4.append_open('account2', 'DDD', price=100.0, volume=30, strategy='hedge', reason='开仓')
    s4.append_open('account2', 'DDD', price=110.0, volume=50, strategy='hedge', reason='加仓')
    s4.append_close('account2', 'DDD', close_action='sell', volume=40, price=105.0,
                    strategy='dynamic_tp', reason='跨批平仓')
    ev4 = read_events(s4, 'account2', 'DDD')
    close_rows = ev4[ev4['event_type'].isin(['REDUCE', 'CLOSE', 'FORCE_CLOSE', 'RECONCILE'])]
    ok4 = (len(close_rows) == 2
           and close_rows.iloc[0]['volume'] == '30'
           and close_rows.iloc[0]['related_lot_id'] == ev4.iloc[0]['lot_id']
           and close_rows.iloc[0]['event_type'] == 'CLOSE'
           and close_rows.iloc[1]['volume'] == '10'
           and close_rows.iloc[1]['related_lot_id'] == ev4.iloc[1]['lot_id']
           and close_rows.iloc[1]['event_type'] == 'REDUCE')
    check('S4 一次平仓跨两批次: FIFO 拆 2 行', ok4)

    # ==================== S5 全平后重新开仓 → OPEN（而非 ADD） ====================
    s5 = TradeStore(TMP / 's5')
    s5.append_open('account1', 'EEE', price=8.0, volume=10, strategy='hedge', reason='开仓')
    s5.append_close('account1', 'EEE', close_action='buy', volume=10, price=8.5,
                    strategy='dynamic_tp', reason='全平')
    s5.append_open('account1', 'EEE', price=9.0, volume=4, strategy='hedge', reason='重新开仓')
    ev5 = read_events(s5, 'account1', 'EEE')
    third = ev5.iloc[-1]
    open_ids = set(ev5.iloc[:-1].loc[ev5['event_type'].isin(['OPEN', 'ADD']), 'lot_id'])
    ok5 = (third['event_type'] == 'OPEN'
           and third['lot_id'] not in open_ids)
    su5 = read_summary(s5, 'account1')
    row5 = su5[su5['code'] == 'EEE'].iloc[0]
    ok5 = ok5 and (int(row5['lot_count']) == 2 and int(row5['remaining_vol']) == 4
                   and row5['status'] == 'partial')  # 已有部分平仓量 → partial
    check('S5 全平后重新开仓记 OPEN 且新批次计入汇总', ok5)

    # ==================== S6 target_lot_id 指定批次优先 ====================
    s6 = TradeStore(TMP / 's6')
    lotA = s6.append_open('account1', 'FFF', price=10.0, volume=10, strategy='hedge', reason='A')
    lotB = s6.append_open('account1', 'FFF', price=12.0, volume=10, strategy='hedge', reason='B')
    assert s6.append_close('account1', 'FFF', close_action='buy', volume=6, price=11.0,
                           target_lot_id=lotB, strategy='dynamic_tp', reason='指定批次')
    ev6 = read_events(s6, 'account1', 'FFF')
    last = ev6.iloc[-1]
    ok6 = (last['related_lot_id'] == lotB
           and last['remaining_vol_after'] == '4')
    lots6 = s6.get_open_lots('account1', 'FFF')
    ok6 = ok6 and (len(lots6) == 2
                   and {l['lot_id']: l['remaining'] for l in lots6} == {lotA: 10, lotB: 4})
    check('S6 target_lot_id 指定批次优先扣减', ok6)

    # ==================== S7 平仓量 > 未平批次（不报错、只分配可平部分） ====================
    s7 = TradeStore(TMP / 's7')
    s7.append_open('account1', 'GGG', price=10.0, volume=10, strategy='hedge', reason='开仓')
    ok = s7.append_close('account1', 'GGG', close_action='buy', volume=15, price=9.0,
                         strategy='dynamic_tp', reason='超额平仓')
    ev7 = read_events(s7, 'account1', 'GGG')
    last7 = ev7.iloc[-1]
    su7 = read_summary(s7, 'account1')
    row7 = su7[su7['code'] == 'GGG'].iloc[0]
    ok7 = (ok is True
           and last7['volume'] == '10'
           and int(row7['close_vol']) == 10
           and int(row7['remaining_vol']) == 0
           and row7['status'] == 'closed')
    check('S7 超额平仓: 只记未平批次部分并告警(不炸)', ok7)

    # ==================== S8 汇总佣金只累加平仓行 ====================
    s8 = TradeStore(TMP / 's8')
    s8.append_open('account1', 'HHH', price=100.0, volume=10, strategy='hedge', reason='开仓')
    # 开仓行参考佣金 = 10 * 0.0035（单向）= 0.035
    s8.append_close('account1', 'HHH', close_action='buy', volume=10, price=100.0,
                    strategy='dynamic_tp', reason='平价平仓')
    ev8 = read_events(s8, 'account1', 'HHH')
    open_comm = float(ev8.iloc[0]['commission'])
    close_comm = float(ev8.iloc[1]['commission'])
    su8 = read_summary(s8, 'account1')
    sum_comm = float(su8[su8['code'] == 'HHH'].iloc[0]['commission'])
    ok8 = (abs(open_comm - 10 * COMMISSION_PER_SHARE) < 1e-9           # 开行=单向
           and abs(close_comm - 10 * COMMISSION_PER_SHARE * 2) < 1e-9  # 平=双向
           and abs(sum_comm - close_comm) < 1e-9)                      # 汇总=仅平仓行
    check('S8 佣金口径: 开行单向/平=双向/汇总只累加平仓行(不双重计算)', ok8,
          f'open_comm={open_comm} close_comm={close_comm} sum={sum_comm}')

    # ==================== S9 actual_commission 按比例拆分 ====================
    s9 = TradeStore(TMP / 's9')
    s9.append_open('account1', 'III', price=10.0, volume=30, strategy='hedge', reason='开仓1')
    s9.append_open('account1', 'III', price=10.0, volume=10, strategy='hedge', reason='开仓2')
    s9.append_close('account1', 'III', close_action='buy', volume=40, price=11.0,
                    strategy='dynamic_tp', reason='全平', actual_commission=1.00)
    ev9 = read_events(s9, 'account1', 'III')
    cr = ev9[ev9['event_type'].isin(['REDUCE', 'CLOSE', 'FORCE_CLOSE', 'RECONCILE'])]
    c1, c2 = float(cr.iloc[0]['commission']), float(cr.iloc[1]['commission'])
    ok9 = (abs(c1 - 0.75) < 1e-9 and abs(c2 - 0.25) < 1e-9 and abs(c1 + c2 - 1.00) < 1e-9)
    check('S9 实际佣金按分配量比例拆分(30:10 → 0.75/0.25)', ok9, f'{c1} / {c2}')

    # ==================== S10 事件/批次 ID 格式与唯一性 ====================
    s10 = TradeStore(TMP / 's10')
    l1 = s10.append_open('account1', 'JJJ', price=5.0, volume=2, strategy='hedge', reason='a')
    l2 = s10.append_open('account1', 'JJJ', price=5.5, volume=3, strategy='hedge', reason='b')
    s10.append_close('account1', 'JJJ', close_action='buy', volume=5, price=6.0,
                     strategy='dynamic_tp', reason='c')
    ev10 = read_events(s10, 'account1', 'JJJ')
    ids = ev10['event_id'].tolist() + [l1, l2]
    ok10 = (len(set(ids)) == len(ids)
            and all('-L' in x for x in (l1, l2))
            and all('-' in x and x.count('-') >= 3 for x in ev10['event_id'].tolist()))
    check('S10 事件/批次 ID 唯一且含账户/标的/序号', ok10, ids)

    # ==================== S11 查询接口语义 ====================
    s11 = TradeStore(TMP / 's11')
    s11.append_open('account1', 'KKK', price=5.0, volume=10, strategy='hedge', reason='a')
    s11.append_open('account2', 'KKK', price=5.0, volume=10, strategy='hedge', reason='a')
    s11.append_close('account1', 'KKK', close_action='buy', volume=10, price=6.0,
                     strategy='dynamic_tp', reason='全平')
    ok11 = (s11.open_codes('account1') == set()          # 已全平 → 不在未平集合
            and s11.open_codes('account2') == {'KKK'}    # 未平 → 在集合
            and s11.has_open_events('account1', 'KKK')   # 有开仓事件
            and s11.has_open_events('account2', 'KKK')
            and not s11.has_open_events('account1', 'ZZZ')
            and s11.get_open_lots('account2', 'KKK')[0]['remaining'] == 10)
    check('S11 open_codes/has_open_events/get_open_lots 语义正确', ok11)

    # ==================== S12 事件类型：reconcile / force ====================
    s12 = TradeStore(TMP / 's12')
    s12.append_open('account2', 'LLL', price=10.0, volume=5, strategy='hedge', reason='a')
    s12.append_open('account2', 'LLL', price=10.0, volume=5, strategy='hedge', reason='b')
    s12.append_close('account2', 'LLL', close_action='sell', volume=3, price=11.0,
                     strategy='reconcile', reason='调平', reconcile=True)
    s12.append_close('account2', 'LLL', close_action='sell', volume=7, price=12.0,
                     strategy='force_close', reason='强平', force=True)
    ev12 = read_events(s12, 'account2', 'LLL')
    types = ev12['event_type'].tolist()
    ok12 = (types[2] == 'RECONCILE'
            and types[3] == 'FORCE_CLOSE' and types[4] == 'FORCE_CLOSE')
    check('S12 reconcile→RECONCILE / force→FORCE_CLOSE 事件类型', ok12, types)

    # ==================== S13 旧策略一开一平兼容（全量平 → closed） ====================
    s13 = TradeStore(TMP / 's13')
    # 做空建仓 44.0，42.5 买回 → 盈利（旧一开一平正收益场景）
    s13.append_open('account1', 'MMM', price=44.0, volume=23, strategy='hedge',
                    reason='盘前预挂单成交确认')
    s13.append_close('account1', 'MMM', close_action='buy', volume=23, price=42.5,
                     strategy='dynamic_tp', reason='触发平仓')
    su13 = read_summary(s13, 'account1')
    row13 = su13[su13['code'] == 'MMM'].iloc[0]
    gross13 = 23 * (44.0 - 42.5)
    comm13 = 23 * COMMISSION_PER_SHARE * 2
    # 汇总字段 round(2) 存储，断言容差放宽到 0.01
    ok13 = (row13['status'] == 'closed'
            and int(row13['remaining_vol']) == 0
            and abs(float(row13['gross_profit']) - gross13) < 0.01
            and abs(float(row13['profit']) - (gross13 - comm13)) < 0.01
            and int(row13['lot_count']) == 1
            and int(row13['event_count']) == 2
            and row13['action'] == 'sell'
            and row13['account'] == 'account1')
    check('S13 旧一开一平场景: 汇总语义完整（closed/剩余0/盈亏口径）', ok13,
          f'gross={row13["gross_profit"]} profit={row13["profit"]}')

    print()
    if failures:
        print('❌ 失败用例:', ', '.join(failures))
        sys.exit(1)
    print('✅ 全部事件流存储回归测试通过（加仓/部分减仓/跨批平仓/佣金口径/兼容旧语义）')


if __name__ == '__main__':
    main()
