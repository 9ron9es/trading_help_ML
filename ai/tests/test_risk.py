import os
import json
import pytest
from main_enhaced_3 import AdvancedRiskManager, ACCOUNT_BALANCE_START, ACCOUNT_STATE_FILE, save_account_state

def test_pre_trade_checks_daily_limit(tmp_path, monkeypatch):
    # ensure account state file exists with starting balance
    save_account_state(balance=ACCOUNT_BALANCE_START, total_trades=0, wins=0, total_pnl=0)
    rm = AdvancedRiskManager()
    rm.daily_pnl = -(ACCOUNT_BALANCE_START * 0.03)  # exceed daily 2% loss
    ok, reason = rm.pre_trade_checks('forex', 'EUR/USD', 'BUY', 1.1, 50, 1)
    assert not ok
    assert "Daily loss limit" in reason or "Maximum account drawdown" in reason

def test_consecutive_losses_block(tmp_path):
    save_account_state(balance=ACCOUNT_BALANCE_START, total_trades=0, wins=0, total_pnl=0)
    rm = AdvancedRiskManager()
    rm.consecutive_losses = 3
    ok, reason = rm.pre_trade_checks('stocks', 'AAPL', 'BUY', 150.0, 20, 1)
    assert not ok
    assert "Too many consecutive losses" in reason