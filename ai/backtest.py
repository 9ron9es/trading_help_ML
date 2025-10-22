import random

class Backtester:
    def __init__(self, slippage_pct=0.0005, commission_per_trade=1.0):
        """
        slippage_pct: fraction of price (e.g., 0.0005 = 5 bps)
        commission_per_trade: flat fee per executed side
        """
        self.slippage_pct = slippage_pct
        self.commission = commission_per_trade

    def apply_slippage(self, price, side='buy'):
        """Apply symmetric slippage model with small randomness."""
        sign = 1 if side.lower() == 'buy' else -1
        slippage = abs(price) * (self.slippage_pct * (1 + random.uniform(-0.5, 0.5)))
        return price + sign * slippage

    def simulate_fill(self, desired_qty, available_qty):
        """Simple partial fill: return executed quantity."""
        if available_qty <= 0:
            return 0
        if desired_qty <= available_qty:
            return desired_qty
        
        return int(available_qty * 0.9)

    def simulate_trade(self, entry_price, side, qty, available_liquidity=None):
        """Return executed_price, executed_qty, fees"""
        executed_qty = qty
        if available_liquidity is not None:
            executed_qty = self.simulate_fill(qty, available_liquidity)
        exec_price = self.apply_slippage(entry_price, side)
        fees = self.commission
        return exec_price, executed_qty, fees