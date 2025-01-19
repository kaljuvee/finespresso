import pandas as pd
import numpy as np
from datetime import datetime
from utils.backtest_price_util import create_price_moves
from utils.logging.log_util import get_logger

logger = get_logger(__name__)

def calculate_position_size(capital, position_size_pct):
    return capital * position_size_pct

def calculate_shares(position_size, entry_price):
    return int(position_size / entry_price)

def run_backtest(news_df, initial_capital, position_size, take_profit, stop_loss, enable_advanced=False):
    logger.info("Starting backtest")
    
    if news_df.empty:
        logger.warning("No news data provided for backtest")
        return None
    
    # Create price moves for the news events
    price_moves_df = create_price_moves(news_df)
    
    if price_moves_df.empty:
        logger.warning("No price moves generated")
        return None
    
    # Initialize trade tracking
    trades = []
    current_capital = initial_capital
    
    # Sort by published date
    price_moves_df = price_moves_df.sort_values('published_date')
    
    for _, row in price_moves_df.iterrows():
        try:
            # Calculate position size for this trade
            trade_position_size = calculate_position_size(current_capital, position_size)
            
            # Entry price is the price at news publication
            entry_price = row['begin_price']
            shares = calculate_shares(trade_position_size, entry_price)
            
            if shares == 0:
                continue
                
            # Determine trade direction
            is_long = row['predicted_side'] == 'UP'
            
            # Calculate target and stop prices
            if is_long:
                target_price = entry_price * (1 + take_profit)
                stop_price = entry_price * (1 - stop_loss)
            else:
                target_price = entry_price * (1 - take_profit)
                stop_price = entry_price * (1 + stop_loss)
            
            # Calculate actual P&L
            exit_price = row['end_price']
            pnl = shares * (exit_price - entry_price) if is_long else shares * (entry_price - exit_price)
            pnl_pct = (pnl / trade_position_size) * 100
            
            # Update capital
            current_capital += pnl
            
            # Record trade
            trade = {
                'entry_time': row['published_date'],
                'exit_time': row['published_date'] + pd.Timedelta(days=1),
                'ticker': row['ticker'],
                'direction': 'LONG' if is_long else 'SHORT',
                'shares': shares,
                'entry_price': entry_price,
                'exit_price': exit_price,
                'target_price': target_price,
                'stop_price': stop_price,
                'pnl': pnl,
                'pnl_pct': pnl_pct,
                'capital_after': current_capital,
                'news_event': row['event'],
                'predicted_move': row['predicted_move'],
                'actual_move': row['price_change_percentage'],
                'link': row['link']
            }
            trades.append(trade)
            
        except Exception as e:
            logger.error(f"Error processing trade for {row['ticker']}: {e}")
            continue
    
    if not trades:
        logger.warning("No trades generated during backtest")
        return None
    
    # Create trades DataFrame
    trades_df = pd.DataFrame(trades)
    
    # Calculate metrics
    metrics = calculate_metrics(trades_df, initial_capital)
    
    return trades_df, metrics

def calculate_metrics(trades_df, initial_capital):
    """Calculate backtest performance metrics"""
    total_trades = len(trades_df)
    winning_trades = len(trades_df[trades_df['pnl'] > 0])
    
    # Calculate total PnL in dollars
    total_pnl = trades_df['pnl'].sum()
    
    # Calculate total return (as percentage)
    total_return = (total_pnl / initial_capital) * 100
    
    # Calculate annualized return
    if not trades_df.empty:
        period_begin = pd.to_datetime(trades_df['entry_time'].min())
        period_end = pd.to_datetime(trades_df['exit_time'].max())
        days_diff = (period_end - period_begin).days
        
        if days_diff > 0:
            annualized_return = total_return * 365 / days_diff
        else:
            annualized_return = total_return  # If same day, use total return
    else:
        annualized_return = 0
    
    metrics = {
        'total_trades': total_trades,
        'winning_trades': winning_trades,
        'win_rate': (winning_trades / total_trades * 100) if total_trades > 0 else 0,
        'total_pnl': total_pnl,
        'total_return': total_return,
        'annualized_return': annualized_return
    }
    
    return metrics 