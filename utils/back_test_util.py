import pandas as pd
import numpy as np
from datetime import datetime
from utils.backtest_price_util import create_price_moves, get_intraday_prices
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
    
    # Debug log the date range
    logger.info(f"Date range in news_df: {news_df['published_date'].min()} to {news_df['published_date'].max()}")
    
    # Ensure published_date is timezone-aware
    if not pd.api.types.is_datetime64tz_dtype(news_df['published_date']):
        logger.info("Converting published_date to timezone-aware")
        news_df['published_date'] = pd.to_datetime(news_df['published_date']).dt.tz_localize('UTC')
    
    # Debug log a few sample dates before filtering
    logger.info("Sample of published dates before filtering:")
    logger.info(news_df['published_date'].head())
    
    # Note: We don't need to filter the dates here since backtester.py already filters them
    
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
            
            # Get entry time and price
            entry_time = row['trade_time']
            
            # For regular market trades, check intraday prices for exit conditions
            if row['market'] == 'regular_market':
                intraday_data = get_intraday_prices(row['ticker'], entry_time.date())
                
                if not intraday_data.empty:
                    # Get prices after entry time
                    prices_after_entry = intraday_data[intraday_data.index >= entry_time]
                    
                    exit_price = row['end_price']  # default to end of day
                    exit_time = row['published_date'] + pd.Timedelta(days=1)
                    hit_target = False
                    hit_stop = False
                    
                    for idx, price_data in prices_after_entry.iterrows():
                        current_price = float(price_data['Close'])  # Convert to float
                        
                        if is_long:
                            if current_price >= target_price:
                                exit_price = target_price
                                exit_time = idx
                                hit_target = True
                                break
                            elif current_price <= stop_price:
                                exit_price = stop_price
                                exit_time = idx
                                hit_stop = True
                                break
                        else:  # short trade
                            if current_price <= target_price:
                                exit_price = target_price
                                exit_time = idx
                                hit_target = True
                                break
                            elif current_price >= stop_price:
                                exit_price = stop_price
                                exit_time = idx
                                hit_stop = True
                                break
                else:
                    # Fallback to simple exit price logic if no intraday data
                    end_price = float(row['end_price'])  # Convert to float
                    hit_target = end_price >= target_price if is_long else end_price <= target_price
                    hit_stop = end_price <= stop_price if is_long else end_price >= stop_price
                    exit_price = target_price if hit_target else (stop_price if hit_stop else end_price)
                    exit_time = row['published_date'] + pd.Timedelta(days=1)
            else:
                # For pre/after market trades, use existing logic
                end_price = float(row['end_price'])  # Convert to float
                hit_target = end_price >= target_price if is_long else end_price <= target_price
                hit_stop = end_price <= stop_price if is_long else end_price >= stop_price
                exit_price = target_price if hit_target else (stop_price if hit_stop else end_price)
                exit_time = row['published_date'] + pd.Timedelta(days=1)
            
            # Calculate P&L
            pnl = shares * (exit_price - entry_price) if is_long else shares * (entry_price - exit_price)
            pnl_pct = (pnl / trade_position_size) * 100
            
            # Update capital
            current_capital += pnl
            
            # Record trade
            trade = {
                'published_date': row['published_date'],
                'market': row['market'],
                'entry_time': entry_time,
                'exit_time': exit_time,
                'ticker': row['ticker'],
                'direction': 'LONG' if is_long else 'SHORT',
                'shares': shares,
                'entry_price': entry_price,
                'exit_price': exit_price,
                'target_price': target_price,
                'stop_price': stop_price,
                'hit_target': hit_target,
                'hit_stop': hit_stop,
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