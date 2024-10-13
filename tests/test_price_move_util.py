import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime
from utils.price_move_util import set_prices, create_price_moves, create_price_move

class TestPriceMoveUtil(unittest.TestCase):

    @patch('utils.price_move_util.yf.download')
    @patch('utils.price_move_util.get_calendar')
    def test_set_prices(self, mock_get_calendar, mock_yf_download):
        # Mock the calendar
        mock_calendar = MagicMock()
        mock_calendar.valid_days.return_value = pd.date_range(start='2023-01-01', end='2023-01-10')
        mock_get_calendar.return_value = mock_calendar

        # Mock yfinance data
        mock_data = pd.DataFrame({
            'Open': [100, 101, 102],
            'Close': [101, 102, 103]
        }, index=['2023-01-02', '2023-01-03', '2023-01-04'])
        mock_yf_download.return_value = mock_data

        # Test input
        row = pd.Series({
            'ticker': 'AAPL',
            'published_date': pd.Timestamp('2023-01-03 10:00:00'),
            'market': 'market_open'
        })

        result = set_prices(row)

        self.assertEqual(result['begin_price'], 101)
        self.assertEqual(result['end_price'], 102)
        self.assertAlmostEqual(result['price_change'], 1)
        self.assertAlmostEqual(result['price_change_percentage'], 1/101)

    @patch('utils.price_move_util.set_prices')
    @patch('utils.price_move_util.store_price_move')
    def test_create_price_moves(self, mock_store_price_move, mock_set_prices):
        # Mock set_prices function
        mock_set_prices.return_value = pd.Series({
            'begin_price': 100,
            'end_price': 101,
            'index_begin_price': 1000,
            'index_end_price': 1010,
            'price_change': 1,
            'index_price_change': 10,
            'price_change_percentage': 0.01,
            'index_price_change_percentage': 0.01,
            'Volume': 1000000,
            'market': 'market_open'
        })

        # Test input
        news_df = pd.DataFrame({
            'news_id': ['1', '2'],  # Add this line
            'ticker': ['AAPL', 'GOOGL'],
            'published_date': [pd.Timestamp('2023-01-03 10:00:00'), pd.Timestamp('2023-01-04 10:00:00')],
            'market': ['market_open', 'market_open']
        })

        result = create_price_moves(news_df)

        self.assertEqual(len(result), 2)
        self.assertEqual(mock_store_price_move.call_count, 2)

    def test_create_price_move(self):
        price_move = create_price_move(
            news_id='1',
            ticker='AAPL',
            published_date=datetime(2023, 1, 3, 10, 0),
            begin_price=100,
            end_price=101,
            index_begin_price=1000,
            index_end_price=1010,
            volume=1000000,
            market='market_open',
            price_change=1,
            price_change_percentage=0.01,
            index_price_change=10,
            index_price_change_percentage=0.01,
            actual_side='LONG'
        )

        self.assertEqual(price_move.news_id, '1')
        self.assertEqual(price_move.ticker, 'AAPL')
        self.assertEqual(price_move.begin_price, 100)
        self.assertEqual(price_move.end_price, 101)
        self.assertEqual(price_move.actual_side, 'LONG')
        self.assertAlmostEqual(price_move.daily_alpha, 0)

if __name__ == '__main__':
    unittest.main()