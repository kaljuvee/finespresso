import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from tasks.prices.price_move import run_price_move_task

class TestPriceMoveTask(unittest.TestCase):

    @patch('tasks.price_move.news_db_util.get_news_df')
    @patch('tasks.price_move.price_move_util.set_prices')
    @patch('tasks.price_move.price_move_util.create_price_move')
    @patch('tasks.price_move.price_move_db_util.store_price_move')
    def test_run_price_move_task(self, mock_store_price_move, mock_create_price_move, mock_set_prices, mock_get_news_df):
        # Mock the news data
        mock_news_df = pd.DataFrame({
            'news_id': ['1', '2'],
            'ticker': ['AAPL', 'GOOGL'],
            'published_date': [pd.Timestamp('2023-01-03 10:00:00'), pd.Timestamp('2023-01-04 10:00:00')]
        })
        mock_get_news_df.return_value = mock_news_df

        # Mock the set_prices function
        mock_set_prices.return_value = {
            'begin_price': 100,
            'end_price': 101,
            'index_begin_price': 1000,
            'index_end_price': 1010,
            'Volume': 1000000,
            'market': 'market_open',
            'price_change': 1,
            'price_change_percentage': 0.01,
            'index_price_change': 10,
            'index_price_change_percentage': 0.01,
            'actual_side': 'LONG'
        }

        # Mock the create_price_move function
        mock_price_move = MagicMock()
        mock_create_price_move.return_value = mock_price_move

        # Call the function
        run_price_move_task(['publisher1'], days_back=7)

        # Assertions
        mock_get_news_df.assert_called_once()
        self.assertEqual(mock_set_prices.call_count, 2)
        self.assertEqual(mock_create_price_move.call_count, 2)
        self.assertEqual(mock_store_price_move.call_count, 2)

    @patch('tasks.price_move.news_db_util.get_news_df')
    @patch('tasks.price_move.price_move_util.set_prices')
    def test_run_price_move_task_with_missing_data(self, mock_set_prices, mock_get_news_df):
        # Mock the news data with missing fields
        mock_news_df = pd.DataFrame({
            'news_id': ['1', '2'],
            'ticker': ['AAPL', None],  # Missing ticker
            'published_date': [pd.Timestamp('2023-01-03 10:00:00'), None]  # Missing published_date
        })
        mock_get_news_df.return_value = mock_news_df

        # Mock the set_prices function to return None for begin_price and end_price
        mock_set_prices.return_value = {
            'begin_price': None,
            'end_price': None
        }

        # Call the function
        run_price_move_task(['publisher1'], days_back=7)

        # Assertions
        mock_get_news_df.assert_called_once()
        self.assertEqual(mock_set_prices.call_count, 1)  # Should only be called for the first row with complete data

if __name__ == '__main__':
    unittest.main()