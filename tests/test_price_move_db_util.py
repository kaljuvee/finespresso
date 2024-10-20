import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
from utils.db.price_move_db_util import PriceMove, store_price_move

class TestPriceMoveDbUtil(unittest.TestCase):

    def test_price_move_creation(self):
        price_move = PriceMove(
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
            daily_alpha=0,
            actual_side='LONG'
        )

        self.assertEqual(price_move.news_id, '1')
        self.assertEqual(price_move.ticker, 'AAPL')
        self.assertEqual(price_move.begin_price, 100)
        self.assertEqual(price_move.end_price, 101)
        self.assertEqual(price_move.actual_side, 'LONG')

    @patch('utils.price_move_db_util.Session')
    def test_store_price_move(self, mock_session):
        # Create a mock session
        mock_session_instance = MagicMock()
        mock_session.return_value = mock_session_instance

        # Create a sample PriceMove object
        price_move = PriceMove(
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
            daily_alpha=0,
            actual_side='LONG'
        )

        # Call the function
        store_price_move(price_move)

        # Assert that the session methods were called
        mock_session.assert_called_once()
        mock_session_instance = mock_session.return_value.__enter__.return_value
        mock_session_instance.add.assert_called_once_with(price_move)
        mock_session_instance.commit.assert_called_once()
        self.assertEqual(mock_session_instance.close.call_count, 2)  # Change this line

        # Test the verification query
        mock_verify_session = MagicMock()
        mock_session.return_value = mock_verify_session
        mock_verify_session.query.return_value.filter_by.return_value.first.return_value = price_move

        # Call the function again to trigger verification
        store_price_move(price_move)

        # Assert that the verification query was made
        mock_verify_session.query.assert_called_once_with(PriceMove)
        mock_verify_session.query.return_value.filter_by.assert_called_once_with(news_id='1')
        mock_verify_session.close.assert_called_once()

if __name__ == '__main__':
    unittest.main()