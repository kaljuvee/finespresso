import sys
import os
from datetime import datetime, timedelta
import json
from pathlib import Path
import pandas as pd

# Add project root to Python path properly
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from ai.market_agent import MarketAgent
from ai.utils.logger_util import setup_logger

logger = setup_logger(__name__)

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, pd.Timestamp)):
        return obj.isoformat()
    if isinstance(obj, timedelta):
        return str(obj)
    raise TypeError(f"Type {type(obj)} not serializable")

def save_test_results(test_name: str, results: dict):
    """Save test results to a timestamped JSON file"""
    # Create data directory if it doesn't exist
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    
    # Create filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = data_dir / f"{test_name}_{timestamp}.json"
    
    # Save results with custom serializer
    with open(filename, 'w') as f:
        json.dump(results, f, indent=2, default=json_serial)
    logger.info(f"Test results saved to {filename}")

def test_timeframe_parsing():
    agent = MarketAgent()
    results = {
        "test_name": "timeframe_parsing",
        "timestamp": datetime.now(),
        "cases": []
    }
    
    test_cases = [
        ("How has EUR/USD performed in the last 24 hours?", timedelta(hours=24)),
        ("Show me the USD/JPY trend for the past 7 days", timedelta(days=7)),
        ("What's the GBP/USD rate for the last week?", timedelta(days=7)),
        ("Analysis of EUR/JPY for the past month", timedelta(days=30))
    ]
    
    for input_text, expected_delta in test_cases:
        logger.info(f"\nTesting timeframe parsing for: {input_text}")
        result = agent.parse_timeframe(input_text)
        logger.info(f"Expected: {expected_delta}, Got: {result}")
        
        case_result = {
            "input": input_text,
            "expected": str(expected_delta),
            "result": str(result),
            "passed": result == expected_delta
        }
        results["cases"].append(case_result)
        
        if result != expected_delta:
            logger.error(f"❌ Test failed for '{input_text}'")
        else:
            logger.info(f"✅ Test passed for '{input_text}'")
    
    save_test_results("timeframe_parsing", results)

def test_currency_pair_normalization():
    agent = MarketAgent()
    results = {
        "test_name": "currency_pair_normalization",
        "timestamp": datetime.now(),
        "cases": []
    }
    
    test_cases = [
        ("How is EUR doing against USD?", "EUR/USD"),
        ("What's the JPY rate?", "JPY/USD"),
        ("Compare GBP and EUR", "GBP/EUR"),
        ("Random text without currencies", None)
    ]
    
    for input_text, expected_pair in test_cases:
        logger.info(f"\nTesting currency pair normalization for: {input_text}")
        result = agent.normalize_currency_pair(input_text)
        logger.info(f"Expected: {expected_pair}, Got: {result}")
        
        case_result = {
            "input": input_text,
            "expected": expected_pair,
            "result": result,
            "passed": result == expected_pair
        }
        results["cases"].append(case_result)
        
        if result != expected_pair:
            logger.error(f"❌ Test failed for '{input_text}'")
        else:
            logger.info(f"✅ Test passed for '{input_text}'")
    
    save_test_results("currency_pair_normalization", results)

def test_data_fetching():
    agent = MarketAgent()
    results = {
        "test_name": "data_fetching",
        "timestamp": datetime.now(),
        "cases": []
    }
    
    test_cases = [
        {
            "currency_pair": "EUR/USD",
            "query": "How has EUR/USD performed in the last hour?",
            "description": "Last hour data (intraday)"
        },
        {
            "currency_pair": "EUR/USD",
            "query": "How has EUR/USD performed in the last 24 hours?",
            "description": "Last 24 hours data"
        },
        {
            "currency_pair": "GBP/USD",
            "query": "What's the GBP/USD rate for today?",
            "description": "Today's data"
        },
        {
            "currency_pair": "USD/JPY",
            "query": "Show me USD/JPY for the last week",
            "description": "Last week data"
        },
        {
            "currency_pair": "GBP/EUR",
            "query": "Show me GBP/EUR for the last month",
            "description": "Last month data"
        }
    ]
    
    for test_case in test_cases:
        logger.info(f"\nTesting data fetching for {test_case['currency_pair']} - {test_case['description']}")
        data = agent.get_currency_data(test_case['currency_pair'], test_case['query'])
        
        # Convert sample data to JSON-serializable format
        sample_data = None
        if data is not None and not data.empty:
            sample = data.tail(5)
            sample_data = {
                'index': [str(idx) for idx in sample.index],
                'data': sample.to_dict(orient='records')
            }
        
        case_result = {
            "currency_pair": test_case['currency_pair'],
            "query": test_case['query'],
            "description": test_case['description'],
            "success": data is not None and not data.empty,
            "data_info": {
                "shape": list(data.shape) if data is not None else None,
                "columns": data.columns.tolist() if data is not None else None,
                "start_time": str(data.index[0]) if data is not None and not data.empty else None,
                "end_time": str(data.index[-1]) if data is not None and not data.empty else None,
                "latest_rates": {
                    "open": float(data['Open'].iloc[-1]) if data is not None and not data.empty else None,
                    "high": float(data['High'].iloc[-1]) if data is not None and not data.empty else None,
                    "low": float(data['Low'].iloc[-1]) if data is not None and not data.empty else None,
                    "close": float(data['Close'].iloc[-1]) if data is not None and not data.empty else None
                },
                "sample_data": sample_data
            }
        }
        results["cases"].append(case_result)
        
        if data is not None and not data.empty:
            logger.info(f"✅ Successfully fetched data for {test_case['currency_pair']}")
            logger.info(f"Data shape: {data.shape}")
            logger.info(f"Time range: {data.index[0]} to {data.index[-1]}")
            logger.info(f"Latest rates: Open={data['Open'].iloc[-1]:.4f}, Close={data['Close'].iloc[-1]:.4f}")
            logger.info(f"Sample data:\n{data.tail(3)}")
        else:
            logger.error(f"❌ Failed to fetch data for {test_case['currency_pair']}")
    
    save_test_results("data_fetching", results)

if __name__ == '__main__':
    logger.info("Starting Market Agent tests...")
    
    try:
        test_timeframe_parsing()
        test_currency_pair_normalization()
        test_data_fetching()
        logger.info("\nAll tests completed!")
    except Exception as e:
        logger.error(f"Test failed with error: {e}", exc_info=True) 