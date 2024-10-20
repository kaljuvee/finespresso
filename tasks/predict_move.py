import pandas as pd
import joblib
from utils import rate_db_util
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def predict_rate_move(df):
    predictions = []
    models = {}
    vectorizers = {}

    for index, row in df.iterrows():
        event = row['event'].lower().replace(' ', '_')
        
        model_filename = f'models/{event}_regression.joblib'
        vectorizer_filename = f'models/{event}_tfidf_vectorizer_regression.joblib'
        
        logger.info(f"Looking for model file: {model_filename}")
        logger.info(f"Looking for vectorizer file: {vectorizer_filename}")
        
        try:
            # Load model and vectorizer if not already loaded
            if event not in models:
                if os.path.exists(model_filename) and os.path.exists(vectorizer_filename):
                    logger.info(f"Loading model and vectorizer for event: {event}")
                    models[event] = joblib.load(model_filename)
                    vectorizers[event] = joblib.load(vectorizer_filename)
                else:
                    logger.warning(f"Model or vectorizer not found for event: {event}")
                    logger.warning(f"Model file exists: {os.path.exists(model_filename)}")
                    logger.warning(f"Vectorizer file exists: {os.path.exists(vectorizer_filename)}")
                    predictions.append(None)
                    continue

            # Use the loaded model and vectorizer
            logger.info(f"Predicting for row {index}, event: {event}")
            transformed_content = vectorizers[event].transform([row['content']])
            prediction = models[event].predict(transformed_content)
            predictions.append(prediction[0])
            logger.info(f"Prediction for row {index}: {prediction[0]}")
            
        except Exception as e:
            logger.error(f"Error predicting for row {index}: {e}", exc_info=True)
            predictions.append(None)

    df['predicted_rate_change'] = predictions
    
    return df

def main():
    # Get all news and rate moves
    logger.info("Fetching news and rate moves data")
    merged_df = rate_db_util.get_news_rate_moves()
    
    # Make predictions
    logger.info("Starting move predictions")
    pred_df = predict_rate_move(merged_df)
    
    # Update the rate_move table with move predictions
    logger.info("Updating rate_move table with move predictions")
    rate_db_util.update_rate_move_value_predictions(pred_df)
    
    logger.info("Move predictions completed and rate_move table updated.")

if __name__ == '__main__':
    main()