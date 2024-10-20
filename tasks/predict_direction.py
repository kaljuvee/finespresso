import pandas as pd
import joblib
from utils import news_db_util
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def predict_rate_side(df):
    predictions = []
    models = {}
    vectorizers = {}

    for index, row in df.iterrows():
        event = row['event'].lower().replace(' ', '_')
        
        model_filename = f'models/{event}_classifier_binary.joblib'
        vectorizer_filename = f'models/{event}_tfidf_vectorizer_binary.joblib'
        
        try:
            # Load model and vectorizer if not already loaded
            if event not in models:
                if os.path.exists(model_filename) and os.path.exists(vectorizer_filename):
                    logger.info(f"Loading model and vectorizer for event: {event}")
                    models[event] = joblib.load(model_filename)
                    vectorizers[event] = joblib.load(vectorizer_filename)
                else:
                    logger.warning(f"Model or vectorizer not found for event: {event}")
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

    df['predicted_side'] = predictions
    df['predicted_side'] = df['predicted_side'].map({1: 'up', 0: 'down'})
    
    return df

def main():
    # Get rate moves with null side predictions (including news data)
    logger.info("Fetching news and rate moves data")
    merged_df = rate_db_util.get_news_rate_moves()
    
    # Make predictions
    logger.info("Starting side predictions")
    pred_df = predict_rate_side(merged_df)
    
    # Update the rate_move table with side predictions
    logger.info("Updating rate_move table with side predictions")
    rate_db_util.update_rate_move_side_predictions(pred_df)
    
    logger.info("Side predictions completed and rate_move table updated.")

if __name__ == '__main__':
    main()