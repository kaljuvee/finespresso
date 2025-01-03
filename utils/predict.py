import pandas as pd
import joblib
from utils.db import news_db_util
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_models(event, model_type):
    model_filename = f'models/{event}_{model_type}.joblib'
    vectorizer_filename = f'models/{event}_tfidf_vectorizer_{model_type}.joblib'
    
    logger.info(f"Looking for model file: {model_filename}")
    logger.info(f"Looking for vectorizer file: {vectorizer_filename}")
    
    if os.path.exists(model_filename) and os.path.exists(vectorizer_filename):
        logger.info(f"Loading model and vectorizer for event: {event}")
        model = joblib.load(model_filename)
        vectorizer = joblib.load(vectorizer_filename)
        return model, vectorizer
    else:
        logger.warning(f"Model or vectorizer not found for event: {event}")
        logger.warning(f"Model file exists: {os.path.exists(model_filename)}")
        logger.warning(f"Vectorizer file exists: {os.path.exists(vectorizer_filename)}")
        return None, None

def predict(df):
    move_models = {}
    move_vectorizers = {}
    side_models = {}
    side_vectorizers = {}

    for index, row in df.iterrows():
        event = row['event'].lower().replace(' ', '_')
        
        # Predict move
        if pd.isnull(row['predicted_move']):
            if event not in move_models:
                move_models[event], move_vectorizers[event] = load_models(event, 'regression')
            
            if move_models[event] and move_vectorizers[event]:
                try:
                    logger.info(f"Predicting move for row {index}, event: {event}")
                    transformed_content = move_vectorizers[event].transform([row['content']])
                    prediction = move_models[event].predict(transformed_content)
                    df.at[index, 'predicted_move'] = prediction[0]
                    logger.info(f"Move prediction for row {index}: {prediction[0]}")
                except Exception as e:
                    logger.error(f"Error predicting move for row {index}: {e}", exc_info=True)
        
        # Predict side
        if pd.isnull(row['predicted_side']):
            if event not in side_models:
                side_models[event], side_vectorizers[event] = load_models(event, 'classifier_binary')
            
            if side_models[event] and side_vectorizers[event]:
                try:
                    logger.info(f"Predicting side for row {index}, event: {event}")
                    transformed_content = side_vectorizers[event].transform([row['content']])
                    prediction = side_models[event].predict(transformed_content)
                    df.at[index, 'predicted_side'] = 'UP' if prediction[0] == 1 else 'DOWN'
                    logger.info(f"Side prediction for row {index}: {df.at[index, 'predicted_side']}")
                except Exception as e:
                    logger.error(f"Error predicting side for row {index}: {e}", exc_info=True)
    
    return df

def main():
    # Get all news
    logger.info("Fetching all news data")
    news_df = news_db_util.get_news_df()
    
    # Make predictions
    logger.info("Starting predictions")
    pred_df = predict(news_df)
    
    # Update the news table with predictions
    logger.info("Updating news table with predictions")
    news_db_util.update_news_predictions(pred_df)
    
    logger.info("Predictions completed and news table updated.")

if __name__ == '__main__':
    main()
