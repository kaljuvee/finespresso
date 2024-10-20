import pandas as pd
import spacy
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
import joblib
import time
from utils.model_db_util import ModelResultsRegression, save_regression_results
import logging
import numpy as np
import os
import math
from utils.price_move_db_util import get_news_price_moves

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load spaCy model
nlp = spacy.load("en_core_web_sm")

def preprocess(text):
    doc = nlp(text)
    return " ".join([token.lemma_ for token in doc if not token.is_stop and not token.is_punct])

def train_and_save_model_for_event(event, df):
    try:
        event_df = df[df['event'] == event].copy()
        logging.info(f"Processing event: {event}, Number of samples: {len(event_df)}")
        
        if len(event_df) < 10:  # Arbitrary minimum number of samples
            logging.warning(f"Not enough data for event {event}. Skipping.")
            return None

        # Check if 'content' is null or empty, use 'title' if so
        event_df['text_to_process'] = event_df.apply(lambda row: row['title'] if pd.isnull(row['content']) or row['content'] == '' else row['content'], axis=1)
        event_df['processed_content'] = event_df['text_to_process'].apply(preprocess)
        
        # Use daily_alpha as the target variable
        y = event_df['daily_alpha']

        tfidf = TfidfVectorizer(max_features=1000)
        X = tfidf.fit_transform(event_df['processed_content'])

        logging.info(f"Shape of X: {X.shape}, Shape of y: {y.shape}")

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        model = RandomForestRegressor()
        model.fit(X_train, y_train)

        # Create a directory for models if it doesn't exist
        os.makedirs('models', exist_ok=True)

        # Save model and vectorizer with event-specific names
        model_filename = f'models/{event.replace(" ", "_").lower()}_regression.joblib'
        vectorizer_filename = f'models/{event.replace(" ", "_").lower()}_tfidf_vectorizer_regression.joblib'

        joblib.dump(model, model_filename)
        joblib.dump(tfidf, vectorizer_filename)

        y_pred = model.predict(X_test)

        mse = mean_squared_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        mae = mean_absolute_error(y_test, y_pred)
        rmse = math.sqrt(mse)

        logging.info(f"Model trained successfully for event: {event}")
        logging.info(f"MSE: {mse}, R2: {r2}, MAE: {mae}, RMSE: {rmse}")

        return {
            'event': event,
            'mse': mse,
            'r2': r2,
            'mae': mae,
            'rmse': rmse,
            'test_sample': len(y_test),
            'training_sample': len(y_train),
            'total_sample': len(event_df),
            'model_filename': model_filename,
            'vectorizer_filename': vectorizer_filename
        }

    except Exception as e:
        logging.error(f"Error processing event {event}: {str(e)}")
        logging.exception("Detailed traceback:")
        return None

def train_models_per_event(df):
    results = []
    for event in df['event'].unique():
        try:
            result = train_and_save_model_for_event(event, df)
            if result is not None:
                results.append(result)
        except Exception as e:
            logging.error(f"Error training/saving model for event '{event}': {e}")
    return results

def process_results(results, df):
    try:
        valid_results = [r for r in results if r['mse'] is not None]
        
        results_df = pd.DataFrame(valid_results)
        event_counts = df['event'].value_counts().to_dict()
        results_df['total_sample'] = results_df['event'].map(event_counts)
        results_df = results_df.sort_values(by='r2', ascending=False)
        
        # Ensure correct data types
        results_df['event'] = results_df['event'].astype(str)
        results_df['mse'] = results_df['mse'].astype(float)
        results_df['r2'] = results_df['r2'].astype(float)
        results_df['mae'] = results_df['mae'].astype(float)
        results_df['rmse'] = results_df['rmse'].astype(float)
        results_df['test_sample'] = results_df['test_sample'].astype(int)
        results_df['training_sample'] = results_df['training_sample'].astype(int)
        results_df['total_sample'] = results_df['total_sample'].astype(int)
        
        # Replace NaN values with None for database compatibility
        results_df = results_df.replace({np.nan: None})
        
        # Save to CSV
        results_df.to_csv('data/model_results_regression.csv', index=False)
        logging.info('Successfully wrote results to CSV file')
        
        # Save results to the database
        success, run_id = save_regression_results(results_df)
        if success:
            logging.info(f'Successfully wrote results to database with run_id: {run_id}')
        else:
            logging.error('Failed to write results to database')
        
        logging.info(f'Average R2 score: {results_df["r2"].mean()}')
    except Exception as e:
        logging.error(f"Error processing/saving results: {e}")
        logging.exception("Detailed traceback:")

def main():
    logging.info("Starting main function")
    
    # Get all news and price moves
    logging.info("Calling get_news_price_moves")
    merged_df = get_news_price_moves()
    
    logging.info(f"Shape of merged_df: {merged_df.shape}")
    logging.info(f"Columns in merged_df: {merged_df.columns.tolist()}")
    
    if merged_df.empty:
        logging.error("No data retrieved from the database. Please check the SQL query and database connection.")
        return
    
    # Ensure all required columns are present
    required_columns = ['id', 'content', 'daily_alpha']
    missing_columns = [col for col in required_columns if col not in merged_df.columns]
    
    if missing_columns:
        logging.error(f"Missing required columns: {missing_columns}")
        logging.info(f"Available columns: {merged_df.columns.tolist()}")
        return
    
    logging.info("All required columns are present")
    
    # Remove rows with null values in required columns
    merged_df = merged_df.dropna(subset=required_columns)
    logging.info(f"Shape after removing null values: {merged_df.shape}")
    
    # Print some statistics about the data
    logging.info(f"Daily alpha statistics:\n{merged_df['daily_alpha'].describe()}")
    
    logging.info("Starting to train models for each event")
    # Train models for each event and save them
    results = train_models_per_event(merged_df)
    logging.info(f"Number of events processed: {len(results)}")

    if not results:
        logging.warning("No models were trained. Check the data and event filtering.")
        return

    logging.info("Processing and saving results")
    # Process the results and save to a file and database
    process_results(results, merged_df)

    logging.info("Main function completed")

if __name__ == '__main__':
    main()
