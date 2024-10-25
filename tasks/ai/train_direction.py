import pandas as pd
import spacy
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score
import joblib
import time
from utils.db.model_db_util import save_results
import numpy as np
import os
from utils.db.price_move_db_util import get_news_price_moves
from utils.logging.log_util import get_logger

logger = get_logger(__name__)

# Load spaCy model
nlp = spacy.load("en_core_web_sm")

def preprocess(text):
    doc = nlp(text)
    return " ".join([token.lemma_ for token in doc if not token.is_stop and not token.is_punct])

def train_and_save_model_for_event(event, df):
    try:
        event_df = df[df['event'] == event].copy()
        logger.info(f"Processing event: {event}, Number of samples: {len(event_df)}")
        
        # Filter out 'unknown' values
        event_df = event_df[event_df['actual_side'].isin(['UP', 'DOWN'])]
        
        logger.info(f"Value counts of actual_side after filtering: {event_df['actual_side'].value_counts().to_dict()}")
        
        if len(event_df) < 10:  # Arbitrary minimum number of samples
            logger.warning(f"Not enough data for event {event} after filtering. Skipping.")
            return None

        # Check if 'content' is null or empty, use 'title' if so
        event_df['text_to_process'] = event_df.apply(lambda row: row['title'] if pd.isnull(row['content']) or row['content'] == '' else row['content'], axis=1)
        event_df['processed_content'] = event_df['text_to_process'].apply(preprocess)
        
        # Use actual_side as the target variable
        y = event_df['actual_side'].map({'UP': 1, 'DOWN': 0})
        
        if len(y.unique()) < 2:
            logger.warning(f"Only one class present in the target variable for event {event}. Skipping.")
            return None

        tfidf = TfidfVectorizer(max_features=1000)
        X = tfidf.fit_transform(event_df['processed_content'])

        logger.info(f"Shape of X: {X.shape}, Shape of y: {y.shape}")
        logger.info(f"Value counts of y: {y.value_counts().to_dict()}")

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        model = RandomForestClassifier()
        model.fit(X_train, y_train)

        # Create a directory for models if it doesn't exist
        os.makedirs('models', exist_ok=True)

        # Save model and vectorizer with event-specific names
        model_filename = f'models/{event.replace(" ", "_").lower()}_classifier_binary.joblib'
        vectorizer_filename = f'models/{event.replace(" ", "_").lower()}_tfidf_vectorizer_binary.joblib'

        joblib.dump(model, model_filename)
        joblib.dump(tfidf, vectorizer_filename)

        y_pred = model.predict(X_test)
        y_pred_proba = model.predict_proba(X_test)[:, 1]

        accuracy = accuracy_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred, zero_division=0)
        recall = recall_score(y_test, y_pred, zero_division=0)
        f1 = f1_score(y_test, y_pred, zero_division=0)
        auc_roc = roc_auc_score(y_test, y_pred_proba) if len(np.unique(y_test)) > 1 else 0

        logger.info(f"Model trained successfully for event: {event}")
        logger.info(f"Accuracy: {accuracy}, Precision: {precision}, Recall: {recall}, F1: {f1}, AUC-ROC: {auc_roc}")

        return {
            'event': event,
            'accuracy': accuracy,
            'precision': precision,
            'recall': recall,
            'f1_score': f1,
            'auc_roc': auc_roc,
            'test_sample': len(y_test),
            'training_sample': len(y_train),
            'total_sample': len(event_df),
            'model_filename': model_filename,
            'vectorizer_filename': vectorizer_filename
        }

    except Exception as e:
        logger.error(f"Error processing event {event}: {str(e)}")
        logger.exception("Detailed traceback:")
        return None

def train_models_per_event(df):
    results = []
    for event in df['event'].unique():
        try:
            result = train_and_save_model_for_event(event, df)
            if result is not None:
                results.append(result)
        except Exception as e:
            logger.error(f"Error training/saving model for event '{event}': {e}")
    return results

def process_results(results, df):
    try:
        valid_results = [r for r in results if r['accuracy'] is not None]
        
        results_df = pd.DataFrame(valid_results)
        event_counts = df['event'].value_counts().to_dict()
        results_df['total_sample'] = results_df['event'].map(event_counts)
        results_df = results_df.sort_values(by='accuracy', ascending=False)
        
        # Ensure correct data types
        results_df['event'] = results_df['event'].astype(str)
        results_df['accuracy'] = results_df['accuracy'].astype(float)
        results_df['precision'] = results_df['precision'].astype(float)
        results_df['recall'] = results_df['recall'].astype(float)
        results_df['f1_score'] = results_df['f1_score'].astype(float)
        results_df['auc_roc'] = results_df['auc_roc'].astype(float)
        results_df['test_sample'] = results_df['test_sample'].astype(int)
        results_df['training_sample'] = results_df['training_sample'].astype(int)
        results_df['total_sample'] = results_df['total_sample'].astype(int)
        
        # Replace NaN values with None for database compatibility
        results_df = results_df.replace({np.nan: None})
        
        # Save to CSV
        results_df.to_csv('data/model_results_binary.csv', index=False)
        logger.info('Successfully wrote results to CSV file')
        
        # Save results to the database
        success, run_id = save_results(results_df)
        if success:
            logger.info(f'Successfully wrote results to database with run_id: {run_id}')
        else:
            logger.error('Failed to write results to database')
        
        logger.info(f'Average accuracy score: {results_df["accuracy"].mean()}')
    except Exception as e:
        logger.error(f"Error processing/saving results: {e}")
        logger.exception("Detailed traceback:")

def main():
    logger.info("Starting main function")
    
    # Get all news and price moves
    logger.info("Calling get_news_price_moves")
    merged_df = get_news_price_moves()
    
    logger.info(f"Shape of merged_df: {merged_df.shape}")
    logger.info(f"Columns in merged_df: {merged_df.columns.tolist()}")
    
    if merged_df.empty:
        logger.error("No data retrieved from the database. Please check the SQL query and database connection.")
        return
    
    logger.info(f"Value counts of actual_side: {merged_df['actual_side'].value_counts(dropna=False).to_dict()}")
    logger.info(f"Number of non-null actual_side values: {merged_df['actual_side'].notnull().sum()}")
    logger.info(f"Number of null actual_side values: {merged_df['actual_side'].isnull().sum()}")
    
    # Print out counts for each unique value in actual_side
    for value in merged_df['actual_side'].unique():
        count = (merged_df['actual_side'] == value).sum()
        logger.info(f"Count of '{value}' in actual_side: {count}")
    
    # Ensure all required columns are present
    required_columns = ['id', 'content', 'actual_side']
    missing_columns = [col for col in required_columns if col not in merged_df.columns]
    
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        logger.info(f"Available columns: {merged_df.columns.tolist()}")
        return
    
    logger.info("All required columns are present")
    
    # Remove rows with null values in required columns
    merged_df = merged_df.dropna(subset=required_columns)
    logger.info(f"Shape after removing null values: {merged_df.shape}")
    
    # Print some statistics about the data
    logger.info(f"Number of unique events: {merged_df['event'].nunique()}")
    logger.info(f"Event value counts:\n{merged_df['event'].value_counts()}")   
    logger.info(f"Actual side value counts:\n{merged_df['actual_side'].value_counts()}")
    logger.info(f"Number of rows with actual_side as 'UP' or 'DOWN': {merged_df['actual_side'].isin(['UP', 'DOWN']).sum()}")
    
    if merged_df['actual_side'].isin(['UP', 'DOWN']).sum() == 0:
        logger.error("No valid 'UP' or 'DOWN' values in actual_side column. Cannot train models.")
        return
    
    logger.info("Starting to train models for each event")
    # Train models for each event and save them
    results = train_models_per_event(merged_df)
    logger.info(f"Number of events processed: {len(results)}")

    if not results:
        logger.warning("No models were trained. Check the data and event filtering.")
        return

    logger.info("Processing and saving results")
    # Process the results and save to a file and database
    process_results(results, merged_df)

    logger.info("Main function completed")

if __name__ == '__main__':
    main()
