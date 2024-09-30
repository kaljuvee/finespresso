import pandas as pd
import spacy
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
import joblib
from util import db_util
import time

# Load spaCy model
nlp = spacy.load("en_core_web_sm")
#nlp = spacy.load("en_core_web_trf")

# Preprocessing function using spaCy
def preprocess(text):
    doc = nlp(text)
    return " ".join([token.lemma_ for token in doc if not token.is_stop and not token.is_punct])

def train_and_save_model_for_topic(topic, df):
    try:
        # Create a copy of the filtered DataFrame to avoid SettingWithCopyWarning
        topic_df = df[df['topic'] == topic].copy()

        # Process the title and create features
        topic_df['processed_title'] = topic_df['title'].apply(preprocess)
        topic_df['features'] = topic_df['processed_title']

        # Feature extraction using TF-IDF
        tfidf = TfidfVectorizer(max_features=1000)
        X = tfidf.fit_transform(topic_df['features'])
        y = topic_df['actual_action']

        # Splitting the dataset into training and testing sets
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Train the model
        model = RandomForestClassifier()
        model.fit(X_train, y_train)

        model_filename = f'models/{topic}_classifier.joblib'
        vectorizer_filename = f'models/{topic}_tfidf_vectorizer.joblib'

        joblib.dump(model, model_filename)
        joblib.dump(tfidf, vectorizer_filename)

        # Model evaluation
        y_pred = model.predict(X_test)
        report = classification_report(y_test, y_pred, output_dict=True)

        return report['accuracy'], report['macro avg']['support'], model_filename, vectorizer_filename

    except Exception as e:
        print(f"Error processing topic {topic}: {e}")
        return None, None, None, None

def train_models_per_topic(df):
    results = []
    for topic in df['topic'].unique():
        try:
            accuracy, support, model_filename, vectorizer_filename = train_and_save_model_for_topic(topic, df)
            # Append a dictionary with results for better readability and data manipulation
            results.append({
                'topic': topic,
                'accuracy': accuracy,
                'test_sample': support
            })
        except Exception as e:
            print(f"Error training/saving model for topic '{topic}': {e}")
    return results

def process_results(results, df):
    try:
        # Convert results to a DataFrame
        results_df = pd.DataFrame(results)

        # Get topic counts from the DataFrame
        topic_counts = df['topic'].value_counts().to_dict()

        # Add a 'total_sample' column to the results DataFrame
        results_df['total_sample'] = results_df['topic'].map(topic_counts)
        results_df = results_df.sort_values(by='accuracy', ascending=False)
        results_df.fillna(0, inplace=True)
        # Save the results DataFrame to a file
        results_df.to_csv('models/model_results.csv', index=False)
        print('successfully wrote results to file: ', results_df)
        # Get the current time in seconds since the Epoch
        current_time_seconds = time.time()

        # Convert the current time to a bigint by removing the decimal part
        runid = int(current_time_seconds)
        db_util.write_table(results_df, 'model_run')
        print('successfully wrote results: ', results_df)
        print('avg accuracy: ', results_df['accuracy'].mean())
    except Exception as e:
        print(f"Error processing/saving results: {e}")


def main():
    # Load your dataset
    #df = pd.read_csv('../data/news_price.csv')
    df = db_util.read_news_price()
    print('df: ', df)
    # Apply formatting to the 'topic' column of the DataFrame: lowercase, replace spaces and forward slashes with underscores
    df['topic'] = df['topic'].str.lower().str.replace(" ", "_").str.replace("/", "_").str.replace("&", "").str.replace("'", "")
    df = df.dropna(subset=['topic', 'actual_action'])

    # Train models for each topic and save them
    results = train_models_per_topic(df)

    # Process the results and save to a file
    process_results(results, df)

    # Make a prediction for a random title from the DataFrame
    #predict_random_title(df)

# Check if the script is executed directly and call main
if __name__ == '__main__':
    main()


