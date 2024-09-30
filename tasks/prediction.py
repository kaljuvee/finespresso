import pandas as pd
import spacy
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
import joblib
from util import db_util
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Load spaCy model
nlp = spacy.load("en_core_web_sm")
#nlp = spacy.load("en_core_web_trf")

# Preprocessing function using spaCy
def preprocess(text):
    doc = nlp(text)
    return " ".join([token.lemma_ for token in doc if not token.is_stop and not token.is_punct])

def predict_news(df):
    # Ensure all text transformations applied during training are mirrored here
    transform_topic = lambda topic: topic.lower().replace(" ", "_").replace("/", "_").replace("&", "").replace("'", "")

    # Initialize a list to store predictions
    predictions = []

    for index, row in df.iterrows():
        #start_time = time.time()  # Capture the start time
        topic = transform_topic(row['topic'])
        
        # Construct model and vectorizer filenames based on the topic
        model_filename = f'models/{topic}_classifier.joblib'
        vectorizer_filename = f'models/{topic}_tfidf_vectorizer.joblib'
        
        #print(f"Starting prediction for record {index} with topic '{topic}'...")  # Print statement indicating start
              
        try:
        	
            # Load the model and vectorizer
            loaded_model = joblib.load(model_filename)
            loaded_tfidf = joblib.load(vectorizer_filename)
            
            # Preprocess the title and transform it using the loaded TF-IDF vectorizer
            processed_title = preprocess(row['title'])  # Ensure preprocess function is defined
            transformed_title = loaded_tfidf.transform([processed_title])
            
            # Predict the class for the title
            prediction = loaded_model.predict(transformed_title)
            predictions.append(prediction[0])  # Assuming prediction is a list-like object
            
            #end_time = time.time()  # Capture the end time
            #print(f"Completed prediction for record {index}. Time taken: {end_time - start_time:.2f} seconds")  # Print time taken
            condidence_df = pd.read_csv('model_results.csv')
        except Exception as e:
            print(f"Error predicting for row {index}: {e}")
            predictions.append(None)  # Append None or a default value if prediction fails

    # Add the predictions as a new column to the DataFrame
    df['predicted_action'] = predictions
    df['confidence'] = condidence_df['accuracy']
    
    return df

ses_smtp_server = 'email-smtp.eu-north-1.amazonaws.com'
ses_smtp_port = 587  # Use 587 for STARTTLS or 465 for SSL/TLS
ses_smtp_username = 'AKIAV72VFJHAHPOBDIVJ'
ses_smtp_password = 'BMm56WkMRoKTyEnXmrxnd9fJos0itBplCzSqoISQGj9S'
from_addr = 'info@predictivelabs.co.uk'
to_addr = 'kaljuvee@gmail.com'

def send_mail(df, confidence_threshold, to_addr):
    high_confidence_df = df[df['confidence'] >= confidence_threshold]
    # If no rows match the condition, return
    if high_confidence_df.empty:
        return
    msg = MIMEMultipart()
    msg['From'] = from_addr
    msg['To'] = to_addr
    msg['Subject'] = 'High Confidence Predictions'
    body = high_confidence_df.to_html(index=False)
    msg.attach(MIMEText(body, 'html'))
    try:
        print('Attempting to send email')
        server = smtplib.SMTP(ses_smtp_server, ses_smtp_port)
        server.starttls()  # Secure the connection
        server.login(ses_smtp_username, ses_smtp_password)
        text = msg.as_string()
        server.sendmail(from_addr, to_addr, text)
        server.quit()
        print('Email sent successfully!')
    except Exception as e:
        print(f'Failed to send email: {e}')

def send_email_with_high_confidence(df, confidence_threshold, sender_email, sender_password, receiver_email):
    # Filter rows with confidence greater than the threshold
    high_confidence_df = df[df['confidence'] >= confidence_threshold]
    
    # If no rows match the condition, return
    if high_confidence_df.empty:
        return
    
    # Construct the email message
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = 'High Confidence Predictions'
    
    # Create email body with the filtered DataFrame
    body = high_confidence_df.to_html(index=False)
    msg.attach(MIMEText(body, 'html'))
    
    # Connect to SMTP server and send the email
    with smtplib.SMTP(ses_smtp_server, ses_smtp_port) as server:
        server.starttls()
        server.login(from_addr, ses_smtp_password)
        server.send_message(msg)
        print('Email sent successfully!')
        
def main():
    # Load your dataset
    df = db_util.get_news_all()
    pred_df = predict_news(df)
    #send_email_with_high_confidence(pred_df, 0.8, "", "", "") # Check readme for gmail setup
    send_mail(pred_df, 0.6, to_addr)
    pred_df.to_csv('predictions.csv')
    db_util.update_prediction(df)
   

# Check if the script is executed directly and call main
if __name__ == '__main__':
    main()