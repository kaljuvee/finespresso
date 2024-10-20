import streamlit as st

st.markdown("""
# Methodology

Our approach to predicting currency movements based on financial news involves several key steps:

## Data Preparation

1. **News Collection**: We gather financial news articles from various sources.
2. **Currency Movement Data**: We collect historical currency rate movements.
3. **Data Merging**: We combine news articles with corresponding currency movements.

## Text Processing

1. **Preprocessing**: We clean and preprocess the text data, removing stop words and punctuation.
2. **Vectorization**: We convert the text data into numerical vectors that can be used by machine learning models.

## Model Training

We train two types of models for each currency event:

1. **Binary Classification Model**: 
   - Predicts whether the currency will move up or down.
   - Uses preprocessed news text as input.
   - Outputs a binary prediction (up/down).

2. **Regression Model**: 
   - Predicts the magnitude of the currency movement.
   - Uses the same preprocessed news text as input.
   - Outputs a continuous value representing the predicted percentage change.

## Inference

When new financial news comes in:

1. We preprocess and vectorize the news text.
2. We use the appropriate binary classification model to predict the direction of movement.
3. We use the regression model to predict the magnitude of the movement.

## Additional Features

- **Region and Currency Extraction**: We use an AI-powered natural language processing model to automatically extract relevant regions and currencies from the news text.

## Model Evaluation

We evaluate our models using standard metrics:

- For binary classification: accuracy, precision, recall, F1-score, and AUC-ROC.
- For regression: mean squared error (MSE), R-squared (R2), mean absolute error (MAE), and root mean squared error (RMSE).

## Future Improvements

1. **Ensemble Methods**: Combine predictions from multiple models for improved accuracy.
2. **Deep Learning**: Explore advanced neural network architectures for both classification and regression tasks.
3. **Real-time Updates**: Implement a system for continuous model updating as new data becomes available.
4. **Feature Engineering**: Develop more sophisticated features from the news text and incorporate additional relevant data sources.
5. **Explainable AI**: Implement techniques to better understand and interpret model predictions.
6. **Cross-validation**: Implement more robust cross-validation techniques to ensure model generalization.
7. **Hyperparameter Tuning**: Use advanced techniques like Bayesian optimization for hyperparameter tuning.

""")