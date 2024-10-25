import streamlit as st
import pandas as pd
from datetime import datetime
import re
from utils.db.model_db_util import get_results, get_regression_results

def format_event(event):
    if event:
        words = event.split('_')
        return ' '.join(word.capitalize() for word in words)
    return event

def format_date(date_string):
    date_obj = pd.to_datetime(date_string)
    return date_obj.strftime('%Y-%m-%d %H:%M (GMT)')

def display_model_results(results_df=None):
    if results_df is None:
        results_df = get_results()
    
    if not results_df.empty:
        st.write("Model Results:")
        
        # Format the event column
        results_df['event'] = results_df['event'].apply(format_event)
        
        # Format numeric columns
        numeric_columns = ['accuracy', 'precision', 'recall', 'f1_score', 'auc_roc', 'test_sample', 'training_sample', 'total_sample']
        for col in numeric_columns:
            if col == 'accuracy':
                results_df[col] = results_df[col].apply(lambda x: f"{x*100:.2f}%" if pd.notnull(x) else "")
            elif col in ['total_sample', 'training_sample', 'test_sample']:
                results_df[col] = results_df[col].apply(lambda x: f"{int(x):,}" if pd.notnull(x) else "")
            else:
                results_df[col] = results_df[col].apply(lambda x: f"{x:.4f}" if pd.notnull(x) else "")
        
        # Format the timestamp column
        results_df['timestamp'] = results_df['timestamp'].apply(format_date)
        
        # Reorder columns as requested
        columns_order = ['event', 'accuracy', 'total_sample', 'training_sample', 'test_sample', 'precision', 'recall', 'f1_score', 'auc_roc', 'timestamp']
        results_df = results_df[columns_order]
        
        # Rename columns for display
        column_names = ['Event', 'Accuracy (%)', 'Total Sample', 'Training Sample', 'Inference Sample', 'Precision', 'Recall', 'F1 Score', 'AUC ROC', 'Timestamp']
        results_df.columns = column_names
        
        # Display the dataframe as an HTML table
        st.markdown(results_df.to_html(escape=False, index=False), unsafe_allow_html=True)

        # Add custom CSS to style the table
        st.markdown("""
        <style>
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th, td {
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
        }
        th {
            background-color: #f2f2f2;
        }
        tr:nth-child(even) {
            background-color: #f9f9f9;
        }
        </style>
        """, unsafe_allow_html=True)
        
        # Display the total number of models
        st.write(f"Total number of models displayed: {len(results_df)}")
    else:
        st.write("No model results available.")

def display_regression_model_results(results_df=None):
    if results_df is None:
        results_df = get_regression_results()
    
    if not results_df.empty:
        st.write("Regression Model Results:")
        
        # Format the event column
        results_df['event'] = results_df['event'].apply(format_event)
        
        # Format numeric columns
        numeric_columns = ['mse', 'r2', 'mae', 'rmse', 'test_sample', 'training_sample', 'total_sample']
        for col in numeric_columns:
            if col == 'r2':
                results_df[col] = results_df[col].apply(lambda x: f"{x*100:.2f}%" if pd.notnull(x) else "")
            elif col in ['total_sample', 'training_sample', 'test_sample']:
                results_df[col] = results_df[col].apply(lambda x: f"{int(x):,}" if pd.notnull(x) else "")
            else:
                results_df[col] = results_df[col].apply(lambda x: f"{x:.4f}" if pd.notnull(x) else "")
        
        # Format the timestamp column
        results_df['timestamp'] = results_df['timestamp'].apply(format_date)
        
        # Reorder columns
        columns_order = ['event', 'r2', 'mse', 'mae', 'rmse', 'total_sample', 'training_sample', 'test_sample', 'timestamp']
        results_df = results_df[columns_order]
        
        # Rename columns for display
        column_names = ['Event', 'R² Score', 'MSE', 'MAE', 'RMSE', 'Total Sample', 'Training Sample', 'Inference Sample', 'Timestamp']
        results_df.columns = column_names
        
        # Display the dataframe as an HTML table
        st.markdown(results_df.to_html(escape=False, index=False), unsafe_allow_html=True)

        # Add custom CSS to style the table
        st.markdown("""
        <style>
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th, td {
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
        }
        th {
            background-color: #f2f2f2;
        }
        tr:nth-child(even) {
            background-color: #f9f9f9;
        }
        </style>
        """, unsafe_allow_html=True)
        
        # Display the total number of models
        st.write(f"Total number of models displayed: {len(results_df)}")
    else:
        st.write("No regression model results available.")

def display_prediction_table(df):
    # Create a copy of the dataframe to modify for display
    df_display = df.copy()

    # Select and reorder columns
    columns = ['title', 'published_date', 'publisher', 'category', 'event', 'link', 'currency_pair', 'predicted_move', 'Probability', 'region', 'reason']
    df_display = df_display[[col for col in columns if col in df_display.columns]]

    # Rename columns
    column_names = ['Title', 'Published Date', 'Publisher', 'Category', 'Event', 'Link', 'Currency Pair', 'Predicted Move (mean, 1 day forward)', 'Probability (Direction)', 'Region', 'Reason']
    df_display.columns = [name for name, col in zip(column_names, columns) if col in df_display.columns]

    # Format the Event column if it exists
    if 'Event' in df_display.columns:
        df_display['Event'] = df_display['Event'].apply(format_event)

    # Format the Published Date column if it exists
    if 'Published Date' in df_display.columns:
        df_display['Published Date'] = df_display['Published Date'].apply(format_date)

    # Create clickable links for the Title column if Link column exists
    if 'Link' in df_display.columns:
        df_display['Title'] = df_display.apply(lambda row: f'<a href="{row["Link"]}" target="_blank">{row["Title"]}</a>', axis=1)
        df_display = df_display.drop(columns=['Link'])

    # Format numeric columns with color
    def format_percentage(value):
        if pd.isna(value):
            return ""
        color = "green" if value > 0 else "red"
        return f'<span style="color: {color}">{value:.2f}%</span>'

    if 'Predicted Move (mean, 1 day forward)' in df_display.columns:
        df_display['Predicted Move (mean, 1 day forward)'] = df_display['Predicted Move (mean, 1 day forward)'].apply(format_percentage)

    # Format the Probability column
    if 'Probability (Direction)' in df_display.columns:
        df_display['Probability (Direction)'] = df_display['Probability (Direction)'].apply(lambda x: f"{x*100:.2f}%" if pd.notnull(x) else "")

    # Display the dataframe as an HTML table
    st.markdown(df_display.to_html(escape=False, index=False), unsafe_allow_html=True)

    # Add custom CSS to style the table
    st.markdown("""
    <style>
    table {
        width: 100%;
        border-collapse: collapse;
    }
    th, td {
        border: 1px solid #ddd;
        padding: 8px;
        text-align: left;
    }
    th {
        background-color: #f2f2f2;
    }
    tr:nth-child(even) {
        background-color: #f9f9f9;
    }
    a {
        color: #0066cc;
        text-decoration: none;
    }
    a:hover {
        text-decoration: underline;
    }
    </style>
    """, unsafe_allow_html=True)
