import os
from openai import OpenAI
from dotenv import load_dotenv
from gptcache import cache

load_dotenv()

client = OpenAI()
cache.init()
cache.set_openai_key()

# Load environment variables

model_name = "gpt-4o"  # Updated model name

def tag_news(news, tags):
    prompt = f'Answering with one tag only, pick up the best tag which describes the news "{news}" from the list: {tags}'
    response = client.chat.completions.create(
        model=model_name,
        messages=[{"role": "user", "content": prompt}]
    )
    tag = response.choices[0].message.content
    return tag

def enrich_reason(content, predicted_move):
    system_prompt = """You are a financial analyst providing concise market insights. Your responses should be clear and readable without using any special characters or explicit formatting such as new lines. Use standard punctuation and avoid line breaks within sentences. Separate ideas with periods and commas as needed."""

    if predicted_move is not None:
        direction = "up" if predicted_move > 0 else "down"
        user_prompt = f"""Analyze: "{content}" Asset predicted to move {direction} by {predicted_move:+.2f}%. In less than 40 words: 1. Explain the likely cause of this {direction}ward movement. 2. Briefly discuss potential market implications. 3. Naturally include "predicted {direction}ward move of {predicted_move:+.2f}%". Be concise yet comprehensive. Ensure a complete response with no cut-off sentences."""
    else:
        user_prompt = f'In less than 40 words, summarize the potential market impact of this news. Ensure a complete response with no cut-off sentences: "{content}"'
    
    response = client.chat.completions.create(
        model=model_name,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        max_tokens=80  # Adjusted for up to 40 words
    )
    reason = response.choices[0].message.content.strip()
    return reason

def extract_ticker(company):
    prompt = f'Extract the company or issuer ticker symbol corresponding to the company name provided. Return only the ticker symbol in uppercase, without any additional text. If you cannot assign a ticker symbol, return "N/A". Company name: "{company}"'
    response = client.chat.completions.create(
        model=model_name,
        messages=[{"role": "user", "content": prompt}]
    )
    ticker = response.choices[0].message.content.strip().upper()
    return ticker if ticker != "N/A" else None

def extract_issuer(news):
    prompt = f'Extract the company or issuer name corresponding to the text provided. Return concise entity name only. If you cannot assign a ticker symbol, return "N/A". News: "{news}"'
    response = client.chat.completions.create(
        model=model_name,
        messages=[{"role": "user", "content": prompt}]
    )
    ticker = response.choices[0].message.content.strip().upper()
    return ticker if ticker != "N/A" else None