import requests
from bs4 import BeautifulSoup

def fetch_url_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        # Extract text from paragraphs, removing any scripts or styles
        for script in soup(["script", "style"]):
            script.decompose()
        text = ' '.join([p.get_text() for p in soup.find_all('p')])
        return text[:1000]  # Return first 1000 characters
    except requests.RequestException:
        return "Failed to fetch content"