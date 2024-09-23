# NASDAQ OMX Nordic News Scraper

This repository contains a Python script that uses Playwright to scrape company news from the NASDAQ OMX Nordic website.

## Prerequisites

- Python 3.7 or higher
- pip (Python package installer)

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/nasdaq-nordic-scraper.git
   cd nasdaq-nordic-scraper
   ```

2. Create a virtual environment (optional but recommended):
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
   ```

3. Install the required packages:
   ```
   pip install playwright pandas
   ```

4. Install Playwright browsers:
   ```
   playwright install
   ```

## Usage

To run the web scraper:

```
python scrape_nasdaq_news.py [OPTIONS]
```

### Options:

- `--url`: The URL to scrape (default: https://www.nasdaqomxnordic.com/news/companynews)
- `--browser {chromium,firefox,webkit}`: Browser to use (default: chromium)
- `--ignore-https-errors`: Ignore HTTPS errors (optional)

### Example:

To scrape the NASDAQ OMX Nordic company news using Chromium:

```
python scrape_nasdaq_news.py
```

To use Firefox and ignore HTTPS errors:

```
python scrape_nasdaq_news.py --browser firefox --ignore-https-errors
```

## Output

The script will create a CSV file named `nasdaq_news.csv` in the current directory, containing the scraped news data.

## Troubleshooting

If you encounter any issues:

1. Make sure you have the latest version of Playwright:
   ```
   pip install --upgrade playwright
   playwright install
   ```

2. Check your internet connection and make sure you can access the website manually in a regular browser.

3. If you're behind a corporate firewall or using a VPN, try using the `--ignore-https-errors` option.

4. If you're still having issues with one browser, try another (e.g., switch from Chromium to Firefox).

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
