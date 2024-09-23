# Playwright Web Scraper

This repository contains a Python script that uses Playwright to scrape tables from websites. It supports multiple browser engines and custom URLs.

## Prerequisites

- Python 3.7 or higher
- pip (Python package installer)

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/web-scraper.git
   cd web-scraper
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

   Note: If you're on WSL (Windows Subsystem for Linux) or encountering issues, you might need to install additional dependencies:
   ```
   sudo apt-get update
   sudo apt-get install -y libglib2.0-0 libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 libdbus-1-3 libxcb1 libxkbcommon0 libx11-6 libxcomposite1 libxdamage1 libxext6 libxfixes3 libxrandr2 libgbm1 libpango-1.0-0 libcairo2 libasound2 libatspi2.0-0 libwayland-client0
   ```

## Usage

To run the web scraper:

```
python scrape_website.py [URL] [OPTIONS]
```

### Arguments:

- `URL`: The URL of the website to scrape (required)

### Options:

- `--browser {chromium,firefox,webkit}`: Browser to use (default: chromium)
- `--ignore-https-errors`: Ignore HTTPS errors (optional)

### Example:

To scrape the NASDAQ OMX Nordic company news using Chromium:

```
python scrape_website.py https://www.nasdaqomxnordic.com/news/companynews --browser chromium
```

To use Firefox and ignore HTTPS errors:

```
python fetch_news.py https://www.nasdaqomxnordic.com/news/companynews --browser firefox --ignore-https-errors
```

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
