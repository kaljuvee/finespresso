# Finespresso

This application is a Flask-based web service that schedules and runs data download tasks for Baltics, Euronext, and OMX markets.

## Prerequisites

- Python 3.7+
- pip (Python package installer)

## Installation

1. Clone the repository:
   ```
   git clone [your-repo-url]
   cd [your-repo-directory]
   ```

2. Create a virtual environment:
   ```
   python -m venv venv
   ```

3. Activate the virtual environment:
   - On Windows:
     ```
     venv\Scripts\activate
     ```
   - On macOS and Linux:
     ```
     source venv/bin/activate
     ```

4. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

## Configuration

1. Create a `.env` file in the root directory with the following content:
   ```
   DATABASE_URL=your_database_url_here
   ```

2. Create a `.flaskenv` file in the root directory with the following content:
   ```
   FLASK_APP=app.py
   FLASK_ENV=production
   FLASK_RUN_PORT=8000
   FLASK_RUN_HOST=0.0.0.0
   ```

## Running the Web Application

### Web / Streamlit app

To run the application in development mode:

```
streamlit run Home.py
```
- Then open a web browser and navigate to `http://localhost:8501` (or your server's address).

## Running the Flask Scheduler

### Development Mode

To run the application in development mode:

```
flask run
```

### Usage

Once the application is running:

1. Open a web browser and navigate to `http://localhost:8000` (or your server's address).
2. Use the web interface to start/stop the scheduler, run tasks manually, and set task frequencies.
3. The scheduler will run tasks automatically based on the set frequencies.


### Production Mode with Gunicorn

1. Ensure Gunicorn is installed:
   ```
   pip install gunicorn
   ```

2. Create a `start.sh` file in the root directory with the following content:
   ```bash
   #!/bin/bash
   gunicorn app:app -b 0.0.0.0:$PORT
   ```

3. Make the script executable:
   ```
   chmod +x start.sh
   ```

4. Run the application:
   ```
   ./start.sh
   ```
### Running Tasks Manually

1. Tasks can also be run manually, one by one, for example download tasks:
   ```
   python -m tasks.clean
   python -m tasks.baltics
   python -m tasks.omx
   python -m tasks.euronext
   ```
1. Enrichment tasks:
   ```
   python -m tasks.enrich_tag
   python -m tasks.enrich_summary
   python -m tasks.enrich_content
   python -m tasks.enrich_ticker
 
 ### Run Unit Tests
 ```
python -m unittest discover tests
```