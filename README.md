# Flask Scheduler Application

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

## Running the Application

### Development Mode

To run the application in development mode:

```
flask run
```

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

   If you need to specify a port, you can do so by setting the PORT environment variable:
   ```
   PORT=8000 ./start.sh
   ```

## Deployment

For deployment on platforms like Render:

1. Set the build command to:
   ```
   pip install -r requirements.txt
   ```

2. Set the start command to:
   ```
   ./start.sh
   ```

3. Ensure all environment variables (like DATABASE_URL) are set in your deployment platform's configuration.

## Usage

Once the application is running:

1. Open a web browser and navigate to `http://localhost:8000` (or your server's address).
2. Use the web interface to start/stop the scheduler, run tasks manually, and set task frequencies.
3. The scheduler will run tasks automatically based on the set frequencies.
