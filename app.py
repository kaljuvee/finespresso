# app.py
from flask import Flask, render_template, request, jsonify, redirect, url_for, flash
from flask_apscheduler import APScheduler
from datetime import datetime, timedelta
import logging
import asyncio
import os
import threading
from collections import deque
from tasks.baltics import main as baltics_main
from tasks.euronext import main as euronext_main
from tasks.omx import main as omx_main
from tasks.clean import main as clean_main
from tasks.enrich_all import enrich_all as enrich_main
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'fallback_secret_key')
app.config['REMEMBER_COOKIE_DURATION'] = timedelta(days=90)
scheduler = APScheduler()
scheduler.init_app(app)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Use a deque to store run history with a maximum length
max_history_length = 100
run_history = deque(maxlen=max_history_length)

# Store task statuses and frequencies
task_info = {
    'baltics': {'status': 'Not run', 'frequency': 6},
    'euronext': {'status': 'Not run', 'frequency': 1},
    'omx': {'status': 'Not run', 'frequency': 1}
}

# User model
class User(UserMixin):
    def __init__(self, id):
        self.id = id

# Hardcoded user
users = {'admin': {'password': generate_password_hash('Uudised2$2')}}

@login_manager.user_loader
def load_user(user_id):
    return User(user_id)

def run_task_with_timeout(task_name, task_func, timeout=300):
    def task_wrapper():
        try:
            if task_name in ['euronext', 'omx']:
                asyncio.run(task_func())
            else:
                task_func()
            task_info[task_name]['status'] = 'Completed'
            run_history.appendleft(f"{task_name} task completed successfully at {datetime.now()}")
        except Exception as e:
            error_message = f"Error in {task_name} task at {datetime.now()}: {str(e)}"
            logger.error(error_message)
            run_history.appendleft(error_message)
            task_info[task_name]['status'] = 'Failed'

    thread = threading.Thread(target=task_wrapper)
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        logger.warning(f"{task_name} task timed out after {timeout} seconds")
        task_info[task_name]['status'] = 'Timed out'
        run_history.appendleft(f"{task_name} task timed out at {datetime.now()}")

def run_task(task_name, task_func):
    logger.info(f"Running {task_name} task at {datetime.now()}")
    task_info[task_name]['status'] = 'Running'
    run_history.appendleft(f"Started {task_name} task at {datetime.now()}")
    run_task_with_timeout(task_name, task_func)

def schedule_task(task_name, task_func, frequency):
    job_id = f'{task_name}_task'
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
    scheduler.add_job(id=job_id, func=run_task, trigger='interval', hours=frequency, args=[task_name, task_func])
    logger.info(f"Scheduled {task_name} task to run every {frequency} hours")

def init_schedules():
    task_functions = {
        'baltics': baltics_main,
        'euronext': euronext_main,
        'omx': omx_main
    }
    for task_name, info in task_info.items():
        schedule_task(task_name, task_functions[task_name], info['frequency'])

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        if username in users and check_password_hash(users[username]['password'], password):
            user = User(username)
            login_user(user, remember=True)
            return redirect(url_for('index'))
        else:
            flash('Invalid username or password')
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

@app.route('/')
@login_required
def index():
    scheduler_status = "Running" if scheduler.running else "Stopped"
    return render_template('index.html', task_info=task_info, scheduler_status=scheduler_status, run_history=run_history)

@app.route('/start', methods=['POST'])
@login_required
def start_scheduler():
    if not scheduler.running:
        scheduler.start()
        logger.info("Scheduler started")
    return redirect(url_for('index'))

@app.route('/stop', methods=['POST'])
@login_required
def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown()
        logger.info("Scheduler stopped")
    return redirect(url_for('index'))

@app.route('/run_task/<task_name>', methods=['POST'])
@login_required
def run_task_manually(task_name):
    task_functions = {
        'baltics': baltics_main,
        'euronext': euronext_main,
        'omx': omx_main
    }
    if task_name in task_functions:
        task_info[task_name]['status'] = 'Running'  # Set status to Running immediately
        threading.Thread(target=run_task, args=(task_name, task_functions[task_name])).start()
    return redirect(url_for('index'))

@app.route('/set_frequency/<task_name>', methods=['POST'])
@login_required
def set_task_frequency(task_name):
    frequency = int(request.form['frequency'])
    task_info[task_name]['frequency'] = frequency
    task_functions = {
        'baltics': baltics_main,
        'euronext': euronext_main,
        'omx': omx_main
    }
    schedule_task(task_name, task_functions[task_name], frequency)
    return redirect(url_for('index'))

@app.route('/get_logs')
@login_required
def get_logs():
    return jsonify({"logs": list(run_history)})

@app.route('/scheduler_status')
@login_required
def scheduler_status():
    status = "Running" if scheduler.running else "Stopped"
    return jsonify({"status": status})

@app.route('/task_info')
@login_required
def get_task_info():
    return jsonify(task_info)

def start_scheduler():
    if not scheduler.running:
        scheduler.start()
        logger.info("Scheduler started")

if __name__ == '__main__':
    init_schedules()
    start_scheduler()
    port = int(os.environ.get('PORT', 8000))
    app.run(host='0.0.0.0', port=port, debug=False)
else:
    # This block will be executed when running with Gunicorn
    init_schedules()
    start_scheduler()
