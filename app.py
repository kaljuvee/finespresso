# app.py
from flask import Flask, render_template, request, jsonify
from flask_apscheduler import APScheduler
from datetime import datetime
import logging
import asyncio
import os
from tasks.baltics import main as baltics_main
from tasks.euronext import main as euronext_main
from tasks.omx import main as omx_main
from tasks.omx import clean as clean_main
from tasks.omx import enrich as enrich_main
from utils.db_util import create_tables

app = Flask(__name__)
scheduler = APScheduler()
scheduler.init_app(app)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Ensure database tables are created
create_tables()

# Store run history
run_history = []

# Store task statuses and frequencies
task_info = {
    'baltics': {'status': 'Not run', 'frequency': 6},
    'euronext': {'status': 'Not run', 'frequency': 1},
    'omx': {'status': 'Not run', 'frequency': 1},
    'clean': {'status': 'Not run', 'frequency': 2},
    'enrich': {'status': 'Not run', 'frequency': 2}
}

def run_task(task_name, task_func):
    logger.info(f"Running {task_name} task at {datetime.now()}")
    task_info[task_name]['status'] = 'Running'
    try:
        if task_name in ['euronext', 'omx']:
            asyncio.run(task_func())
        else:
            task_func()
        run_history.append(f"{task_name} task completed successfully at {datetime.now()}")
        task_info[task_name]['status'] = 'Completed'
    except Exception as e:
        error_message = f"Error in {task_name} task at {datetime.now()}: {str(e)}"
        logger.error(error_message)
        run_history.append(error_message)
        task_info[task_name]['status'] = 'Failed'

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
        'omx': omx_main,
        'clean': clean_main,
        'enrich': enrich_main
    }
    for task_name, info in task_info.items():
        schedule_task(task_name, task_functions[task_name], info['frequency'])

@app.route('/')
def index():
    return render_template('index.html', task_info=task_info)

@app.route('/start', methods=['POST'])
def start_scheduler():
    if not scheduler.running:
        scheduler.start()
        logger.info("Scheduler started")
    return jsonify({"status": "Scheduler started"})

@app.route('/stop', methods=['POST'])
def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown()
        logger.info("Scheduler stopped")
    return jsonify({"status": "Scheduler stopped"})

@app.route('/run_task/<task_name>', methods=['POST'])
def run_task_manually(task_name):
    task_functions = {
        'baltics': baltics_main,
        'euronext': euronext_main,
        'omx': omx_main
    }
    if task_name in task_functions:
        run_task(task_name, task_functions[task_name])
        return jsonify({"status": f"{task_name} task executed"})
    else:
        return jsonify({"status": "Invalid task name"}), 400

@app.route('/set_frequency/<task_name>', methods=['POST'])
def set_task_frequency(task_name):
    frequency = int(request.form['frequency'])
    task_info[task_name]['frequency'] = frequency
    task_functions = {
        'baltics': baltics_main,
        'euronext': euronext_main,
        'omx': omx_main
    }
    schedule_task(task_name, task_functions[task_name], frequency)
    return jsonify({"status": f"{task_name} frequency set to {frequency} hours"})

@app.route('/get_logs')
def get_logs():
    return jsonify({"logs": run_history})

@app.route('/scheduler_status')
def scheduler_status():
    status = "Running" if scheduler.running else "Stopped"
    return jsonify({"status": status})

@app.route('/task_info')
def get_task_info():
    return jsonify(task_info)

if __name__ == '__main__':
    init_schedules()
    scheduler.start()
    port = int(os.environ.get('PORT', 8000))
    app.run(host='0.0.0.0', port=port, debug=False)
