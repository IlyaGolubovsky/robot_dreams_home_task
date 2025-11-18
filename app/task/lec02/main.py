"""
Job 1 - Extract sales data from API and save to JSON

Usage:
    python main.py

Then call:
iwr http://localhost:8081/ -Method POST -ContentType "application/json" -Body '{"raw_dir":"file_storage/raw","date":"2022-08-09"}'
"""

from flask import Flask
from presentation.job1_routes import job1_bp

app = Flask(__name__)
app.register_blueprint(job1_bp)


if __name__ == '__main__':
    app.run(host='localhost', port=8081, debug=False)