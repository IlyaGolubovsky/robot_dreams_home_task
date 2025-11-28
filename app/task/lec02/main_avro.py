"""
Job 2 - Transform JSON files to Avro format

Usage:
    python main_avro.py

Then call:
iwr http://localhost:8082/ -Method POST -ContentType "application/json" -Body '{"raw_dir":"file_storage/raw/sales/2022-08-09","stg_dir":"file_storage/stg/sales/2022-08-09"}'
"""

from flask import Flask
from presentation.job2_routes import job2_bp

app = Flask(__name__)
app.register_blueprint(job2_bp)


if __name__ == '__main__':
    app.run(host='localhost', port=8082, debug=False)