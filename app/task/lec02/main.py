import os
import json
import requests
import shutil
from flask import Flask, request, jsonify
from dotenv import load_dotenv

"""
# Job 1 - витягує дані
python main.py

curl -X POST http://localhost:8081/ `
  -H "Content-Type: application/json" `
  -d '{"raw_dir":"file_storage/raw","date":"2022-08-09"}'  

"""

# Загружаємо змінні середовища
config_path = os.path.join(os.path.dirname(__file__), 'Access', 'config.env')
load_dotenv(config_path)

app = Flask(__name__)


@app.route('/', methods=['POST'])
def extract_sales():
    try:
        data = request.get_json()
        raw_dir = data.get('raw_dir')
        date_str = data.get('date')

        if not raw_dir or not date_str:
            return jsonify({"error": "Потрібні параметри: raw_dir та date"}), 400

        # Формуємо шлях
        sales_dir = os.path.join(raw_dir, 'sales', date_str)

        # Очищаємо директорію
        if os.path.exists(sales_dir):
            shutil.rmtree(sales_dir)
        os.makedirs(sales_dir, exist_ok=True)

        # Витягуємо дані з API
        auth_token = os.getenv('AUTH_TOKEN')
        api_base = os.getenv('API_URL')

        if not auth_token or not api_base:
            return jsonify({"error": "AUTH_TOKEN або API_URL не знайдені"}), 500

        api_url = f"{api_base.rstrip('/')}/sales"
        headers = {"Authorization": auth_token}

        all_records = []
        page = 1
        max_pages = 100

        while page <= max_pages:
            params = {"date": date_str, "page": page}

            try:
                response = requests.get(api_url, headers=headers, params=params, timeout=10)

                # Якщо 404 - дані закінчилися
                if response.status_code == 404:
                    break

                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                return jsonify({"error": str(e)}), 500

            data_page = response.json()

            if not data_page or len(data_page) == 0:
                break

            all_records.extend(data_page)
            page += 1

        # Зберігаємо в один файл
        file_path = os.path.join(sales_dir, f"sales_{date_str}.json")
        with open(file_path, 'w', encoding='utf-8') as f:
            # json.dump(all_records, f, ensure_ascii=False)
            json.dump(all_records, f, ensure_ascii=False, indent=2)

        return jsonify({
            "status": "success",
            "records": len(all_records),
            "file": file_path
        }), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(host='localhost', port=8081, debug=False)