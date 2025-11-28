import requests
from config import AUTH_TOKEN, API_URL


class SalesApiClient:
    """Data access layer - handles API communication"""

    def __init__(self):
        self.auth_token = AUTH_TOKEN
        self.api_url = API_URL

    def fetch_sales(self, date_str: str) -> list:
        """
        Fetch sales data from API

        Args:
            date_str: Date in format 'YYYY-MM-DD'

        Returns:
            List of sales records
        """
        all_records = []
        page = 1
        max_pages = 100

        while page <= max_pages:
            params = {"date": date_str, "page": page}
            headers = {"Authorization": self.auth_token}

            try:
                response = requests.get(
                    f"{self.api_url}/sales",
                    headers=headers,
                    params=params,
                    timeout=10
                )

                # If 404 - data ended
                if response.status_code == 404:
                    break

                response.raise_for_status()

            except requests.exceptions.HTTPError as e:
                raise Exception(f"API Error: {str(e)}")

            data_page = response.json()

            if not data_page or len(data_page) == 0:
                break

            all_records.extend(data_page)
            page += 1

        return all_records