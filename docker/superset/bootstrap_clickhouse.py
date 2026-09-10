import os
import time

import requests


base_url = os.environ["SUPERSET_URL"]
session = requests.Session()

for attempt in range(30):
    try:
        login_response = session.post(
            f"{base_url}/api/v1/security/login",
            json={
                "username": os.environ["SUPERSET_USERNAME"],
                "password": os.environ["SUPERSET_PASSWORD"],
                "provider": "db",
                "refresh": True,
            },
            timeout=10,
        )
        login_response.raise_for_status()
        access_token = login_response.json()["access_token"]
        headers = {"Authorization": f"Bearer {access_token}"}

        csrf_response = session.get(
            f"{base_url}/api/v1/security/csrf_token/",
            headers=headers,
            timeout=10,
        )
        csrf_response.raise_for_status()
        headers.update({"X-CSRFToken": csrf_response.json()["result"]})

        databases_response = session.get(
            f"{base_url}/api/v1/database/",
            headers=headers,
            params={"q": '{"page":0,"page_size":100}'},
            timeout=10,
        )
        databases_response.raise_for_status()
        databases = databases_response.json()["result"]
        existing = next(
            (
                database
                for database in databases
                if database["database_name"]
                == os.environ["CLICKHOUSE_DATABASE_NAME"]
            ),
            None,
        )
        payload = {
            "database_name": os.environ["CLICKHOUSE_DATABASE_NAME"],
            "sqlalchemy_uri": os.environ["CLICKHOUSE_SQLALCHEMY_URI"],
            "expose_in_sqllab": True,
            "allow_run_async": True,
        }

        if existing:
            response = session.put(
                f"{base_url}/api/v1/database/{existing['id']}",
                headers=headers,
                json=payload,
                timeout=10,
            )
        else:
            response = session.post(
                f"{base_url}/api/v1/database/",
                headers=headers,
                json=payload,
                timeout=10,
            )
        response.raise_for_status()
        print("ClickHouse database registered in Superset")
        break
    except (requests.RequestException, KeyError) as error:
        if attempt == 29:
            raise
        print(f"Superset is not ready yet: {error}")
        time.sleep(2)