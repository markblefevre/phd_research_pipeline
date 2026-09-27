import os
import requests

API_KEY = os.environ.get("JQUANTS_API_KEY")

if not API_KEY:
    raise SystemExit("JQUANTS_API_KEY is not set")

url = "https://api.jquants.com/v2/equities/bars/daily"
headers = {"x-api-key": API_KEY}

# Toyota Motor
params = {
    "code": "7203",
    "from": "2015-01-01",
    "to": "2017-01-01",
}

r = requests.get(url, headers=headers, params=params, timeout=30)

print("HTTP status:", r.status_code)

if r.status_code != 200:
    print(r.text[:2000])
    raise SystemExit(1)

js = r.json()

rows = (
    js.get("daily_quotes")
    or js.get("data")
    or []
)

print("Rows returned:", len(rows))

if rows:
    dates = sorted(
        row.get("Date") or row.get("date")
        for row in rows
        if row.get("Date") or row.get("date")
    )

    print("Earliest date:", dates[0])
    print("Latest date:  ", dates[-1])

    print("\nFirst row:")
    print(rows[0])
else:
    print("No Toyota data returned.")
    print(js)