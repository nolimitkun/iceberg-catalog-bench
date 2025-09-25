import random
import time
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()


def generate_sales_events(num_rows: int, start_date: datetime) -> list[dict]:
    countries = ["US", "GB", "DE", "FR", "IN", "CN", "JP", "BR", "CA", "AU"]
    rows: list[dict] = []
    base_ts = start_date
    for i in range(num_rows):
        event_ts = base_ts + timedelta(seconds=random.randint(0, 60 * 60 * 24 * 30))
        qty = random.randint(1, 10)
        price = round(random.uniform(1, 999), 2)
        row = {
            "event_id": i + 1,
            "tenant_id": random.randint(1, 1000),
            "event_ts": event_ts,
            "sku": fake.bothify(text="SKU-????-#####"),
            "qty": qty,
            "price": price,
            "country": random.choice(countries),
            "ds": event_ts.date(),
        }
        rows.append(row)
    return rows
