from faker import Faker
import pandas as pd
from datetime import datetime
import pytz

fake = Faker()

def generate_fake_data(num_rows=5):
    data = []
    for _ in range(num_rows):
        data.append({
            "name": fake.name(),
            "address": fake.address().replace("\n", ", "),
            "email": fake.email(),
            "ingestion_time": datetime.now(pytz.timezone("Asia/Kolkata")).isoformat()
        })
    return pd.DataFrame(data)