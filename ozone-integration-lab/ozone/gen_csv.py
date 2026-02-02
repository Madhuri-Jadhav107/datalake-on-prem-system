import csv
import random
from datetime import datetime, timedelta

def generate_csv(filename, records=1000):
    headers = ['id', 'user_id', 'transaction_date', 'amount', 'category', 'status']
    categories = ['Electronics', 'Groceries', 'Clothing', 'Entertainment', 'Utilities']
    statuses = ['COMPLETED', 'PENDING', 'FAILED']
    
    start_date = datetime(2025, 1, 1)
    
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for i in range(1, records + 1):
            tx_date = start_date + timedelta(days=random.randint(0, 20), hours=random.randint(0, 23))
            writer.writerow([
                i,
                f"user_{random.randint(100, 999)}",
                tx_date.strftime('%Y-%m-%d %H:%M:%S'),
                round(random.uniform(5.0, 2000.0), 2),
                random.choice(categories),
                random.choice(statuses)
            ])
    print(f"Generated {filename} with {records} records.")

if __name__ == "__main__":
    generate_csv('large_sample.csv', 1000)
