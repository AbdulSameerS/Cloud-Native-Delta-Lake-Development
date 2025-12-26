import csv
import json
import random
import datetime
import os
import uuid

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# Constants
NUM_PRODUCTS = 50
NUM_USERS = 100
NUM_ORDERS = 1000
NUM_REVIEWS = 500

PRODUCTS = []
categories = ["Electronics", "Books", "Clothing", "Home", "Toys"]
adjectives = ["Super", "Generic", "Awesome", "Budget", "Luxury"]
nouns = ["Widget", "Gadget", "Thingamajig", "Doohickey", "Device"]

print("Generating Products...")
for i in range(NUM_PRODUCTS):
    PRODUCTS.append({
        "product_id": f"P-{i:04d}",
        "name": f"{random.choice(adjectives)} {random.choice(nouns)} {i}",
        "category": random.choice(categories),
        "price": round(random.uniform(10.0, 500.0), 2)
    })

print("Generating Orders (Structured Data)...")
orders_file = os.path.join(DATA_DIR, "orders.csv")
with open(orders_file, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["order_id", "user_id", "product_id", "quantity", "order_date", "total_amount"])
    
    for i in range(NUM_ORDERS):
        product = random.choice(PRODUCTS)
        qty = random.randint(1, 5)
        total = round(product["price"] * qty, 2)
        writer.writerow([
            f"O-{i:06d}",
            f"U-{random.randint(0, NUM_USERS-1):04d}",
            product["product_id"],
            qty,
            (datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 365))).isoformat(),
            total
        ])

print("Generating Reviews (Unstructured Data)...")
reviews_file = os.path.join(DATA_DIR, "reviews.json")
# Simulate unstructured lines of JSON (NDJSON) or a JSON array. 
# Delta Lake / Glue often handles JSON lines well. Let's do JSON Lines.

review_texts = [
    "This product is amazing! I love it.",
    "Terrible, broke after one day.",
    "It's okay, not worth the price.",
    "Fast delivery, good data quality.",
    "Five stars for the design, but performance is lacking.",
    "Does exactly what it says on the box.",
    "I would not recommend this to my worst enemy.",
    "Best purchase of the year!",
    "Cloud scalable architecture was missing from this toaster.",
    "ACID compliance achieved in my kitchen."
]

with open(reviews_file, 'w') as f:
    for i in range(NUM_REVIEWS):
        review = {
            "review_id": str(uuid.uuid4()),
            "product_id": random.choice(PRODUCTS)["product_id"],
            "user_id": f"U-{random.randint(0, NUM_USERS-1):04d}",
            "rating": random.randint(1, 5),
            "review_text": random.choice(review_texts) + f" (Random Ref: {random.randint(1000,9999)})",
            "timestamp": (datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 365))).isoformat()
        }
        f.write(json.dumps(review) + "\n")

print(f"Data generation complete. Files created in {DATA_DIR}: orders.csv, reviews.json")
