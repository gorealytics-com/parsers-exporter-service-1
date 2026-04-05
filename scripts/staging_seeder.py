#!/usr/bin/env python3
"""
Staging Data Seeder — pushes synthetic data into Redis queues.

Ported from: scripts/staging_seeder.py

Reads exporter definitions from Helm values.yaml (or env vars) to discover
queue names, generates realistic synthetic payloads with Faker, and pushes
them to Redis. Runs as a one-shot container during dev/staging setup.

Environment:
  REDIS_URL         — Redis connection string
  SEED_BATCH_SIZE   — records per queue (default: 15)
  VALUES_FILE       — path to values.yaml (default: helm/aragog-exporters/values.yaml)
"""

import logging
import os
import random
import uuid
from hashlib import sha1

import orjson
import redis
import yaml
from faker import Faker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SEEDER] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("staging-seeder")

fake = Faker()
Faker.seed(42)
random.seed(42)

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
SEED_BATCH = int(os.environ.get("SEED_BATCH_SIZE", "15"))
VALUES_FILE = os.environ.get("VALUES_FILE", "helm/aragog-exporters/values.yaml")

# --- Categories for realistic data ---
CATEGORIES = [
    ["Restaurant", "Italian"],
    ["Cafe", "Coffee Shop"],
    ["Hotel", "Lodging"],
    ["Bar", "Nightlife"],
    ["Fast Food", "Delivery"],
    ["Grocery", "Supermarket"],
]


def generate_org_item() -> dict:
    """Generate a synthetic organisation_data record."""
    place_id = sha1(fake.uuid4().encode()).hexdigest()[:40]
    cat = random.choice(CATEGORIES)

    item = {
        "place_id": place_id,
        "source_place_id": f"src_{fake.uuid4()[:12]}",
        "name": fake.company(),
        "categories": cat,
        "city": fake.city(),
        "country": fake.country_code(),
        "link": fake.url(),
        "reviews": random.randint(0, 5000),
        "rate": round(random.uniform(1.0, 5.0), 1),
        "lon": float(fake.longitude()),
        "lat": float(fake.latitude()),
        "address": fake.address().replace("\n", ", "),
        "phone": fake.phone_number(),
        "website": fake.url(),
        "price_range": random.choice(["$", "$$", "$$$", "$$$$", ""]),
        "opening_hrs": {"Mon": "9:00-22:00", "Tue": "9:00-22:00"},
        "additional_info": {
            "contacts": {
                "phones": [fake.phone_number()],
                "mails": [fake.email()],
            }
        },
        "email": fake.email(),
        "menu": [
            {
                "id": sha1(f"{place_id}_{i}".encode()).hexdigest()[:40],
                "place_id": place_id,
                "name": fake.word().capitalize(),
                "category": cat[0],
                "price": str(round(random.uniform(5, 50), 2)),
                "is_available": True,
            }
            for i in range(random.randint(0, 3))
        ],
    }
    return item


def generate_review_item(source_name: str) -> dict:
    """Generate a synthetic review record."""
    review_date = fake.date_time_between(start_date="-2y", end_date="now")
    return {
        "source_product_id": sha1(fake.uuid4().encode()).hexdigest()[:20],
        "review_language": random.choice(["en", "ru", "de", "fr", "ja"]),
        "review_text": fake.paragraph(nb_sentences=3),
        "original_review_text": "",
        "source_review_id": str(uuid.uuid4()),
        "url": fake.url(),
        "owner_response": random.choice(["", fake.sentence()]),
        "original_owner_response": "",
        "source": source_name,
        "user_id": str(random.randint(100000, 999999)),
        "username": fake.user_name(),
        "review_rating": str(round(random.uniform(1.0, 5.0), 1)),
        "review_location": fake.city(),
        "review_date": review_date.strftime("%Y-%m-%d %H:%M:%S"),
        "additional_info": {},
    }


def load_queues() -> list:
    """Load queue definitions from values.yaml."""
    queues = []
    try:
        with open(VALUES_FILE) as f:
            values = yaml.safe_load(f)
        for name, cfg in values.get("exporters", {}).items():
            queues.append(
                {
                    "name": name,
                    "queue": cfg["queue"],
                    "type": cfg.get("type", "organization"),
                    "source_name": cfg.get("sourceName", name),
                }
            )
    except FileNotFoundError:
        log.warning("values.yaml not found at %s, using env fallback", VALUES_FILE)
        # Fallback: minimal set for dev
        queues = [
            {
                "name": "bing-map-search",
                "queue": "bing_map_search:items",
                "type": "organization",
                "source_name": "Bing",
            },
            {
                "name": "tripadvisor-reviews",
                "queue": "tripadvisor_reviews:items",
                "type": "reviews",
                "source_name": "Tripadvisor",
            },
        ]
    return queues


def main():
    log.info("Connecting to Redis: %s", REDIS_URL.split("@")[-1] if "@" in REDIS_URL else REDIS_URL)
    r = redis.from_url(REDIS_URL)
    r.ping()

    queues = load_queues()
    log.info("Found %d queues, seeding %d items each", len(queues), SEED_BATCH)

    total = 0
    for q in queues:
        items = []
        for _ in range(SEED_BATCH):
            if q["type"] == "reviews":
                item = generate_review_item(q["source_name"])
            else:
                item = generate_org_item()
            items.append(orjson.dumps(item))

        if items:
            r.rpush(q["queue"], *items)
            total += len(items)
            log.info("  %-30s → %d items (queue: %s)", q["name"], len(items), q["queue"])

    log.info("Seeding complete: %d total items across %d queues", total, len(queues))
    r.close()


if __name__ == "__main__":
    main()
