"""Create the LEGACY source system: an on-prem PostgreSQL OLTP database.

    python src/generate_legacy_db.py                  # (re)create DB with seeded data
    python src/generate_legacy_db.py --apply-changes  # simulate a day of new activity

Why synthetic + seeded? Same numbers on every run, so reports are reproducible.

The legacy DB is deliberately "messy", like real 15-year-old systems:
  * no primary keys / foreign keys enforced  -> duplicates and orphans can exist
  * order_date stored as VARCHAR             -> mixed formats and impossible dates
  * free-text city names                     -> 'mumbai', 'MUMBAI ', ' Mumbai'
  * ~2% bad rows: duplicates, NULLs, negative quantities, bad dates, orphan FKs
Every table has `updated_at`, which our CDC (change data capture) uses.
Also writes semi-structured supplier files (CSV + JSON) into the landing zone.
"""
import argparse
import csv
import io
import json
import random
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import psycopg2
from faker import Faker

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import CONFIG, ROOT, banner  # noqa: E402

SEED = 42
N_CUSTOMERS, N_PRODUCTS, N_STORES, N_ORDERS = 2000, 200, 20, 50000
START, END = datetime(2024, 1, 1), datetime(2025, 12, 31, 23, 59)

# (city, state, region) - stores and customers live in these cities
CITIES = [
    ("Mumbai", "Maharashtra", "West"), ("Pune", "Maharashtra", "West"),
    ("Ahmedabad", "Gujarat", "West"), ("Delhi", "Delhi", "North"),
    ("Jaipur", "Rajasthan", "North"), ("Lucknow", "Uttar Pradesh", "North"),
    ("Chandigarh", "Chandigarh", "North"), ("Bengaluru", "Karnataka", "South"),
    ("Chennai", "Tamil Nadu", "South"), ("Hyderabad", "Telangana", "South"),
    ("Kochi", "Kerala", "South"), ("Kolkata", "West Bengal", "East"),
    ("Bhubaneswar", "Odisha", "East"), ("Patna", "Bihar", "East"),
    ("Guwahati", "Assam", "East"),
]
CATEGORIES = {
    "Grocery": (20, 600), "Electronics": (500, 45000), "Apparel": (300, 4000),
    "Home & Kitchen": (150, 8000), "Beauty": (100, 2500), "Sports": (250, 9000),
}
BRANDS = ["Tata", "Reliance", "Amul", "Boat", "Prestige", "Lakme", "Nivaka", "Raymond",
          "Bajaj", "Himalaya", "Decathlon", "Godrej", "Havells", "Fabindia"]

DDL = """
DROP TABLE IF EXISTS order_items, orders, customers, products, stores;
-- NOTE: no PRIMARY KEY / FOREIGN KEY constraints - typical of the legacy app.
CREATE TABLE customers (customer_id INT, first_name VARCHAR(50), last_name VARCHAR(50),
  email VARCHAR(120), phone VARCHAR(30), city VARCHAR(50), state VARCHAR(50),
  signup_date DATE, updated_at TIMESTAMP);
CREATE TABLE products (product_id INT, product_name VARCHAR(120), category VARCHAR(50),
  brand VARCHAR(50), unit_price NUMERIC(10,2), updated_at TIMESTAMP);
CREATE TABLE stores (store_id INT, store_name VARCHAR(80), city VARCHAR(50),
  region VARCHAR(20), opened_date DATE, updated_at TIMESTAMP);
CREATE TABLE orders (order_id INT, customer_id INT, store_id INT,
  order_date VARCHAR(25),           -- legacy: date kept as text!
  status VARCHAR(20), payment_method VARCHAR(20), updated_at TIMESTAMP);
CREATE TABLE order_items (order_item_id INT, order_id INT, product_id INT,
  quantity INT, unit_price NUMERIC(10,2), discount_pct NUMERIC(5,2), updated_at TIMESTAMP);
"""


def connect(dbname):
    pg = CONFIG["postgres"]
    return psycopg2.connect(host=pg["host"], port=pg["port"], user=pg["user"],
                            password=pg["password"], dbname=dbname)


def recreate_database():
    conn = connect("postgres")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {CONFIG['postgres']['database']}")
        cur.execute(f"CREATE DATABASE {CONFIG['postgres']['database']}")
    conn.close()


def copy_rows(cur, table, columns, rows):
    """Bulk-load with COPY (much faster than row-by-row INSERTs)."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    for r in rows:
        writer.writerow(["\\N" if v is None else v for v in r])
    buf.seek(0)
    cur.copy_expert(f"COPY {table} ({','.join(columns)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)


def messy_city(city, rnd):
    """~4% of free-text cities are typed inconsistently by store staff."""
    r = rnd.random()
    if r < 0.015:
        return city.lower()
    if r < 0.03:
        return city.upper() + " "
    if r < 0.04:
        return " " + city
    return city


def random_phone(rnd):
    n = f"{rnd.choice('6789')}{rnd.randint(100000000, 999999999)}"
    return rnd.choice([f"+91-{n}", f"0{n}", n, f"+91 {n[:5]} {n[5:]}"])  # 4 legacy formats


def build_customers(fake, rnd):
    rows = []
    for cid in range(1, N_CUSTOMERS + 1):
        city, state, _ = rnd.choice(CITIES)
        first, last = fake.first_name(), fake.last_name()
        email = f"{first}.{last}{cid}@{rnd.choice(['gmail.com', 'yahoo.co.in', 'outlook.com'])}".lower()
        signup = fake.date_between(date(2019, 1, 1), date(2023, 12, 31))
        rows.append([cid, first, last, email, random_phone(rnd), messy_city(city, rnd), state,
                     signup, datetime.combine(signup, datetime.min.time()) + timedelta(hours=10)])
    for r in rnd.sample(rows, 20):   # 1% missing email (legacy form allowed it)
        r[3] = None
    rows += [list(r) for r in rnd.sample(rows, 20)]  # 1% exact duplicates (buggy nightly re-insert)
    return rows


def build_products(fake, rnd):
    rows = []
    for pid in range(1, N_PRODUCTS + 1):
        cat = rnd.choice(list(CATEGORIES))
        lo, hi = CATEGORIES[cat]
        price = Decimal(str(round(rnd.uniform(lo, hi), 2)))
        name = f"{rnd.choice(BRANDS)} {fake.word().title()} {cat.split()[0]} {pid}"
        rows.append([pid, name, cat, rnd.choice(BRANDS), price, datetime(2023, 12, 1, 9)])
    rows[10][2] = None                   # missing category -> silver fills 'Unknown'
    rows[57][2] = None
    rows[99][4] = Decimal("-49.00")      # negative price -> quarantined
    return rows


def build_stores(rnd):
    rows = []
    for sid in range(1, N_STORES + 1):
        city, _, region = CITIES[(sid - 1) % len(CITIES)]
        rows.append([sid, f"RetailCo {city} #{sid}", messy_city(city, rnd), region,
                     date(2015 + sid % 8, 1 + sid % 12, 1), datetime(2023, 12, 1, 9)])
    rows[3][2], rows[16][2] = rows[3][2].upper(), rows[16][2].lower()  # guaranteed casing issues
    return rows


def order_timestamp(rnd):
    """Random order time with growth (2025 > 2024) and a Diwali (Oct/Nov) spike."""
    while True:
        ts = START + timedelta(minutes=rnd.randint(0, int((END - START).total_seconds() // 60)))
        weight = 1.0 + (0.15 if ts.year == 2025 else 0) + (0.6 if ts.month in (10, 11) else 0)
        if rnd.random() < weight / 1.75:
            return ts.replace(hour=rnd.randint(9, 21))


def build_orders_and_items(rnd, products):
    prices = {p[0]: p[4] for p in products}
    orders, items, item_id = [], [], 1
    for oid in range(1, N_ORDERS + 1):
        ts = order_timestamp(rnd)
        order_date = ts.strftime("%Y-%m-%d %H:%M:%S")
        r = rnd.random()
        if r < 0.010:
            order_date = ts.strftime("%d/%m/%Y %H:%M")        # other legacy format: fixable
        elif r < 0.013:
            order_date = rnd.choice(["00/00/0000", "2025-02-30 10:00:00", "N/A", ""])  # garbage
        elif r < 0.015:
            order_date = ts.replace(year=2031).strftime("%Y-%m-%d %H:%M:%S")      # future date
        customer = None if rnd.random() < 0.005 else rnd.randint(1, N_CUSTOMERS)
        status = rnd.choices(["DELIVERED", "DELIVERED", "DELIVERED", "SHIPPED", "CANCELLED", "RETURNED"],
                             weights=[60, 15, 10, 5, 6, 4])[0]
        orders.append([oid, customer, rnd.randint(1, N_STORES), order_date, status,
                       rnd.choice(["UPI", "UPI", "CARD", "CASH", "WALLET"]), ts + timedelta(minutes=5)])
        for _ in range(rnd.choices([1, 2, 3, 4, 5], weights=[30, 30, 20, 12, 8])[0]):
            pid = rnd.randint(1, N_PRODUCTS)
            qty = rnd.choices([1, 2, 3, 4], weights=[60, 25, 10, 5])[0]
            q = rnd.random()
            if q < 0.008:
                qty = -qty                                    # negative quantity (keying error)
            elif q < 0.010:
                pid = 9999                                    # orphan product id
            disc = rnd.choice([0, 0, 0, 5, 10, 15])
            items.append([item_id, oid, pid, qty, abs(prices.get(pid, Decimal("100.00"))),
                          Decimal(disc), ts + timedelta(minutes=5)])
            item_id += 1
    orders += [list(o) for o in rnd.sample(orders, 400)]       # ~0.8% duplicate orders
    items += [list(i) for i in rnd.sample(items, 600)]         # ~0.5% duplicate lines
    return orders, items


def write_supplier_files(rnd):
    """Semi-structured files from 2 suppliers. The JSON from March has extra/nested
    fields that the January file does not - classic schema drift."""
    landing = ROOT / "lake" / "landing" / "suppliers"
    # one folder per dataset, because a Hive/Spark table points at a folder
    (landing / "master").mkdir(parents=True, exist_ok=True)
    (landing / "deliveries").mkdir(parents=True, exist_ok=True)
    with open(landing / "master" / "supplier_master.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["supplier_id", "supplier_name", "city", "gstin", "rating"])
        for i in range(1, 16):
            city = CITIES[i - 1][0]
            w.writerow([i, f"{rnd.choice(BRANDS)} Distributors {i}", city,
                        f"27ABCDE{1000 + i}F1Z{i % 10}", round(rnd.uniform(2.5, 5), 1)])
    jan = [{"delivery_id": f"D{i:04d}", "supplier_id": rnd.randint(1, 15), "product_id": rnd.randint(1, N_PRODUCTS),
            "qty": rnd.randint(10, 500), "delivered_on": f"2025-01-{rnd.randint(1, 28):02d}"} for i in range(1, 41)]
    mar = [{"delivery_id": f"D{i:04d}", "supplier_id": rnd.randint(1, 15), "product_id": rnd.randint(1, N_PRODUCTS),
            "qty": rnd.randint(10, 500), "delivered_on": f"2025-03-{rnd.randint(1, 28):02d}",
            # NEW in March: nested object + array + an extra column
            "transport": {"mode": rnd.choice(["road", "rail"]), "vehicle_no": f"MH12AB{rnd.randint(1000, 9999)}"},
            "batch_codes": [f"B{rnd.randint(100, 999)}" for _ in range(rnd.randint(1, 3))],
            "temperature_c": round(rnd.uniform(2, 30), 1)} for i in range(41, 81)]
    for name, rows in (("deliveries_2025_01.json", jan), ("deliveries_2025_03.json", mar)):
        with open(landing / "deliveries" / name, "w") as f:
            for r in rows:                      # JSON Lines = one JSON object per line
                f.write(json.dumps(r) + "\n")


def seed():
    banner("Creating legacy PostgreSQL database 'legacy_retail' (seeded)")
    rnd, fake = random.Random(SEED), Faker("en_IN")
    Faker.seed(SEED)
    recreate_database()
    customers = build_customers(fake, rnd)
    products = build_products(fake, rnd)
    stores = build_stores(rnd)
    orders, items = build_orders_and_items(rnd, products)
    conn = connect(CONFIG["postgres"]["database"])
    with conn, conn.cursor() as cur:
        cur.execute(DDL)
        copy_rows(cur, "customers", ["customer_id", "first_name", "last_name", "email", "phone", "city",
                                     "state", "signup_date", "updated_at"], customers)
        copy_rows(cur, "products", ["product_id", "product_name", "category", "brand", "unit_price",
                                    "updated_at"], products)
        copy_rows(cur, "stores", ["store_id", "store_name", "city", "region", "opened_date", "updated_at"], stores)
        copy_rows(cur, "orders", ["order_id", "customer_id", "store_id", "order_date", "status",
                                  "payment_method", "updated_at"], orders)
        copy_rows(cur, "order_items", ["order_item_id", "order_id", "product_id", "quantity", "unit_price",
                                       "discount_pct", "updated_at"], items)
    conn.close()
    write_supplier_files(rnd)
    for t, rows in [("customers", customers), ("products", products), ("stores", stores),
                    ("orders", orders), ("order_items", items)]:
        print(f"  {t:<12} {len(rows):>7,} rows")
    print("  supplier files written to lake/landing/suppliers/")


def apply_changes():
    """Simulate new activity in the legacy app AFTER the first migration load.
    The incremental (CDC) load should pick up exactly these rows."""
    banner("Applying new activity to legacy DB (for the CDC demo)")
    rnd = random.Random(7)
    now = datetime.now().replace(microsecond=0)
    conn = connect(CONFIG["postgres"]["database"])
    with conn, conn.cursor() as cur:
        # 1) 25 customers moved city -> SCD Type 2 should keep both versions
        movers = rnd.sample(range(1, 201), 25)
        for cid in movers:
            city, state, _ = rnd.choice(CITIES)
            cur.execute("UPDATE customers SET city=%s, state=%s, updated_at=%s WHERE customer_id=%s",
                        (city, state, now, cid))
        # 2) price change on 10 products
        cur.execute("UPDATE products SET unit_price = round(unit_price*1.05, 2), updated_at=%s "
                    "WHERE product_id BETWEEN 1 AND 10", (now,))
        # 3) 10 brand-new customers
        cur.execute("SELECT max(customer_id) FROM customers")
        next_cid = cur.fetchone()[0] + 1
        for i in range(10):
            city, state, _ = rnd.choice(CITIES)
            cur.execute("INSERT INTO customers VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (next_cid + i, f"New{i}", "Customer", f"new{i}.customer@gmail.com",
                         f"+91-98{rnd.randint(10000000, 99999999)}", city, state, now.date(), now))
        # 4) 200 new orders (with items) placed today
        cur.execute("SELECT (SELECT max(order_id) FROM orders), (SELECT max(order_item_id) FROM order_items)")
        next_oid, next_item = (x + 1 for x in cur.fetchone())
        for i in range(200):
            oid = next_oid + i
            cur.execute("INSERT INTO orders VALUES (%s,%s,%s,%s,%s,%s,%s)",
                        (oid, rnd.choice(movers + [next_cid + j for j in range(10)]), rnd.randint(1, N_STORES),
                         now.strftime("%Y-%m-%d %H:%M:%S"), "DELIVERED", "UPI", now))
            for _ in range(2):
                pid = rnd.randint(1, N_PRODUCTS)
                cur.execute("INSERT INTO order_items SELECT %s,%s,%s,%s,abs(unit_price),0,%s FROM products "
                            "WHERE product_id=%s", (next_item, oid, pid, rnd.randint(1, 3), now, pid))
                next_item += 1
        # 5) 50 old orders get a status change (e.g. returned)
        cur.execute("UPDATE orders SET status='RETURNED', updated_at=%s WHERE order_id IN "
                    "(SELECT order_id FROM orders WHERE status='DELIVERED' ORDER BY order_id LIMIT 50)", (now,))
    conn.close()
    print(f"  25 customers moved city, 10 price changes, 10 new customers, 200 new orders, "
          f"50 status updates  (updated_at = {now})")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply-changes", action="store_true", help="simulate new activity for CDC")
    args = ap.parse_args()
    apply_changes() if args.apply_changes else seed()
