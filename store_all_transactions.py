import time
import api_wrapper
import sqlite3
import json
from set_up_db import setup, conn_new as conn, bulk_insert_transactions

setup(conn)

# You can keep or remove manual CREATE TABLE IF NOT EXISTS if needed

print("starting main loop")
uuid_to_id = {}
item_key_to_id = {}
item_keys = {}
loop = 0

while True:
    loop += 1
    now = time.time()
    count, cache = api_wrapper.update_transaction_cache()
    if count == 0:
        loop -= 1
        time.sleep(1/250 * 10)
        continue

    if loop < 10 or loop % 10000 == 0 or loop in [10, 100, 1000] or count > 80:
        print(
            f"Processing {count} new transactions...at {now:.4f} seconds, fetching took {time.time()-now:.4f} seconds")

    rows = []
    for i in range(count):
        data = cache[i]
        item = data["item"]
        row = {
            'price': data["price"],
            'time': data["unixMillisDateSold"],
            'seller_name': data["seller"]["name"],
            'seller_uuid': data["seller"]["uuid"],
            'item_identifier': item["id"],
            'item_count': item["count"],
            'item_display_name': item["display_name"],
            'item_enchants': json.dumps(item["enchants"]),
            'item_lore': json.dumps(item.get("lore")),
            'item_trim': json.dumps(item.get("enchants", {}).get("trim", {})),
            'item_contents': json.dumps(item.get("contents"))
        }
        rows.append(row)

    # Bulk insert transactions and related mappings
    bulk_insert_transactions(conn, rows)

    if loop < 10 or loop % 10000 == 0 or loop in [10, 100, 1000] or count > 80:
        print(
            f"Loop {loop} completed in {time.time()-now:.4f} seconds, inserted {count} transactions.")

conn.close()
