import time
import api_wrapper
import sqlite3
import json
conn = sqlite3.connect("all_transactions.db")
cur = conn.cursor()


cur.execute('''
CREATE TABLE IF NOT EXISTS seller (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    seller_name TEXT,
    uuid TEXT UNIQUE
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS item_map (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id TEXT,
    count INTEGER,
    display_name TEXT,
    enchants TEXT,
    lore TEXT,
    trim TEXT,
    contents TEXT,
    UNIQUE (item_id, count, display_name, enchants, lore, trim, contents)
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    price INTEGER,
    time INTEGER,
    seller_id INTEGER,
    item_id INTEGER,
    FOREIGN KEY (seller_id) REFERENCES seller(id),
    FOREIGN KEY (item_id) REFERENCES item_map(id)
)
''')
print("starting main loop")
uuid_to_id = {}
item_key_to_id = {}
item_keys = {}
while True:
    count, cache = api_wrapper.update_transaction_cache()
    if count == 0:
        continue
    print(f"{count}-items")
    now = time.time()
    for i in range(count):
        data = cache[i]
        if data["seller"]["uuid"] in uuid_to_id:
            continue
        cur.execute('''
        INSERT OR IGNORE INTO seller (seller_name, uuid)
        VALUES (?, ?)
        ''', (data["seller"]["name"], data["seller"]["uuid"]))
    conn.commit()

    for i in range(count):
        data = cache[i]
        # Get seller ID
        if data["seller"]["uuid"] in uuid_to_id:
            continue
        cur.execute('SELECT id FROM seller WHERE uuid = ?',
                    (data["seller"]["uuid"],))
        seller_id = cur.fetchone()[0]
        uuid_to_id[data["seller"]["uuid"]] = seller_id

    for i in range(count):
        data = cache[i]
        # --- Insert item
        item = data["item"]
        lore = json.dumps(item.get("lore"))
        trim = json.dumps(item.get("enchants", {}).get("trim", {}))
        contents = json.dumps(item.get("contents"))

        item_key = (
            item["id"],
            item["count"],
            item["display_name"],
            json.dumps(item["enchants"]),
            lore,
            trim,
            contents
        )
        item_keys[i] = item_key
        if item_key in item_key_to_id:
            continue
        cur.execute('''
        INSERT OR IGNORE INTO item_map (
            item_id, count, display_name, enchants, lore, trim, contents
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', item_key)
    conn.commit()

    for i in range(count):
        data = cache[i]
        # Get item ID
        if item_keys[i] in item_key_to_id:
            continue
        cur.execute('''
        SELECT id FROM item_map
        WHERE item_id = ? AND count = ? AND display_name = ? AND enchants = ? AND lore = ? AND
                    trim = ? AND contents = ?
        ''', item_keys[i])
        item_id = cur.fetchone()[0]
        item_key_to_id[item_keys[i]] = item_id

    for i in range(count):
        data = cache[i]
        # --- Insert transaction
        cur.execute('''
        INSERT OR REPLACE INTO transactions ( price, time, seller_id, item_id)
        VALUES ( ?, ?, ?, ?)
        ''', (data["price"], data["unixMillisDateSold"], uuid_to_id[data["seller"]["uuid"]], item_key_to_id[item_keys[i]]))
    conn.commit()

    print(time.time()-now, count)

conn.close()
