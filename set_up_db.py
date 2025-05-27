import subprocess
import time
import itertools
import sqlite3
import json


def batches(conn, table_name, batch_size, start_with=0, time_wait=0.1):
    cur = conn.cursor()
    cur.execute(f'''Select Max(id) from {table_name}''')
    max_id = cur.fetchone()[0]
    if max_id is None:
        return
    for batch_start in range(start_with, max_id+2, batch_size):
        first_after_batch_end = min(batch_start+batch_size, max_id+1)
        yield batch_start, first_after_batch_end, f"id >= {batch_start} AND id < {first_after_batch_end}"
        time.sleep(time_wait)
    return


def start_dynamic_batching(conn, table):
    ref = [0]

    def keep_going():
        for min_id, max_id_excl, cond in batches(conn, table, 5000, ref[0]):
            yield min_id, max_id_excl, cond
            ref[0] = max_id_excl
    return keep_going
# setpoint for synchronising!!!


def setup(conn: sqlite3.Connection):
    cursor = conn.cursor()
    with conn:
        cursor.executescript("""
            Create Table if not exists price_seller_item_mapping(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                price_id INTEGER,
                seller_id INTEGER,
                item_id INTEGER
            );
            CREATE TABLE IF NOT EXISTS transaction_compressed (
                id INTEGER PRIMARY KEY,
                time INTEGER,
                price_seller_item_id INTEGER
            );

            -- Join transaction_compressed with price_seller_item_mapping for a view
            DROP VIEW IF EXISTS transactions;
            CREATE VIEW IF NOT EXISTS transactions AS
            SELECT
            tc.id AS id,
            tc.time AS time,
            psim.price_id as price_id,
            psim.seller_id as seller_id,
            psim.item_id as item_id
            FROM transaction_compressed tc
            JOIN price_seller_item_mapping psim ON tc.price_seller_item_id = psim.id;

            CREATE TABLE IF NOT EXISTS offers_compressed (
                id INTEGER PRIMARY KEY,
                time_left INTEGER,
                ends_at_min INTEGER,
                delta_t INTEGER,
                price_seller_item_id INTEGER
            );
            Drop view If Exists offers;
            CREATE VIEW IF NOT EXISTS offers AS
            SELECT
                oc.id as id,
                oc.time_left as time_left,
                oc.ends_at_min as ends_at_min,
                oc.ends_at_min + oc.delta_t as ends_at_max,
                psim.price_id as price_id,
                psim.seller_id as seller_id,
                psim.item_id as item_id
            FROM offers_compressed oc
            JOIN price_seller_item_mapping psim ON oc.price_seller_item_id = psim.id;

            CREATE TABLE IF NOT EXISTS price_map (
                id INTEGER PRIMARY KEY,
                price INTEGER
            );

            CREATE TABLE IF NOT EXISTS seller_map (
                id INTEGER PRIMARY KEY,
                seller_name TEXT,
                seller_uuid TEXT
            );

            CREATE TABLE IF NOT EXISTS item_identifier_map (
                id INTEGER PRIMARY KEY,
                text TEXT
            );

            CREATE TABLE IF NOT EXISTS item_display_name_map (
                id INTEGER PRIMARY KEY,
                text TEXT
            );

            CREATE TABLE IF NOT EXISTS item_enchantment_map (
                id INTEGER PRIMARY KEY,
                text TEXT
            );

            CREATE TABLE IF NOT EXISTS item_lore_map (
                id INTEGER PRIMARY KEY,
                text TEXT
            );

            CREATE TABLE IF NOT EXISTS item_trim_map (
                id INTEGER PRIMARY KEY,
                text TEXT
            );


            CREATE TABLE IF NOT EXISTS content_item_map (
                id INTEGER PRIMARY KEY,
                item_identifier_id INTEGER,
                item_count INTEGER,
                item_display_name_id INTEGER,
                item_enchantment_id INTEGER,
                item_trim_id INTEGER
            );

            CREATE TABLE IF NOT EXISTS item_contents_map (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                container_id INTEGER,
                content_item_id INTEGER
            );

            CREATE TABLE IF NOT EXISTS item_map (
                id INTEGER PRIMARY KEY,
                item_identifier_id INTEGER,
                item_count INTEGER,
                item_display_name_id INTEGER,
                item_enchantment_id INTEGER,
                item_lore_id INTEGER,
                item_trim_id INTEGER,
                item_contents_id INTEGER
            );

            -- Views
            CREATE VIEW If not exists content_json_view AS
            SELECT
                icm.container_id,
                '[' || GROUP_CONCAT(
                    '{"id":"' || iid.text || '"' ||
                    ',"count":' || ci.item_count ||
                    ',"display_name":' || COALESCE('"' || dname.text || '"', '""') ||
                    ',"enchants": { "enchantments": ' || COALESCE(ench.text, 'null') ||
                    ',"trim":' || COALESCE(trim.text, '{"material": "", "pattern": ""}') ||
                    '}}'
                , ',') || ']' AS item_contents_json
            FROM item_contents_map icm
            JOIN content_item_map ci ON icm.content_item_id = ci.id
            JOIN item_identifier_map iid ON ci.item_identifier_id = iid.id
            LEFT JOIN item_display_name_map dname ON ci.item_display_name_id = dname.id
            LEFT JOIN item_enchantment_map ench ON ci.item_enchantment_id = ench.id
            LEFT JOIN item_trim_map trim ON ci.item_trim_id = trim.id
            GROUP BY icm.container_id

            UNION ALL

            SELECT
                0 AS container_id,
                'null' AS item_contents_json;

            Drop View If Exists t_view;
            CREATE VIEW IF NOT EXISTS t_view AS
            SELECT
                t.id,
                pm.price,
                t.time,
                sm.seller_name,
                sm.seller_uuid,
                idmap.text AS item_identifier,
                im.item_count,
                dname.text AS item_display_name,
                ench.text AS item_enchantment,
                lore.text AS item_lore,
                trim.text AS item_trim,
                COALESCE(cjv.item_contents_json, 'null') as item_contents

            FROM transactions t
            JOIN price_map pm ON t.price_id = pm.id
            JOIN seller_map sm ON t.seller_id = sm.id
            JOIN item_map im ON t.item_id = im.id
            JOIN item_identifier_map idmap ON im.item_identifier_id = idmap.id
            LEFT JOIN item_display_name_map dname ON im.item_display_name_id = dname.id
            LEFT JOIN item_enchantment_map ench ON im.item_enchantment_id = ench.id
            LEFT JOIN item_lore_map lore ON im.item_lore_id = lore.id
            LEFT JOIN item_trim_map trim ON im.item_trim_id = trim.id
            LEFT JOIN content_json_view cjv ON cjv.container_id = im.id;

            Drop View If Exists o_view;
            CREATE VIEW IF NOT EXISTS o_view AS
            SELECT
                o.id,
                pm.price,
                o.time_left,
                o.ends_at_min,
                o.ends_at_max as ends_at_max,
                sm.seller_name,
                sm.seller_uuid,
                idmap.text AS item_identifier,
                im.item_count,
                dname.text AS item_display_name,
                ench.text AS item_enchantment,
                lore.text AS item_lore,
                trim.text AS item_trim,
                COALESCE(cjv.item_contents_json, 'null') as item_contents

            FROM offers o
            JOIN price_map pm ON o.price_id = pm.id
            JOIN seller_map sm ON o.seller_id = sm.id
            JOIN item_map im ON o.item_id = im.id
            JOIN item_identifier_map idmap ON im.item_identifier_id = idmap.id
            LEFT JOIN item_display_name_map dname ON im.item_display_name_id = dname.id
            LEFT JOIN item_enchantment_map ench ON im.item_enchantment_id = ench.id
            LEFT JOIN item_lore_map lore ON im.item_lore_id = lore.id
            LEFT JOIN item_trim_map trim ON im.item_trim_id = trim.id
            LEFT JOIN content_json_view cjv ON cjv.container_id = im.id;


        """)
    conn.commit()


def bulk_insert_and_get_ids(conn, table, columns, values, select_batch_size=300):
    """
    columns: list of column names (1 or more)
    values: list of tuples matching columns, or list of single values if columns is length 1
    select_batch_size: max number of rows to query at once (to avoid SQLite max variables limit)

    Returns list of ids for each input row
    """
    if not hasattr(bulk_insert_and_get_ids, "_cache"):
        bulk_insert_and_get_ids._cache = {}

    cache = bulk_insert_and_get_ids._cache.setdefault(table, {})

    # Normalize values to list of tuples (even for single column)
    if len(columns) == 1:
        values = [(v,) if not isinstance(v, tuple) else v for v in values]

    cur = conn.cursor()

    # Find missing entries (not cached yet)
    missing = list({v for v in values if v not in cache})
    if missing:

        # Batch-select missing rows to update cache
        def chunks(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i:i + n]
        for batch in chunks(missing, select_batch_size):
            where_placeholders = ", ".join(
                "(" + ", ".join("?" for _ in columns) + ")" for _ in batch
            )
            sql_select = f"SELECT id, {', '.join(columns)} FROM {table} WHERE ({', '.join(columns)}) IN ({where_placeholders})"

            flattened_batch = [item for tup in batch for item in tup]
            cur.execute(sql_select, flattened_batch)
            rows = cur.fetchall()

            for row in rows:
                row_id = row[0]
                row_key = tuple(row[1:])
                cache[row_key] = row_id

        missing = list({v for v in values if v not in cache})
    if missing:
        placeholders = "(" + ", ".join("?" for _ in columns) + ")"
        sql_insert = f"INSERT OR IGNORE INTO {table} ({', '.join(columns)}) VALUES {placeholders}"

        # Insert missing values using executemany
        cur.executemany(sql_insert, missing)
        conn.commit()

        for batch in chunks(missing, select_batch_size):
            where_placeholders = ", ".join(
                "(" + ", ".join("?" for _ in columns) + ")" for _ in batch
            )
            sql_select = f"SELECT id, {', '.join(columns)} FROM {table} WHERE ({', '.join(columns)}) IN ({where_placeholders})"

            flattened_batch = [item for tup in batch for item in tup]
            cur.execute(sql_select, flattened_batch)
            rows = cur.fetchall()

            for row in rows:
                row_id = row[0]
                row_key = tuple(row[1:])
                cache[row_key] = row_id

    # Return ids for all input values (cached now)
    return [cache[v] for v in values]


def bulk_insert_into_many_to_many_mapping(conn, table, container_column, content_column, values):
    """
    General function to deduplicate and insert many-to-many container-to-content mappings.

    Args:
        conn: sqlite3 connection
        table: name of the mapping table (e.g., 'item_contents_map')
        container_column: name of the container ID column (e.g., 'container_id')
        content_column: name of the content ID column (e.g., 'content_item_id')
        values: list of lists of content IDs (each list defines one container)
        batch_size: how many values to fetch at once (to avoid SQLite variable limits)

    Returns:
        List of container IDs corresponding to the input `values`.
    """
    cursor = conn.cursor()

    # Step 1: Load all existing container -> set(content) mappings
    cursor.execute(f"SELECT {container_column}, {content_column} FROM {table}")
    container_map = {}
    for container_id, content_id in cursor.fetchall():
        container_map.setdefault(container_id, set()).add(content_id)

    container_map[0] = set()

    # Step 2: Normalize and match or insert
    result_container_ids = []

    for content_list in values:
        content_set = set(content_list)
        matched_id = None

        # Linear search for matching content set
        for cid, existing in container_map.items():
            if existing == content_set:
                matched_id = cid
                break

        if matched_id is not None:
            result_container_ids.append(matched_id)
            continue

        new_container_id = max(container_map.keys()) + 1

        # Insert actual content items
        to_insert = [(new_container_id, cid) for cid in content_list]
        cursor.executemany(
            f"INSERT INTO {table} ({container_column}, {content_column}) VALUES (?, ?)",
            to_insert
        )
        conn.commit()

        # Cache new mapping
        container_map[new_container_id] = content_set
        result_container_ids.append(new_container_id)

    return result_container_ids


def bulk_insert_content_items_return_ids(conn, content_rows):
    """
    content_rows: list of dicts with raw text keys:
        id (str),
        count (int),
        enchants:
            display_name (str or None),
            enchantments (str or None),
        trim (str or None)
    """
    item_identifiers = [(row['id'],) for row in content_rows]
    item_display_names = [(row.get('display_name'),)
                          for row in content_rows]
    item_enchantments = [json.dumps(row.get('enchants').get('enchantments'),)
                         for row in content_rows]
    item_trims = [json.dumps(row.get('enchants').get('trim'),)
                  for row in content_rows]
    item_counts = [row['count'] for row in content_rows]
    item_identifier_ids = bulk_insert_and_get_ids(
        conn, "item_identifier_map", ["text"], item_identifiers)
    item_display_name_ids = bulk_insert_and_get_ids(
        conn, "item_display_name_map", ["text"], item_display_names)
    item_enchantment_ids = bulk_insert_and_get_ids(
        conn, "item_enchantment_map", ["text"], item_enchantments)
    item_trim_ids = bulk_insert_and_get_ids(
        conn, "item_trim_map", ["text"], item_trims)

    content_item_tuples = []
    for i in range(len(content_rows)):
        content_item_tuples.append((
            item_identifier_ids[i],
            item_counts[i],
            item_display_name_ids[i],
            item_enchantment_ids[i],
            item_trim_ids[i]
        ))

    content_item_ids = bulk_insert_and_get_ids(
        conn,
        "content_item_map",
        ["item_identifier_id", "item_count", "item_display_name_id",
            "item_enchantment_id", "item_trim_id"],
        content_item_tuples
    )
    return content_item_ids


def bulk_insert_items(conn, item_rows):
    """
    item_rows: list of dicts with keys:
        item_identifier (str),
        item_count (int),
        item_display_name (str or None),
        item_enchantment (str or None),
        item_lore (str or None),        # for item_map only
        item_trim (str or None),
        content_items json_string(list of dicts) - each dict with keys:
            item_identifier (str),
            item_count (int),
            item_display_name (str or None),
            item_enchantment (str or None),
            item_trim (str or None)
    """
    item_identifiers = [(row['item_identifier'],) for row in item_rows]
    item_counts = [row['item_count'] for row in item_rows]
    item_display_names = [(row.get('item_display_name'),) for row in item_rows]
    item_enchantment = [(row.get('item_enchantment'),) for row in item_rows]
    item_lores = [(row.get('item_lore'),) for row in item_rows]
    item_trims = [(row.get('item_trim'),) for row in item_rows]

    # Parse and flatten all content items
    all_content_rows = []
    container_index_to_content_items = []

    for row in item_rows:
        raw_json = row.get('content_items', '[]')
        content_list = json.loads(raw_json)
        if content_list == None:
            content_list = []
        container_index_to_content_items.append(content_list)
        all_content_rows.extend(content_list)

    # Insert content items (deduplicated)
    content_item_ids = bulk_insert_content_items_return_ids(
        conn, all_content_rows)

    # Group content_item_ids back into container-wise lists
    grouped_content_ids = []
    idx = 0
    for content_list in container_index_to_content_items:
        length = len(content_list)
        grouped_content_ids.append(content_item_ids[idx:idx+length])
        idx += length

    # Insert containers and get their container_id
    container_ids = bulk_insert_into_many_to_many_mapping(
        conn,
        table="item_contents_map",
        container_column="container_id",
        content_column="content_item_id",
        values=grouped_content_ids
    )

    # Insert remaining subcomponents
    item_identifier_ids = bulk_insert_and_get_ids(
        conn, "item_identifier_map", ["text"], item_identifiers)
    item_display_name_ids = bulk_insert_and_get_ids(
        conn, "item_display_name_map", ["text"], item_display_names)
    item_enchantment_ids = bulk_insert_and_get_ids(
        conn, "item_enchantment_map", ["text"], item_enchantment)
    item_lore_ids = bulk_insert_and_get_ids(
        conn, "item_lore_map", ["text"], item_lores)
    item_trim_ids = bulk_insert_and_get_ids(
        conn, "item_trim_map", ["text"], item_trims)

    # Compose final item tuples
    item_tuples = []
    for i in range(len(item_rows)):
        item_tuples.append((
            item_identifier_ids[i],
            item_counts[i],
            item_display_name_ids[i],
            item_enchantment_ids[i],
            item_lore_ids[i],
            item_trim_ids[i],
            container_ids[i]
        ))

    # Insert final item_map rows and return their IDs
    item_ids = bulk_insert_and_get_ids(
        conn,
        "item_map",
        ["item_identifier_id", "item_count", "item_display_name_id",
         "item_enchantment_id", "item_lore_id", "item_trim_id", "item_contents_id"],
        item_tuples
    )

    return item_ids


def bulk_insert_transactions(conn, rows):
    """
    rows: list of dicts with keys:
      price, time, seller_name, seller_uuid,
      item_identifier, item_count,
      item_display_name, item_enchantment, item_lore, item_trim, item_contents
    """
    cursor = conn.cursor()

    # Prepare data to insert/get IDs
    prices = [(row['price'],) for row in rows]
    sellers = [(row['seller_name'], row['seller_uuid']) for row in rows]
    item_rows = []
    for row in rows:
        item_rows.append({
            'item_identifier': row['item_identifier'],
            'item_count': row['item_count'],
            'item_display_name': row['item_display_name'],
            'item_enchantment': row['item_enchantment'],
            'item_lore': row['item_lore'],
            'item_trim': row['item_trim'],
            'content_items': row['item_contents'],  # JSON string
        })

    with conn:
        price_ids = bulk_insert_and_get_ids(
            conn, "price_map", ["price"], prices)
        seller_ids = bulk_insert_and_get_ids(conn, "seller_map", [
            "seller_name", "seller_uuid"], sellers)
        item_ids = bulk_insert_items(conn, item_rows)

        # Prepare psim tuples
        psim_tuples = []
        for i in range(len(rows)):
            psim_tuples.append((
                price_ids[i],
                seller_ids[i],
                item_ids[i]
            ))
        # Insert into price_seller_item_mapping
        psim_ids = bulk_insert_and_get_ids(
            conn, "price_seller_item_mapping", ["price_id", "seller_id", "item_id"], psim_tuples)
        # Insert transactions
        trans_tuples = []
        for i, row in enumerate(rows):
            trans_tuples.append((
                row['time'],
                psim_ids[i]
            ))
        # Insert into transaction_compressed
        cursor.executemany(
            "INSERT INTO transaction_compressed (time, price_seller_item_id) VALUES (?, ?)",
            trans_tuples
        )


def bulk_insert_offers(conn, rows):
    """
    rows: list of dicts with keys:
      price, time_left, ends_at_min, ends_at_max, seller_name, seller_uuid,
      item_identifier, item_count,
      item_display_name, item_enchantment, item_lore, item_trim, item_contents
    """
    cursor = conn.cursor()

    # Prepare prices and sellers
    prices = [(row['price'],) for row in rows]
    sellers = [(row['seller_name'], row['seller_uuid']) for row in rows]

    # Prepare item_rows for bulk_insert_items
    item_rows = []
    for row in rows:
        item_rows.append({
            'item_identifier': row['item_identifier'],
            'item_count': row['item_count'],
            'item_display_name': row['item_display_name'],
            'item_enchantment': row['item_enchantment'],
            'item_lore': row['item_lore'],
            'item_trim': row['item_trim'],
            'content_items': row['item_contents'],  # JSON string
        })

    with conn:
        price_ids = bulk_insert_and_get_ids(
            conn, "price_map", ["price"], prices)
        seller_ids = bulk_insert_and_get_ids(
            conn, "seller_map", ["seller_name", "seller_uuid"], sellers)
        item_ids = bulk_insert_items(conn, item_rows)

        # Prepare price_seller_item_mapping tuples
        psim_tuples = []
        for i in range(len(rows)):
            psim_tuples.append((
                price_ids[i],
                seller_ids[i],
                item_ids[i]
            ))
        # Insert into price_seller_item_mapping
        psim_ids = bulk_insert_and_get_ids(
            conn, "price_seller_item_mapping", ["price_id", "seller_id", "item_id"], psim_tuples)
        # Prepare offer_tuples
        offer_tuples = []
        for i, row in enumerate(rows):
            offer_tuples.append((
                psim_ids[i],
                row['time_left'],
                row['ends_at_min'],
                row['ends_at_max']-row['ends_at_min']  # delta_t
            ))
        # Insert into offers_compressed
        cursor.executemany(
            "INSERT INTO offers_compressed (price_seller_item_id, time_left, ends_at_min, delta_t) VALUES (?, ?, ?, ?)",
            offer_tuples
        )


def batch_fetch_t_view(conn):
    cursor = conn.cursor()
    columns = [
        "id",
        "price",
        "time",
        "seller_name",
        "seller_uuid",
        "item_identifier",
        "item_count",
        "item_display_name",
        "item_enchantment",
        "item_lore",
        "item_trim",
        "item_contents"
    ]

    batcher = start_dynamic_batching(conn, "t_view")  # Use the view name here

    def keep_going():
        for _, _, cond in batcher():
            query = f"""
            SELECT {', '.join(columns)}
            FROM t_view
            WHERE {cond.replace("id", "id")}  -- id is directly from t_view
            ORDER BY id
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            if rows:
                batch = [
                    dict(zip(columns, row))
                    for row in rows
                ]
                yield batch

    return keep_going


def batch_fetch_o_view(conn):
    cursor = conn.cursor()
    columns = [
        "id",
        "price",
        "time_left",
        "ends_at_min",
        "ends_at_max",
        "seller_name",
        "seller_uuid",
        "item_identifier",
        "item_count",
        "item_display_name",
        "item_enchantment",
        "item_lore",
        "item_trim",
        "item_contents"
    ]

    batcher = start_dynamic_batching(conn, "o_view")  # Use the offers view

    def keep_going():
        for _, _, cond in batcher():
            query = f"""
            SELECT {', '.join(columns)}
            FROM o_view
            WHERE {cond.replace("id", "id")}  -- id is from o_view
            ORDER BY id
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            if rows:
                batch = [
                    dict(zip(columns, row))
                    for row in rows
                ]
                yield batch

    return keep_going


def batch_fetch_old_t_view(conn):
    cursor = conn.cursor()
    batcher = start_dynamic_batching(conn, "transactions")

    def keep_going():
        for _, _, cond in batcher():
            print(f"Fetching transactions with condition: {cond}")
            query = f"""
            SELECT
                t.id,
                t.price,
                t.time,
                s.seller_name,
                s.uuid AS seller_uuid,
                i.item_id AS item_identifier,
                i.count AS item_count,
                i.display_name AS item_display_name,
                i.enchants AS item_enchants,
                i.lore AS item_lore,
                i.trim AS item_trim,
                i.contents AS item_contents
            FROM transactions t
            JOIN seller s ON t.seller_id = s.id
            JOIN item_map i ON t.item_id = i.id
            WHERE {cond.replace("id", "t.id")}
            ORDER BY t.id
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            if rows:
                columns = [desc[0] for desc in cursor.description]
                res = [dict(zip(columns, row)) for row in rows]
                for item in res:
                    item["item_enchantment"] = json.dumps(
                        json.loads(item["item_enchants"])["enchantments"])
                yield res

    return keep_going


def batch_fetch_old_o_view(conn):
    cursor = conn.cursor()
    batcher = start_dynamic_batching(conn, "offers")

    def keep_going():
        for _, _, cond in batcher():
            print(f"Fetching offers with condition: {cond}")
            query = f"""
            SELECT
                o.id,
                o.price,
                o.time_left,
                o.ends_at_min,
                o.ends_at_max,
                s.seller_name,
                s.uuid AS seller_uuid,
                i.item_id AS item_identifier,
                i.count AS item_count,
                i.display_name AS item_display_name,
                i.enchants AS item_enchants,
                i.lore AS item_lore,
                i.trim AS item_trim,
                i.contents AS item_contents
            FROM offers o
            JOIN seller s ON o.seller_id = s.id
            JOIN item_map i ON o.item_id = i.id
            WHERE {cond.replace("id", "o.id")}
            ORDER BY o.id
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            if rows:
                columns = [desc[0] for desc in cursor.description]
                res = [dict(zip(columns, row)) for row in rows]
                for item in res:
                    item["item_enchantment"] = json.dumps(
                        json.loads(item["item_enchants"])["enchantments"])
                yield res
    return keep_going


def copy_db(con1, con2):
    it_t = batch_fetch_t_view(con1)
    it_o = batch_fetch_o_view(con1)
    for _ in range(100):
        for batch in it_t():
            bulk_insert_transactions(con2, batch)
        for batch in it_o():
            bulk_insert_offers(con2, batch)


def copy_db_from_old(con1, con2):
    cur_old = con1.cursor()
    for column, table in [("item_id", "item_identifier_map"), ("display_name", "item_display_name_map"), ("enchants", "item_enchantment_map"), ("lore", "item_lore_map"), ("trim", "item_trim_map")]:
        cur_old.execute(f'''
            SELECT count(*), {column} FROM item_map group by {column} order by count(*) desc
        ''')
        for_bulk_add = []
        for row in cur_old.fetchall():
            if row[1] is None:
                continue
            for_bulk_add.append((row[1],))
        print(
            f"Inserting {len(for_bulk_add)} unique {column} values into {table}")
        bulk_insert_and_get_ids(con2, table, ["text"], for_bulk_add)

    cur_old.execute('''
        SELECT count(*), price FROM transactions group by price order by count(*) desc
    ''')
    for_bulk_add = []
    for row in cur_old.fetchall():
        if row[1] is None:
            continue
        for_bulk_add.append((row[1],))
    print(f"Inserting {len(for_bulk_add)} unique price values into price_map")
    bulk_insert_and_get_ids(con2, "price_map", ["price"], for_bulk_add)

    cur_old.execute('''
        SELECT
            COUNT(*), i.id, i.item_id, i.count, i.display_name,i.enchants, i.lore, i.trim, i.contents
        FROM transactions t
        JOIN seller s ON t.seller_id = s.id
        JOIN item_map i ON t.item_id = i.id
        Group By i.id
        ORDER BY COUNT(*) DESC
    ''')
    for_bulk_add = []
    for row in cur_old.fetchall():
        res = {
            "item_identifier": row[2],  # item_id
            "item_count": row[3],  # count
            "item_display_name": row[4],  # display_name
            # enchants
            "item_enchantment": json.dumps(json.loads(row[5])["enchantments"]),
            "item_lore": row[6],  # lore
            "item_trim": row[7],  # trim
            "item_contents": row[8]  # contents
        }
        for_bulk_add.append(res)
    print(len(for_bulk_add))
    cur_old.execute('''
        SELECT
            COUNT(*), s.id, s.seller_name, s.uuid AS seller_uuid
        FROM transactions t
        JOIN seller s ON t.seller_id = s.id
        JOIN item_map i ON t.item_id = i.id
        Group By s.id
        ORDER BY COUNT(*) DESC
    ''')
    for_bulk_add = []
    for row in cur_old.fetchall():
        res = (row[2], row[3])  # seller_name, seller_uuid
        for_bulk_add.append(res)
    print(len(for_bulk_add))
    bulk_insert_and_get_ids(conn_new, "seller_map", [
                            "seller_name", "seller_uuid"], for_bulk_add)

    query = f"""
    SELECT
        count(*),
        t.id,
        t.price,
        s.seller_name,
        s.uuid AS seller_uuid,
        i.item_id AS item_identifier,
        i.count AS item_count,
        i.display_name AS item_display_name,
        i.enchants AS item_enchants,
        i.lore AS item_lore,
        i.trim AS item_trim,
        i.contents AS item_contents
    FROM transactions t
    JOIN seller s ON t.seller_id = s.id
    JOIN item_map i ON t.item_id = i.id
    Group By i.id,s.id,price
    ORDER BY count(*)
    """
    cursor = con1.cursor()
    cursor.execute(query)
    for_bulk_add = []
    for row in cursor.fetchall():
        res = {
            "price": row[2],
            "time": row[0],  # count(*)
            "seller_name": row[3],
            "seller_uuid": row[4],
            "item_identifier": row[5],
            "item_count": row[6],
            "item_display_name": row[7],
            "item_enchantment": json.dumps(json.loads(row[8])["enchantments"]),
            "item_lore": row[9],
            "item_trim": row[10],
            "item_contents": row[11]
        }
        for_bulk_add.append(res)
    print(
        f"Inserting {len(for_bulk_add)} transactions into transaction_compressed")
    bulk_insert_transactions(con2, for_bulk_add)
    # Delete all transactions again
    cur_new = con2.cursor()
    cur_new.execute("DELETE FROM transaction_compressed")
    cur_new.execute(
        "DELETE FROM sqlite_sequence WHERE name='transaction_compressed'")
    con2.commit()

    it_t = batch_fetch_old_t_view(con1)
    it_o = batch_fetch_old_o_view(con1)
    for i in range(10):
        print(f"iteration {i}")
        for batch in it_t():
            bulk_insert_transactions(con2, batch)
        for batch in it_o():
            bulk_insert_offers(con2, batch)

    # Stop runners gracefully
    print("Stopping runners...")
    try:
        subprocess.run(["./stop_offer_collection.sh"], check=True)
        subprocess.run(["./stop_transaction_collection.sh"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error stopping runners: {e}")

    # Wait a moment for runners to shutdown cleanly
    time.sleep(0.1)  # They will have to manage somehow

    # Synchronize WAL (Write-Ahead Logging) mode to flush everything
    print("Checkpointing databases...")
    con1.execute("PRAGMA wal_checkpoint(TRUNCATE);")
    con2.execute("PRAGMA wal_checkpoint(TRUNCATE);")
    con1.commit()
    con2.commit()

    for batch in it_t():
        bulk_insert_transactions(con2, batch)
    for batch in it_o():
        bulk_insert_offers(con2, batch)
    # Restart runners
    print("Starting runners...")
    try:
        subprocess.run(["./start_offer_collection.sh"], check=True)
        subprocess.run(["./start_transaction_collection.sh"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error starting runners: {e}")

    print("Database copy and runner restart complete.")


conn_old = sqlite3.connect("all_transactions.db", timeout=5)
conn_old.execute("PRAGMA journal_mode=WAL;")
cur_old = conn_old.cursor()
cur_old.execute("PRAGMA Foreign_keys = off")
conn_old.commit()


conn_new = sqlite3.connect("transaction_and_offers.db", timeout=5)
conn_new.execute("PRAGMA journal_mode=WAL;")
cur_new = conn_new.cursor()
cur_new.execute("PRAGMA Foreign_keys = off")
conn_new.commit()

conn_temp = sqlite3.connect("temp.db", timeout=5)
conn_temp.execute("PRAGMA journal_mode=WAL;")
cur_temp = conn_temp.cursor()
cur_temp.execute("PRAGMA Foreign_keys = off")
conn_temp.commit()

if __name__ == "__main__":
    setup(conn_new)
    # setup(conn_temp)
    # copy_db(conn_new, conn_temp)
    copy_db_from_old(conn_old, conn_new)
