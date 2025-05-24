import itertools
import sqlite3
import json

# Connect to your SQLite database
# Change to your .db filename
conn = sqlite3.connect("all_transactions.db", timeout=5)
cur = conn.cursor()


def format_price(price):
    if price == 0:
        return "0"
    elif price < 1000:
        return f"{price}"
    elif price < 1000000:
        return f"{price/1000:.1f}k"
    elif price < 100000000:
        return f"{price/1000000:.2f}m"
    elif price < 1000000000:
        return f"{price/1000000:.1f}m"
    else:
        return f"{price/1000000000:.1f}b"


def print_table_sizes():
    print(sqlite3.sqlite_version)
    for table_name in ["seller", "transactions", "item_map", "offers"]:

        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cur.fetchone()[0]
        print(table_name, row_count)
        cur.execute(
            f"SELECT SUM(pgsize) FROM pragma_page_count('{table_name}')")
        page_size = cur.fetchone()[0]
        print(f"  Page Size: {page_size} bytes")
        cur.execute(
            f"SELECT SUM(length(name)) FROM pragma_table_info('{table_name}')")
        column_size = cur.fetchone()[0]
        print(f"  Column Size: {column_size} bytes")
        print(f"  Total Size: {page_size + column_size} bytes")

        cur.execute(f"PRAGMA table_info({table_name})")
        columns = cur.fetchall()  # Each row: (cid, name, type, notnull, dflt_value, pk)
        for col in columns:
            print(f"  {col[1]} - {col[2]}")
        print()


def analys_storage_potential():
    for groups in [["i.id", "s.id", "t.price"], ["i.id", "s.id"], ["i.id", "t.price"], ["i.id"], ["s.id"], ["t.price"]]:
        print(f"Grouping By {', '.join(groups)}")
        cur.execute(f'''
            SELECT
                COUNT(*), i.id , s.id, t.price
            FROM transactions t
            JOIN seller s ON t.seller_id = s.id
            JOIN item_map i ON t.item_id = i.id
            Group By {', '.join(groups)}
            ORDER BY COUNT(*) DESC
        ''')

        rows = cur.fetchall()
        a = sum(map(lambda x: x[0], rows[:255]))
        print(f"Potential storage for top 255 combos: {a}")
        a = sum(map(lambda x: x[0], rows[:65535]))
        print(f"Potential storage for top 65535 combos: {a}")
        a = sum(map(lambda x: x[0], rows))
        print(f"Potential storage for all: {a}")
        print()


def query_transactions():
    # Query all transactions joined with seller and item info
    cur.execute('''
        SELECT
            COUNT(*), SUM(t.price), i.item_id, i.count,
            i.enchants, i.lore, i.trim, i.contents
        FROM transactions t
        JOIN seller s ON t.seller_id = s.id
        JOIN item_map i ON t.item_id = i.id
        Group By t.item_id
        ORDER BY COUNT(*) ASC
    ''')

    # Query all transactions joined with seller and item info
    # cur.execute('''
    #     SELECT
    #         COUNT(*), SUM(t.price), i.item_id, i.count,
    #         i.enchants, i.lore, i.trim, i.contents
    #     FROM transactions t
    #     JOIN seller s ON t.seller_id = s.id
    #     JOIN item_map i ON t.item_id = i.id
    #     Group By t.item_id
    #     ORDER BY COUNT(*) ASC
    # ''')
    # Query all transactions joined with seller and item info
    cur.execute('''
        SELECT 
            COUNT(*), SUM(t.price), i.item_id, i.count
        FROM transactions t
        JOIN seller s ON t.seller_id = s.id
        JOIN item_map i ON t.item_id = i.id
        Group By i.item_id, i.count
        ORDER BY SUM(t.price) ASC
    ''')

    # Fetch all rows
    rows = cur.fetchall()

    # Display nicely
    for row in rows:
        (
            amount, sum,
            item_id, count,
            # enchants, lore, trim, contents
        ) = row

        print(f"{item_id:<30}", end="")
        print(f"{count:<6}", end=": ")
        print(f"{amount:<6}", end="")
        print(f"{format_price(sum):<7}", end="")
        # if enchants.strip() != '{"enchantments": {"levels": null}, "trim": {"material": "", "pattern": ""}}':
        #     print(f"{enchants}", end=" ")
        # if trim.strip() != '{"material": "", "pattern": ""}':
        #     print(f"{trim}", end=" ")
        # if lore != "null":
        #     print(f"{lore}", end=" ")
        print()


print_table_sizes()
analys_storage_potential()
# Close the connection
conn.close()
