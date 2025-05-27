import time
import json
import api_wrapper
from set_up_db import bulk_insert_offers, conn_new as conn, setup
setup(conn)


api_wrapper.start_scheduled_fetch_in_background(
    api_wrapper.offers_link, "", api_wrapper.recently_listed, 0.5)

old_offers = []
loop = 0
while True:
    loop += 1
    newoffers = api_wrapper.find_new_offers("", old_offers)
    if not newoffers:
        loop -= 1
        time.sleep(0.04)
        continue

    # Keep only the latest 100 offers sorted by ends_at_min descending
    old_offers.extend(newoffers)
    old_offers = sorted(
        old_offers, key=lambda x: x["ends_at_min"], reverse=True)[:100]

    rows = []
    for o in newoffers:
        item = o["item"]
        rows.append({
            'price': o["price"],
            'time_left': o["time_left"],
            'ends_at_min': o["ends_at_min"],
            'ends_at_max': o["ends_at_max"],
            'seller_name': o["seller"]["name"],
            'seller_uuid': o["seller"]["uuid"],
            'item_identifier': item["id"],
            'item_count': item["count"],
            'item_display_name': item["display_name"],
            'item_enchants': json.dumps(item["enchants"]),
            'item_lore': json.dumps(item.get("lore")),
            'item_trim': json.dumps(item.get("enchants", {}).get("trim", {})),
            'item_contents': json.dumps(item.get("contents"))
        })

    # Bulk insert offers and all related subtables
    bulk_insert_offers(conn, rows)

    if loop < 10 or loop % 10000 == 0 or loop in [10, 100, 1000] or len(newoffers) > 40:
        print(f"Loop {loop} inserted {len(newoffers)} offers.")
