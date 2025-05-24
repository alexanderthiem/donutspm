import threading
import asyncio
import aiohttp
from collections import defaultdict, deque
import json
from collections import Counter
import bisect
import sys
import os
import time
import requests

# Replace with your actual API endpoint and key
api_url = "https://api.donutsmp.net/"
api_key = "3df6c20daa764201b4ea0a98f0f593b0"

gold_block = "gold block"
diamond_block = "diamond block"
emerald_block = "emerald block"
netherite_block = "netherite_block"
diamond_hoe = "diamond hoe"
hay_bale = "Hay block"

items = [gold_block, diamond_block, emerald_block,
         netherite_block, diamond_hoe, hay_bale]
block = "block"
blocks = [gold_block, diamond_block, emerald_block,
          netherite_block, hay_bale]
id_map = {gold_block: "minecraft:gold_block",
          diamond_block: "minecraft:diamond_block",
          emerald_block: "minecraft:emerald_block",
          netherite_block: "minecraft:netherite_block",
          diamond_hoe: "minecraft:diamond_hoe",
          hay_bale: "minecraft:hay_block"}
id_map[block] = list(map(lambda x: id_map[x], blocks))


def get_item_name(id):
    for item, item_id in id_map.items():
        if id == item_id:
            return item
    return id


lowest_price = "lowest_price"
highest_price = "highest_price"
recently_listed = "recently_listed"
last_listed = "last_listed"

offers_link = "v1/auction/list/"
transactions_link = "v1/auction/transactions/"
# Option 1: API key in headers

offers_buffers = {}


def get_offers_buffer(kind, search, sort_by):
    key = (kind, search, sort_by)
    if key not in offers_buffers:
        offers_buffers[key] = deque()
    return offers_buffers[key]


async def async_raw_api_request(session, kind, search, sort_by, page=1, depth=5):
    if not hasattr(async_raw_api_request, "request_id"):
        async_raw_api_request.request_id = 0
    async_raw_api_request.request_id += 1

    if kind == transactions_link and search != "":
        print("transactions don't support searching")
    if kind == transactions_link and sort_by != recently_listed:
        print("transactions don't support other sorts than recently_listed")

    res = {}
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    payload = {
        "search": search,
        "sort": sort_by,
    }
    url = f"{api_url}{kind}{page}"
    time_before_request = time.time() * 1000

    try:
        async with session.get(url, headers=headers, json=payload) as response:
            time_after_request = time.time() * 1000
            res["status_code"] = response.status
            res["last_page"] = False

            if response.status == 200:
                # print(await response.text())
                text = await response.text()  # its json, but not flagged as json
                data = json.loads(text)
                data = data["result"]
                if data and data[-1] is None:
                    res["last_page"] = True
                data = list(filter(lambda x: x is not None, data))

                for d in data:
                    d["request_id"] = async_raw_api_request.request_id
                    if kind == offers_link:
                        d["ends_at_min"] = d["time_left"] + time_before_request
                        d["ends_at_max"] = d["time_left"] + time_after_request
                res["data"] = data
                raw_time = time_after_request - time_before_request
                if raw_time > 900:
                    print(raw_time)
                return res
            else:
                if response.status == 429:
                    print(
                        f"Rate limit exceeded, sleeping for {2**(5-depth)} intervals ({2**(5-depth)*60/250:.2f} seconds)"
                    )
                    await asyncio.sleep(2 * 60 * (1 / 250) * (2 ** (5 - depth)))

                if depth > 0:
                    return await async_raw_api_request(session, kind, search, sort_by, page, depth - 1)

                print(
                    f"Requested {kind} for {search} with sort {sort_by} and page {page}")
                print(f"Error: {response.status} - {await response.text()}")
                return res
    except aiohttp.ClientError as e:
        print(f"Client error: {e}")
        return res


async def scheduled_fetch(kind, search, sort_by, interval=0.5):
    async with aiohttp.ClientSession() as session:
        async def fetch_loop():
            while True:
                asyncio.create_task(fetch_and_store(
                    session, kind, search, sort_by))
                await asyncio.sleep(interval)

        async def fetch_and_store(session, kind, search, sort_by):
            result = await async_raw_api_request(session, kind, search, sort_by)
            if "data" in result:
                get_offers_buffer(kind, search, sort_by).append(result["data"])

        await fetch_loop()


def start_scheduled_fetch_in_background(kind, search, sort_by, interval=0.5):
    def run_loop():
        asyncio.run(scheduled_fetch(kind, search, sort_by, interval))

    thread = threading.Thread(target=run_loop, daemon=True)
    thread.start()


def simulate_api_call(kind, search, sort_by):
    buffer = get_offers_buffer(kind, search, sort_by)
    new_offers = []
    if buffer:
        new_offers.extend(buffer.popleft())
    return {"status_code": 200, "data": new_offers, "last_page": False}


def raw_api_request(kind, search, sort_by, page=1, depth=5):
    if not hasattr(raw_api_request, "request_id"):
        raw_api_request.request_id = 0
    raw_api_request.request_id += 1
    # print(
    #     f"Requesting {kind} for {search} with sort {sort_by} and page {page}")
    if kind == transactions_link and search != "":
        print("transactions dont support searching")
    if kind == transactions_link and sort_by != recently_listed:
        print("transactions dont support other sorts than recently_listed")

    try:
        res = {}
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json"}
        payload = {
            "search": search,
            "sort": sort_by,
        }
        time_before_request = time.time()*1000
        response = requests.get(api_url + kind + str(page),
                                headers=headers, json=payload, timeout=3)
        time_after_request = time.time()*1000

        res["status_code"] = response.status_code
        res["last_page"] = False

        if response.status_code == 200:
            data = response.json()
            data = data["result"]
            if data[-1] is None:
                res["last_page"] = True
            data = list(filter(lambda x: x is not None, data))

            for d in data:
                d["request_id"] = raw_api_request.request_id
                if kind == offers_link:
                    d["ends_at_min"] = d["time_left"] + time_before_request
                    d["ends_at_max"] = d["time_left"] + time_after_request
            # print("Raw delta: ", time_after_request-time_before_request)
            res["data"] = data
            return res
        else:
            if response.status_code == 429:  # Too Many Requests
                print(
                    f"Rate limit exceeded, sleeping for {2**(5-depth)} intervalls {2**(5-depth)*60/250} seconds")
                # Sleep for 2**(5-depth) intervals
                time.sleep(2 * 60 * (1/250) * (2**(5-depth)))

            if depth > 0:
                return raw_api_request(kind, search, sort_by, page, depth-1)
            print(
                f"Requested {kind} for {search} with sort {sort_by} and page {page}")
            print(f"Error: {response.status_code} - {response.text}")
            return res
    except requests.Timeout as e:
        print(f"Request timed out: {e}")
        if depth > 0:
            return raw_api_request(kind, search, sort_by, page, depth-1)
        return {"status_code": 408, "data": [], "last_page": False}

    except requests.RequestException as e:
        print(f"Request error: {e}")
        if depth > 0:
            return raw_api_request(kind, search, sort_by, page, depth-1)
        return {"status_code": 500, "data": [], "last_page": False}


def apply_filter_now(search, data):
    if search == "alt04":
        return list(filter(lambda x: x["seller"]["name"] == "alt04", data))
    elif search in id_map and isinstance(id_map[search], str):
        return list(filter(lambda x: x["item"]["id"] == id_map[search], data))
    elif search in id_map and isinstance(id_map[search], list):
        return list(filter(lambda x: x["item"]["id"] in id_map[search], data))
    else:
        print(f"Error, not a valid filter: {search}")


def request_api_filtered(kind, search, sort_by, page=1, apply_filter=True, use_buffer=False):
    if use_buffer:
        response = simulate_api_call(kind, search, sort_by)
    else:
        response = raw_api_request(kind, search, sort_by, page)
    if response["status_code"] == 200:
        if apply_filter:
            response["data"] = apply_filter_now(search, response["data"])
        return response
    return False


def get_all_pages(kind, search, sort_by, apply_filter=True):
    # TODO, parralelise this
    page = 1
    all_data = []
    now = time.time()
    while True and page < 10:
        response = request_api_filtered(
            kind, search, sort_by, page, apply_filter)
        if not response:
            break
        data = response["data"]
        all_data.extend(data)
        if response["last_page"]:
            break
        page += 1
    to_sleep = 60/250*page - (time.time()-now)
    if to_sleep > 0:
        time.sleep(to_sleep)  # Rate limit

    return all_data


if os.path.exists("transaction_summary.json"):
    with open("transaction_summary.json", "r", encoding="utf-8") as f:
        raw = json.load(f)
    transaction_summary = defaultdict(lambda: {'number': 0, 'volume': 0},
                                      {eval(k): v for k, v in raw.items()}
                                      )
else:
    transaction_summary = defaultdict(lambda: {'number': 0, 'volume': 0})


def update_transaction_cache():
    """updates and returns the cache for all transactions, unfiltered, sorted by time"""
    transactions_last_fetched = 0
    if not hasattr(update_transaction_cache, "transaction_cache"):
        update_transaction_cache.transaction_cache = []
        transactions_last_fetched = 0
    elif len(update_transaction_cache.transaction_cache) > 0:
        transactions_last_fetched = update_transaction_cache.transaction_cache[
            0]["unixMillisDateSold"]
    else:
        print("Hmmm, empty (existent) transaction cache???")
        transactions_last_fetched = 0

    if ((time.time()*1000) - transactions_last_fetched) < (1000/250 * 60):
        return 0, update_transaction_cache.transaction_cache

    page = 1
    all_data = []
    now = time.time()
    while True and page < 10:
        data = raw_api_request(transactions_link, "",
                               recently_listed, page)["data"]
        page += 1
        # if not data:
        #     break
        all_data.extend(data)
        if len(data) > 0 and data[-1]["unixMillisDateSold"] < transactions_last_fetched:
            break
    all_data = list(
        filter(lambda x: x["unixMillisDateSold"] > transactions_last_fetched, all_data))
    all_data.sort(key=lambda x: x["unixMillisDateSold"], reverse=True)
    # make all data unique
    seen = set()
    all_data = [x for x in all_data if not (x["item"]["id"], x["seller"]["uuid"], x["price"], x["unixMillisDateSold"]) in seen and not seen.add(
        (x["item"]["id"], x["seller"]["uuid"], x["price"], x["unixMillisDateSold"]))]
    new_len = len(all_data)
    all_data.extend(
        update_transaction_cache.transaction_cache[:10000-len(all_data)])
    update_transaction_cache.transaction_cache = all_data
    to_sleep = 60/250*page - (time.time()-now)
    if to_sleep > 0:
        time.sleep(to_sleep)  # Rate limit
    return new_len, update_transaction_cache.transaction_cache


def get_new_transactions_since(search, last_trade):
    _, cache = update_transaction_cache()
    if last_trade:
        index = bisect.bisect(
            cache, -(last_trade+1), key=lambda x: -x["unixMillisDateSold"])
        cache = cache[:index]
    if search:
        cache = apply_filter_now(search, cache)
        # if cache and search == "alt04" and last_trade != 0:
        #     print(last_trade, cache)
        #     exit()
    return cache


def offers_match(offer1, offer2):
    if (
        offer1["seller"]["uuid"] == offer2["seller"]["uuid"] and
        offer1["price"] == offer2["price"] and
        offer1["item"]["id"] == offer2["item"]["id"] and
        offer1["item"]["count"] == offer2["item"]["count"] and
        min(offer1["ends_at_max"], offer2["ends_at_max"]) > max(
            offer1["ends_at_min"], offer2["ends_at_min"])
    ):
        return True
    return False


last_seen_offers_per_search_or_item_id = {}


def find_new_offers(search, old_offers):
    if not last_seen_offers_per_search_or_item_id:
        pass
    old_offers = sorted(
        old_offers, key=lambda x: x["ends_at_min"], reverse=True)[:40]
    new_offers = request_api_filtered(offers_link, search, recently_listed,
                                      page=1, apply_filter=False, use_buffer=True)["data"]
    matches = set()
    strength = {}
    for a, old_offer_a in enumerate(old_offers):
        for b, new_offer_b in enumerate(new_offers):
            if offers_match(old_offer_a, new_offer_b):
                strength[(a, b)] = 0
                for other_matches in matches:
                    a2, b2 = other_matches
                    delta_t1 = old_offers[a]["time_left"] - \
                        old_offers[a2]["time_left"]
                    delta_t2 = new_offers[b]["time_left"] - \
                        new_offers[b2]["time_left"]
                    if delta_t1 == delta_t2:
                        strength[(a, b)] += 1
                        strength[(a2, b2)] += 1
                matches.add((a, b))
    # print(matches)
    delays_min = [0]
    delays_max = [0]
    missleading = set()
    for (a, b) in matches:
        for (a2, b2) in matches:
            if a == a2 or b == b2:
                missleading.add((a, b))
                missleading.add((a2, b2))

    for (a, b) in matches:
        if strength[(a, b)] > 3:
            delays_min.append(old_offers[a]["ends_at_min"] -
                              new_offers[b]["ends_at_min"])
            delays_max.append(old_offers[a]["ends_at_max"] -
                              new_offers[b]["ends_at_max"])
    subtract_from_max = min(delays_max)
    add_to_min = max(delays_min)
    for d in new_offers:
        d["ends_at_max"] -= subtract_from_max
        d["ends_at_min"] -= add_to_min
    # print("new delta:",
    #       (new_offers[0]["ends_at_max"]-new_offers[0]["ends_at_min"])/1000)
    if not matches:
        # print("Dangerous, no matches!!!!", search)
        return new_offers
    else:
        smallest_match = min(matches, key=lambda x: x[1])
        return new_offers[:smallest_match[1]]


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


def print_row(item, data):
    print(f"{item:<15}", end=" ")
    for count in 1, 8, 16, 32, 64:
        filtered_data = list(
            filter(lambda x: x["item"]["count"] == count, data))
        filtered_data = sorted(
            filtered_data, key=lambda x: x["price"], reverse=False)
        if len(filtered_data) > 0:
            if filtered_data[0]["seller"]["name"] == "alt04":
                print("\033[92m", end="")
            print(
                f"{format_price(filtered_data[0]['price']):<7}", end=" ")
            if filtered_data[0]["seller"]["name"] == "alt04":
                print("\033[0m", end="")

        else:
            print(
                f"{"":<7}", end=" ")
    if len(filtered_data) > 0:
        print(
            f"{("|"+format_price(filtered_data[0]['price']/64)):<7}", end=" ")
    else:
        print(
            f"{("|"):<7}", end=" ")
    m = min(map(lambda x: (x["price"]/x["item"]
            ["count"], x["item"]["count"]), data))
    print(f"{("|"+format_price(m[0])+" "+str(m[1])):<10}", end=" ")

    print()


def update_item(item, data, new_transactions, new_offers):
    if len(new_transactions) > 0:
        global last_update
        last_update[item] = new_transactions[0]["unixMillisDateSold"]
    new_transactions = list(filter(lambda x: x["item"]["id"] == id_map.get(item) or
                                   (item in id_map and isinstance(id_map[item], list) and x["item"]["id"] in id_map.get(item)), new_transactions))
    new_offers = list(filter(lambda x: x["item"]["id"] == id_map.get(item) or
                             (item in id_map and isinstance(id_map[item], list) and x["item"]["id"] in id_map.get(item)), new_offers))
    # print(
    #     f"Updating {item} with {len(new_transactions)} transactions and {len(new_offers)} offers")
    # print(f"new offers: {new_offers}")
    data.extend(new_offers)
    for transaction in new_transactions:
        for i in range(len(data)):
            d = data[i]
            if d["item"] == transaction["item"] and d["seller"] == transaction["seller"] and d["price"] == transaction["price"]:
                # print(f"Removing {d['item']['id']}", end="")
                data.pop(i)
                break
    return data


def keep_printing():
    recent_trades = []
    offer_cache = {}
    last_update = {}
    round = -1
    while True:
        round += 1
        if round % 50 == 0:
            # clear console
            os.system('cls' if os.name == 'nt' else 'clear')
        print(f"{"item":<15}", end=" ")
        counts = [1, 8, 16, 32, 64, "|per", "|min"]
        for count in counts:
            print(f"{str(count):<7}", end=" ")
        print()
        for item in items:
            if round % 50 == 0:
                last_update[item] = last_update.get(item, 0)
                data = get_all_pages(offers_link, item, lowest_price)
                offer_cache[item] = data
            else:
                # find_new_offers(item, offer_cache[item])
                if item == gold_block:
                    new_transactions = get_new_transactions_since(
                        block, last_update[item])
                    new_offers = find_new_offers(
                        block, offer_cache[gold_block]+offer_cache[diamond_block]+offer_cache[emerald_block]+offer_cache[netherite_block])
                    # print(len(new_offers))
                    for item2 in [gold_block]+[diamond_block, emerald_block, netherite_block, hay_bale]:
                        offer_cache[item2] = update_item(
                            item2, offer_cache[item2], new_transactions, new_offers)
                    data = offer_cache[item]

                elif item in [diamond_block, emerald_block, netherite_block, hay_bale]:
                    data = offer_cache[item]
                else:
                    new_transactions = get_new_transactions_since(
                        item, last_update[item])
                    new_offers = find_new_offers(item, offer_cache[item])
                    offer_cache[item] = update_item(
                        item, offer_cache[item], new_transactions, new_offers)
                    data = offer_cache[item]

            print_row(item, data)
        recent_trades.extend(get_new_transactions_since("alt04", 0 if len(
            recent_trades) == 0 else recent_trades[0]["unixMillisDateSold"]))
        numlist = min(5, len(recent_trades))
        for i in range(numlist):
            print(f"Recent trade {i+1}: ", end="")
            print(f"{recent_trades[-(i+1)]['item']['id']:<7}", end=" ")
            print(
                f"{get_item_name(recent_trades[-(i+1)]['item']['count']):<7}", end=" ")
            print(
                f"{format_price(recent_trades[-(i+1)]['price']):<7}", end=" ")
            print()

        sys.stdout.write(f"\033[{len(items)+1+numlist}A")
