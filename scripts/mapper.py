import sys
import json

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        records = json.loads(line)  
        if not isinstance(records, list):  # ensuring records is a list
            continue
    except json.JSONDecodeError:
        continue

    for record in records:  
        if not isinstance(record, dict):  # ensuring records is a dictionnary
            continue

        coin_id = record.get('id')
        current_price = record.get('current_price')
        volume = record.get('total_volume')

        # making shure evrething is ok
        if not coin_id or current_price is None or volume is None:
            continue

        try:
            price_float = float(current_price)
            volume_float = float(volume)
        except ValueError:
            continue

        price_squared = price_float * price_float

        print(f"{coin_id}\t{price_float},{price_squared},{volume_float},1")