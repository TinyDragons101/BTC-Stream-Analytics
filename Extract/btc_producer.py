# File: lab4/Extract/btc_producer.py
from confluent_kafka import Producer
import requests, json, time, datetime

producer = Producer({'bootstrap.servers': 'kafka:9092'})

def round_timestamp(ts: float, step_ms: int = 100):
    """
    Làm tròn thời gian epoch `ts` xuống bội số `step_ms` (mili-giây)
    và trả về chuỗi ISO-8601 chuẩn UTC (đuôi 'Z')
    """
    rounded_ms = (int(ts * 1000) // step_ms) * step_ms
    rounded_sec = rounded_ms / 1000.0

    dt_utc = datetime.datetime.fromtimestamp(rounded_sec, tz=datetime.timezone.utc)
    return dt_utc.isoformat(timespec="milliseconds").replace("+00:00", "Z")

while True:
    try:
        r = requests.get("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT")
        data = r.json()
        now = time.time()
        msg = {
            "symbol": "BTCUSDT",
            "price": float(data["price"]),
            "event_time": round_timestamp(now)
        }
        # print("Sending:", msg)
        producer.produce("btc-price", json.dumps(msg).encode('utf-8'))
        producer.poll(0)
    except Exception as e:
        print("Error:", e)
    time.sleep(0.1)
