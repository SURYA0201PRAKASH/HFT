import zmq
import json

ctx = zmq.Context()
socket = ctx.socket(zmq.SUB)
socket.connect("tcp://127.0.0.1:5555")  # must match the publisher endpoint
socket.setsockopt_string(zmq.SUBSCRIBE, "")  # subscribe to all topics

print("🎧 Listening for real ticks...")

while True:
    try:
        # ✅ Receive multipart: [topic, payload]
        topic = socket.recv_string()     # e.g. "BTC-PERPETUAL"
        payload = socket.recv_string()   # e.g. '{"price": 118728.0, ...}'

        # ✅ Parse payload JSON
        data = json.loads(payload)

        instrument = data.get("instrument", topic)
        tick_type = data.get("type", None)
        price = data.get("price", 0.0)
        qty = data.get("quantity", 0.0)
        seq = data.get("sequence", 0)

        print(f"✅ {instrument} | Seq: {seq} | Price: {price} | Qty: {qty} | Type: {tick_type}")

    except json.JSONDecodeError:
        print(f"⚠️ Invalid JSON in payload for topic {topic}")
    except Exception as e:
        print(f"❌ Error: {e}")
