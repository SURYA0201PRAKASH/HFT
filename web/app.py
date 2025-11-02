from flask import Flask, render_template, jsonify
import sqlite3
import os

app = Flask(__name__)

# ✅ Path to your HFT database
DB_PATH = os.path.join(os.path.dirname(__file__), '../build/orderbook_data.db')

def get_orderbook(instrument):
    if not os.path.exists(DB_PATH):
        print(f"❌ Database not found at: {DB_PATH}")
        return [], []

    try:
        # ✅ Open SQLite in read-only mode to avoid "database is locked"
        conn = sqlite3.connect(f'file:{DB_PATH}?mode=ro', uri=True)
        c = conn.cursor()
        c.execute("""
            SELECT side, price, amount 
            FROM orderbook 
            WHERE instrument=? 
            ORDER BY timestamp DESC 
            LIMIT 200
        """, (instrument,))
        rows = c.fetchall()
        conn.close()

    except sqlite3.OperationalError as e:
        print(f"⚠️ SQLite error: {e}")
        return [], []

    # Separate and sort bids and asks
    bids = sorted([r for r in rows if r[0] == 'BID'], key=lambda x: x[1], reverse=True)[:10]
    asks = sorted([r for r in rows if r[0] == 'ASK'], key=lambda x: x[1])[:10]
    return bids, asks


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/orderbook/<instrument>')
def orderbook(instrument):
    bids, asks = get_orderbook(instrument)
    return jsonify({'bids': bids, 'asks': asks})


if __name__ == '__main__':
    print(f"🚀 Flask server starting...")
    print(f"📂 Using database: {DB_PATH}")
    app.run(debug=True)
