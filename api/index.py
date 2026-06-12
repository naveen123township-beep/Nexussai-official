import os
import sqlite3
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app) # Enables frontend cross-origin requests

# Vercel bypass: Use the writeable /tmp/ directory if running in production
if os.environ.get('VERCEL'):
    DB_FILE = "/tmp/database.db"
else:
    DB_FILE = "database.db"

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Automatically builds the schema if the DB file doesn't exist yet."""
    conn = get_db_connection()
    conn.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL UNIQUE,
        balance REAL DEFAULT 0.0
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS utr_submissions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        utr_number TEXT NOT NULL UNIQUE,
        amount REAL NOT NULL,
        status TEXT DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id)
    );
    """)
    # Inject a dummy user for testing if tables are fresh
    try:
        conn.execute("INSERT INTO users (id, username, balance) VALUES (1, 'test_user', 0.0)")
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    finally:
        conn.close()

# Run database setup on startup
init_db()

@app.route('/')
def home():
    return jsonify({"status": "UTR API is running successfully on Vercel"}), 200

@app.route('/api/submit_utr', methods=['POST'])
def submit_utr():
    data = request.json or {}
    user_id = data.get('user_id')
    utr_number = data.get('utr_number', '').strip()
    amount = data.get('amount')

    if not user_id or not utr_number or not amount:
        return jsonify({"error": "Missing required fields"}), 400

    conn = get_db_connection()
    try:
        conn.execute(
            "INSERT INTO utr_submissions (user_id, utr_number, amount) VALUES (?, ?, ?)",
            (user_id, utr_number, amount)
        )
        conn.commit()
        return jsonify({"message": "UTR submitted successfully. Awaiting admin approval."}), 201
    except sqlite3.IntegrityError:
        return jsonify({"error": "This UTR number has already been submitted."}), 400
    finally:
        conn.close()

@app.route('/api/admin/pending_utrs', methods=['GET'])
def get_pending_utrs():
    conn = get_db_connection()
    query = """
        SELECT u.username, utr.id, utr.utr_number, utr.amount, utr.created_at 
        FROM utr_submissions utr
        JOIN users u ON utr.user_id = u.id
        WHERE utr.status = 'pending'
        ORDER BY utr.created_at ASC
    """
    rows = conn.execute(query).fetchall()
    conn.close()
    return jsonify({"pending_utrs": [dict(row) for row in rows]}), 200

@app.route('/api/admin/verify_utr', methods=['POST'])
def verify_utr():
    data = request.json or {}
    submission_id = data.get('submission_id')
    action = data.get('action')

    if action not in ['approve', 'reject']:
        return jsonify({"error": "Invalid action. Use 'approve' or 'reject'"}), 400

    conn = get_db_connection()
    submission = conn.execute("SELECT * FROM utr_submissions WHERE id = ?", (submission_id,)).fetchone()

    if not submission:
        conn.close()
        return jsonify({"error": "Submission not found"}), 404
    if submission['status'] != 'pending':
        conn.close()
        return jsonify({"error": "This transaction has already been processed"}), 400

    try:
        if action == 'approve':
            conn.execute("UPDATE utr_submissions SET status = 'approved' WHERE id = ?", (submission_id,))
            conn.execute("UPDATE users SET balance = balance + ? WHERE id = ?", (submission['amount'], submission['user_id']))
            message = "UTR approved. User balance updated."
        else:
            conn.execute("UPDATE utr_submissions SET status = 'rejected' WHERE id = ?", (submission_id,))
            message = "UTR rejected successfully."
        conn.commit()
        return jsonify({"success": True, "message": message}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({"error": "Database error", "details": str(e)}), 500
    finally:
        conn.close()
  
