import sqlite3
from fastapi import FastAPI, HTTPException, Query
from datetime import datetime
from typing import Optional, List
import statistics
import os

app = FastAPI()

DB_NAME = "data.db"

if not os.path.exists(DB_NAME):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''CREATE TABLE devices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE
                )''')
    c.execute('''CREATE TABLE device_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    device_id INTEGER,
                    timestamp TEXT,
                    x REAL,
                    y REAL,
                    z REAL,
                    FOREIGN KEY (device_id) REFERENCES devices (id)
                )''')
    conn.commit()
    conn.close()

@app.post("/device")
def create_device(name: str):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    try:
        c.execute("INSERT INTO devices (name) VALUES (?)", (name,))
        conn.commit()
        device_id = c.lastrowid
    except sqlite3.IntegrityError:
        conn.close()
        raise HTTPException(status_code=400, detail="Device already exists")
    conn.close()
    return {"id": device_id, "name": name}

@app.post("/device/{device_id}/data")
def add_data(device_id: int, x: float, y: float, z: float):
    timestamp = datetime.utcnow().isoformat()
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT id FROM devices WHERE id = ?", (device_id,))
    if not c.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="Device not found")
    c.execute("""
        INSERT INTO device_data (device_id, timestamp, x, y, z)
        VALUES (?, ?, ?, ?, ?)
    """, (device_id, timestamp, x, y, z))
    conn.commit()
    conn.close()
    return {"status": "data added"}

@app.get("/device/{device_id}/analytics")
def get_analytics(device_id: int,
                  start: Optional[str] = Query(None),
                  end: Optional[str] = Query(None)):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    query = "SELECT x, y, z FROM device_data WHERE device_id = ?"
    params: List = [device_id]
    if start:
        query += " AND timestamp >= ?"
        params.append(start)
    if end:
        query += " AND timestamp <= ?"
        params.append(end)
    c.execute(query, tuple(params))
    rows = c.fetchall()
    conn.close()
    if not rows:
        return {"message": "No data found"}

    x_vals = [r[0] for r in rows]
    y_vals = [r[1] for r in rows]
    z_vals = [r[2] for r in rows]

    def stats(values: List[float]):
        return {
            "min": min(values),
            "max": max(values),
            "count": len(values),
            "sum": sum(values),
            "median": statistics.median(values)
        }

    return {
        "x": stats(x_vals),
        "y": stats(y_vals),
        "z": stats(z_vals)
    }
