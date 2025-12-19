"""
Database module for Weather RSS Feed Application.

Handles SQLite persistence with:
- Separate tables for forecasts (state-based) and alerts (event-based)
- Source health and reliability metrics
- City-based forecast queries
"""

import sqlite3
import threading
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = Path(__file__).parent.parent / "weather.db"


class Database:
    """
    SQLite database wrapper with thread-safe operations.
    
    Dependability features:
    - WAL mode for concurrent reads during writes
    - Automatic schema initialization
    - Data integrity through unique constraints
    - Source reliability tracking
    """
    
    def __init__(self, db_path: str = None) -> None:
        self._db_path = db_path or str(DEFAULT_DB_PATH)
        self._lock = threading.Lock()
        self._conn = None
        self._connect()
        self._init_schema()
        logger.info(f"Database initialized at {self._db_path}")
    
    def _connect(self) -> None:
        """Establish database connection with WAL mode."""
        self._conn = sqlite3.connect(
            self._db_path,
            check_same_thread=False,
            isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
    
    def _init_schema(self) -> None:
        """Initialize database schema."""
        with self._lock:
            # City forecasts table (state-based data from XML)
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS city_forecasts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    city TEXT NOT NULL,
                    forecast_date TEXT NOT NULL,
                    data_date TEXT,
                    temp_min INTEGER NOT NULL,
                    temp_max INTEGER NOT NULL,
                    conditions TEXT,
                    conditions_code TEXT,
                    source_url TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
                    UNIQUE(city, forecast_date, content_hash)
                )
            """)
            
            # Weather alerts table (event-based data from RSS)
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS weather_alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    description TEXT,
                    published_at TEXT,
                    link TEXT,
                    alert_level TEXT,
                    affected_zones TEXT,
                    time_range TEXT,
                    source_url TEXT NOT NULL,
                    content_hash TEXT NOT NULL UNIQUE,
                    fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
                    is_active INTEGER NOT NULL DEFAULT 1
                )
            """)
            
            # Source status table for trustworthiness tracking
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS source_status (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_url TEXT NOT NULL UNIQUE,
                    source_type TEXT NOT NULL,
                    source_name TEXT,
                    last_fetch_at TEXT,
                    last_success_at TEXT,
                    fetch_count INTEGER DEFAULT 0,
                    success_count INTEGER DEFAULT 0,
                    error_count INTEGER DEFAULT 0,
                    last_error TEXT,
                    status TEXT DEFAULT 'unknown',
                    data_quality TEXT DEFAULT 'unknown',
                    is_fresh INTEGER DEFAULT 0,
                    avg_response_time_ms INTEGER DEFAULT 0,
                    last_response_time_ms INTEGER DEFAULT 0,
                    consecutive_failures INTEGER DEFAULT 0,
                    entries_count INTEGER DEFAULT 0
                )
            """)
            
            # Indexes for efficient queries
            self._conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_forecast_city 
                ON city_forecasts(city)
            """)
            self._conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_forecast_date 
                ON city_forecasts(forecast_date)
            """)
            self._conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_alert_level 
                ON weather_alerts(alert_level)
            """)
            self._conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_alert_active 
                ON weather_alerts(is_active)
            """)
    
    # =========================================================================
    # Forecast Operations (State-Based Data)
    # =========================================================================
    
    def insert_forecast(
        self,
        city: str,
        forecast_date: str,
        data_date: str,
        temp_min: int,
        temp_max: int,
        conditions: str,
        conditions_code: str,
        source_url: str,
        content_hash: str
    ) -> bool:
        """Insert or update a city forecast."""
        with self._lock:
            try:
                # Delete old forecasts for this city/date combination
                self._conn.execute("""
                    DELETE FROM city_forecasts 
                    WHERE city = ? AND forecast_date = ?
                """, (city, forecast_date))
                
                self._conn.execute("""
                    INSERT INTO city_forecasts 
                    (city, forecast_date, data_date, temp_min, temp_max, 
                     conditions, conditions_code, source_url, content_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (city, forecast_date, data_date, temp_min, temp_max,
                      conditions, conditions_code, source_url, content_hash))
                return True
            except sqlite3.Error as e:
                logger.error(f"Failed to insert forecast: {e}")
                return False
    
    def get_city_forecast(self, city: str) -> List[Dict[str, Any]]:
        """Get forecasts for a specific city."""
        with self._lock:
            cursor = self._conn.execute("""
                SELECT * FROM city_forecasts
                WHERE city = ?
                ORDER BY forecast_date ASC
            """, (city,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_all_cities(self) -> List[str]:
        """Get list of all cities with forecasts."""
        with self._lock:
            cursor = self._conn.execute("""
                SELECT DISTINCT city FROM city_forecasts
                ORDER BY city ASC
            """)
            return [row[0] for row in cursor.fetchall()]
    
    def search_cities(self, query: str) -> List[str]:
        """Search cities by name prefix."""
        with self._lock:
            cursor = self._conn.execute("""
                SELECT DISTINCT city FROM city_forecasts
                WHERE city LIKE ?
                ORDER BY city ASC
                LIMIT 20
            """, (f"{query}%",))
            return [row[0] for row in cursor.fetchall()]
    
    # =========================================================================
    # Alert Operations (Event-Based Data)
    # =========================================================================
    
    def insert_alert(
        self,
        title: str,
        description: str,
        published_at: Optional[str],
        link: Optional[str],
        alert_level: Optional[str],
        affected_zones: Optional[str],
        time_range: Optional[str],
        source_url: str,
        content_hash: str
    ) -> bool:
        """Insert a weather alert if not duplicate."""
        with self._lock:
            try:
                self._conn.execute("""
                    INSERT OR IGNORE INTO weather_alerts 
                    (title, description, published_at, link, alert_level,
                     affected_zones, time_range, source_url, content_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (title, description, published_at, link, alert_level,
                      affected_zones, time_range, source_url, content_hash))
                
                cursor = self._conn.execute("SELECT changes()")
                return cursor.fetchone()[0] > 0
            except sqlite3.Error as e:
                logger.error(f"Failed to insert alert: {e}")
                return False
    
    def get_active_alerts(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get active weather alerts."""
        with self._lock:
            cursor = self._conn.execute("""
                SELECT * FROM weather_alerts
                WHERE is_active = 1
                ORDER BY published_at DESC
                LIMIT ?
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_alerts_by_level(self, level: str) -> List[Dict[str, Any]]:
        """Get alerts filtered by level (YELLOW, ORANGE, RED)."""
        with self._lock:
            cursor = self._conn.execute("""
                SELECT * FROM weather_alerts
                WHERE alert_level = ? AND is_active = 1
                ORDER BY published_at DESC
            """, (level,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_alert_count(self) -> int:
        """Get total active alert count."""
        with self._lock:
            cursor = self._conn.execute(
                "SELECT COUNT(*) FROM weather_alerts WHERE is_active = 1"
            )
            return cursor.fetchone()[0]
    
    def deactivate_old_alerts(self, hours: int = 24) -> int:
        """Deactivate alerts older than specified hours."""
        with self._lock:
            cursor = self._conn.execute("""
                UPDATE weather_alerts
                SET is_active = 0
                WHERE is_active = 1 
                AND datetime(fetched_at) < datetime('now', ?)
            """, (f"-{hours} hours",))
            return cursor.rowcount
    
    # =========================================================================
    # Source Status Operations
    # =========================================================================
    
    def update_source_status(
        self,
        source_url: str,
        source_type: str,
        source_name: str,
        success: bool,
        data_quality: str,
        is_fresh: bool,
        entries_count: int = 0,
        error_message: Optional[str] = None,
        response_time_ms: int = 0
    ) -> None:
        """Update source status for trustworthiness tracking."""
        with self._lock:
            now = datetime.utcnow().isoformat()
            status = "ok" if success else "error"
            
            cursor = self._conn.execute(
                "SELECT fetch_count, avg_response_time_ms, consecutive_failures FROM source_status WHERE source_url = ?",
                (source_url,)
            )
            existing = cursor.fetchone()
            
            if existing:
                fetch_count = existing[0] or 0
                old_avg = existing[1] or 0
                old_failures = existing[2] or 0
                
                new_avg = int((old_avg * fetch_count + response_time_ms) / (fetch_count + 1)) if fetch_count > 0 else response_time_ms
                consecutive_failures = 0 if success else (old_failures + 1)
                
                self._conn.execute("""
                    UPDATE source_status SET
                        source_type = ?,
                        source_name = ?,
                        last_fetch_at = ?,
                        last_success_at = CASE WHEN ? THEN ? ELSE last_success_at END,
                        fetch_count = fetch_count + 1,
                        success_count = success_count + CASE WHEN ? THEN 1 ELSE 0 END,
                        error_count = error_count + CASE WHEN ? THEN 0 ELSE 1 END,
                        last_error = CASE WHEN ? THEN NULL ELSE ? END,
                        status = ?,
                        data_quality = ?,
                        is_fresh = ?,
                        avg_response_time_ms = ?,
                        last_response_time_ms = ?,
                        consecutive_failures = ?,
                        entries_count = ?
                    WHERE source_url = ?
                """, (
                    source_type, source_name, now, success, now, success, success,
                    success, error_message, status, data_quality, 
                    1 if is_fresh else 0, new_avg, response_time_ms,
                    consecutive_failures, entries_count, source_url
                ))
            else:
                self._conn.execute("""
                    INSERT INTO source_status 
                    (source_url, source_type, source_name, last_fetch_at, last_success_at, 
                     fetch_count, success_count, error_count, last_error, status, 
                     data_quality, is_fresh, avg_response_time_ms, last_response_time_ms, 
                     consecutive_failures, entries_count)
                    VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    source_url, source_type, source_name, now, 
                    now if success else None,
                    1 if success else 0, 0 if success else 1, error_message,
                    status, data_quality, 1 if is_fresh else 0,
                    response_time_ms, response_time_ms, 0 if success else 1, entries_count
                ))
    
    def get_system_status(self) -> List[Dict[str, Any]]:
        """Get status of all feed sources."""
        with self._lock:
            cursor = self._conn.execute("""
                SELECT 
                    source_url, source_type, source_name, last_fetch_at, last_success_at,
                    fetch_count, success_count, error_count, last_error, status,
                    data_quality, is_fresh, avg_response_time_ms, 
                    last_response_time_ms, consecutive_failures, entries_count,
                    CASE WHEN fetch_count > 0 
                         THEN ROUND(CAST(success_count AS FLOAT) / fetch_count * 100, 1)
                         ELSE 0 END as reliability_percent
                FROM source_status
                ORDER BY source_type, source_url
            """)
            return [dict(row) for row in cursor.fetchall()]
    
    def get_data_summary(self) -> Dict[str, Any]:
        """Get summary of all data in database."""
        with self._lock:
            forecast_count = self._conn.execute(
                "SELECT COUNT(*) FROM city_forecasts"
            ).fetchone()[0]
            
            alert_count = self._conn.execute(
                "SELECT COUNT(*) FROM weather_alerts WHERE is_active = 1"
            ).fetchone()[0]
            
            city_count = self._conn.execute(
                "SELECT COUNT(DISTINCT city) FROM city_forecasts"
            ).fetchone()[0]
            
            return {
                "forecast_entries": forecast_count,
                "alert_entries": alert_count,
                "city_count": city_count,
                "total_entries": forecast_count + alert_count
            }
    
    def close(self) -> None:
        """Close database connection."""
        with self._lock:
            if self._conn:
                self._conn.close()
                self._conn = None
                logger.info("Database connection closed")
