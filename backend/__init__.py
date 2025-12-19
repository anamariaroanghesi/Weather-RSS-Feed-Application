"""
Weather RSS Feed Application Backend

A dependable weather data collection system with:
- ANM XML forecast fetching (state-based)
- ANM RSS alert fetching (event-based)
- SQLite persistence with integrity checks
- Scheduled automated updates
- REST API for data access
"""

from .database import Database
from .fetcher import ANMFetcher, CityForecast, WeatherAlert, FetchError, ValidationError
from .scheduler import WeatherScheduler, FetchResult
from .api import app

__version__ = "2.0.0"

__all__ = [
    "Database",
    "ANMFetcher",
    "CityForecast",
    "WeatherAlert",
    "FetchError",
    "ValidationError",
    "WeatherScheduler",
    "FetchResult",
    "app",
]
