"""
Scheduler module for Weather RSS Feed Application.

Handles periodic data fetching from heterogeneous ANM sources:
- XML Forecasts (state-based, periodic updates)
- RSS Alerts (event-based, frequent updates)
"""

import logging
from typing import List, Optional, Dict
from datetime import datetime
from dataclasses import dataclass, asdict

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from .database import Database
from .fetcher import (
    ANMFetcher,
    CityForecast,
    WeatherAlert,
    FetchMetadata,
    SourceType,
    DataQuality,
    FORECAST_SOURCE,
    ALERT_SOURCE,
)

logger = logging.getLogger(__name__)

# Polling intervals
FORECAST_POLL_INTERVAL_MINUTES = 60  # XML forecasts update less frequently
ALERT_POLL_INTERVAL_MINUTES = 10     # Alerts need frequent checking


@dataclass
class FetchResult:
    """Result of a fetch operation."""
    source_url: str
    source_type: str
    source_name: str
    success: bool
    entries_added: int
    total_entries: int
    error_message: Optional[str]
    fetch_time: str
    data_quality: str
    response_time_ms: int


@dataclass
class SyncStatus:
    """Synchronization status across all sources."""
    last_sync_time: str
    forecast_healthy: bool
    alert_healthy: bool
    overall_quality: str
    cities_available: int
    active_alerts: int


class WeatherScheduler:
    """
    Manages periodic weather data fetching from ANM.
    
    Handles heterogeneous sources:
    - Forecasts: XML format, state-based, updates ~hourly
    - Alerts: RSS format, event-based, updates ~10min
    """
    
    def __init__(
        self,
        database: Database,
        forecast_interval: int = FORECAST_POLL_INTERVAL_MINUTES,
        alert_interval: int = ALERT_POLL_INTERVAL_MINUTES
    ):
        self.database = database
        self.forecast_interval = forecast_interval
        self.alert_interval = alert_interval
        self.fetcher = ANMFetcher()
        self.scheduler = BackgroundScheduler()
        self._is_running = False
        
        self._last_forecast_result: Optional[FetchResult] = None
        self._last_alert_result: Optional[FetchResult] = None
        self._last_sync_status: Optional[SyncStatus] = None
    
    def fetch_all(self) -> List[FetchResult]:
        """Fetch data from all sources."""
        results = []
        
        logger.info("Starting full data sync")
        
        # Fetch forecasts (XML)
        forecast_result = self._fetch_forecasts()
        results.append(forecast_result)
        
        # Fetch alerts (RSS)
        alert_result = self._fetch_alerts()
        results.append(alert_result)
        
        # Update sync status
        self._update_sync_status()
        
        logger.info(f"Sync complete: forecasts={'OK' if forecast_result.success else 'FAIL'}, "
                   f"alerts={'OK' if alert_result.success else 'FAIL'}")
        
        return results
    
    def _fetch_forecasts(self) -> FetchResult:
        """Fetch and store XML forecasts."""
        try:
            forecasts, metadata = self.fetcher.fetch_forecasts()
            
            # Store forecasts in database
            added = 0
            for forecast in forecasts:
                if self.database.insert_forecast(
                    city=forecast.city,
                    forecast_date=forecast.forecast_date,
                    data_date=forecast.data_date,
                    temp_min=forecast.temp_min,
                    temp_max=forecast.temp_max,
                    conditions=forecast.conditions,
                    conditions_code=forecast.conditions_code,
                    source_url=forecast.source_url,
                    content_hash=forecast.content_hash
                ):
                    added += 1
            
            # Update source status
            self.database.update_source_status(
                source_url=metadata.source_url,
                source_type="forecast",
                source_name=FORECAST_SOURCE["name"],
                success=metadata.success,
                data_quality=metadata.data_quality.value,
                is_fresh=metadata.is_fresh,
                entries_count=metadata.valid_entries,
                error_message=metadata.error_message,
                response_time_ms=metadata.response_time_ms
            )
            
            result = FetchResult(
                source_url=metadata.source_url,
                source_type="forecast",
                source_name=FORECAST_SOURCE["name"],
                success=metadata.success,
                entries_added=added,
                total_entries=metadata.entries_count,
                error_message=metadata.error_message,
                fetch_time=metadata.fetch_time,
                data_quality=metadata.data_quality.value,
                response_time_ms=metadata.response_time_ms
            )
            
            self._last_forecast_result = result
            return result
            
        except Exception as e:
            logger.error(f"Forecast fetch error: {e}")
            result = FetchResult(
                source_url=FORECAST_SOURCE["url"],
                source_type="forecast",
                source_name=FORECAST_SOURCE["name"],
                success=False,
                entries_added=0,
                total_entries=0,
                error_message=str(e),
                fetch_time=datetime.utcnow().isoformat(),
                data_quality=DataQuality.UNAVAILABLE.value,
                response_time_ms=0
            )
            self._last_forecast_result = result
            return result
    
    def _fetch_alerts(self) -> FetchResult:
        """Fetch and store RSS alerts."""
        try:
            alerts, metadata = self.fetcher.fetch_alerts()
            
            # Store alerts in database
            added = 0
            for alert in alerts:
                if self.database.insert_alert(
                    title=alert.title,
                    description=alert.description,
                    published_at=alert.published_at,
                    link=alert.link,
                    alert_level=alert.alert_level,
                    affected_zones=alert.affected_zones,
                    time_range=alert.time_range,
                    source_url=alert.source_url,
                    content_hash=alert.content_hash
                ):
                    added += 1
            
            # Update source status
            self.database.update_source_status(
                source_url=metadata.source_url,
                source_type="alert",
                source_name=ALERT_SOURCE["name"],
                success=metadata.success,
                data_quality=metadata.data_quality.value,
                is_fresh=metadata.is_fresh,
                entries_count=metadata.valid_entries,
                error_message=metadata.error_message,
                response_time_ms=metadata.response_time_ms
            )
            
            result = FetchResult(
                source_url=metadata.source_url,
                source_type="alert",
                source_name=ALERT_SOURCE["name"],
                success=metadata.success,
                entries_added=added,
                total_entries=metadata.entries_count,
                error_message=metadata.error_message,
                fetch_time=metadata.fetch_time,
                data_quality=metadata.data_quality.value,
                response_time_ms=metadata.response_time_ms
            )
            
            self._last_alert_result = result
            return result
            
        except Exception as e:
            logger.error(f"Alert fetch error: {e}")
            result = FetchResult(
                source_url=ALERT_SOURCE["url"],
                source_type="alert",
                source_name=ALERT_SOURCE["name"],
                success=False,
                entries_added=0,
                total_entries=0,
                error_message=str(e),
                fetch_time=datetime.utcnow().isoformat(),
                data_quality=DataQuality.UNAVAILABLE.value,
                response_time_ms=0
            )
            self._last_alert_result = result
            return result
    
    def _update_sync_status(self) -> None:
        """Update synchronization status."""
        now = datetime.utcnow().isoformat()
        
        forecast_ok = self._last_forecast_result and self._last_forecast_result.success
        alert_ok = self._last_alert_result and self._last_alert_result.success
        
        if forecast_ok and alert_ok:
            overall = "valid"
        elif forecast_ok or alert_ok:
            overall = "partial"
        else:
            overall = "unavailable"
        
        self._last_sync_status = SyncStatus(
            last_sync_time=now,
            forecast_healthy=forecast_ok,
            alert_healthy=alert_ok,
            overall_quality=overall,
            cities_available=len(self.fetcher.get_available_cities()),
            active_alerts=self.database.get_alert_count()
        )
    
    def start(self) -> None:
        """Start the scheduler."""
        if self._is_running:
            logger.warning("Scheduler already running")
            return
        
        # Forecast job (less frequent)
        self.scheduler.add_job(
            self._fetch_forecasts,
            trigger=IntervalTrigger(minutes=self.forecast_interval),
            id='forecast_job',
            name='ANM XML Forecast Fetch',
            max_instances=1,
            coalesce=True,
            replace_existing=True
        )
        
        # Alert job (more frequent)
        self.scheduler.add_job(
            self._fetch_alerts,
            trigger=IntervalTrigger(minutes=self.alert_interval),
            id='alert_job',
            name='ANM RSS Alert Fetch',
            max_instances=1,
            coalesce=True,
            replace_existing=True
        )
        
        self.scheduler.start()
        self._is_running = True
        
        logger.info(f"Scheduler started: forecasts every {self.forecast_interval}min, "
                   f"alerts every {self.alert_interval}min")
    
    def stop(self) -> None:
        """Stop the scheduler."""
        if not self._is_running:
            return
        self.scheduler.shutdown(wait=True)
        self._is_running = False
        logger.info("Scheduler stopped")
    
    def trigger_immediate_fetch(self) -> List[FetchResult]:
        """Trigger immediate fetch of all sources."""
        return self.fetch_all()
    
    def get_last_results(self) -> List[FetchResult]:
        """Get results from last fetch operations."""
        results = []
        if self._last_forecast_result:
            results.append(self._last_forecast_result)
        if self._last_alert_result:
            results.append(self._last_alert_result)
        return results
    
    def get_sync_status(self) -> Optional[SyncStatus]:
        """Get synchronization status."""
        return self._last_sync_status
    
    def get_available_cities(self) -> List[str]:
        """Get list of available cities."""
        return self.fetcher.get_available_cities()
    
    def get_scheduler_status(self) -> dict:
        """Get scheduler status information."""
        forecast_job = self.scheduler.get_job('forecast_job')
        alert_job = self.scheduler.get_job('alert_job')
        
        return {
            "is_running": self._is_running,
            "forecast_interval_minutes": self.forecast_interval,
            "alert_interval_minutes": self.alert_interval,
            "next_forecast_run": forecast_job.next_run_time.isoformat() if forecast_job and forecast_job.next_run_time else None,
            "next_alert_run": alert_job.next_run_time.isoformat() if alert_job and alert_job.next_run_time else None,
            "sync_status": asdict(self._last_sync_status) if self._last_sync_status else None,
        }
    
    @property
    def is_running(self) -> bool:
        return self._is_running
