"""
REST API module for Weather RSS Feed Application.

Provides endpoints for:
- City forecast search and retrieval (from ANM XML)
- Weather alerts (from ANM RSS)
- System trustworthiness monitoring
"""

import logging
import os
from typing import List, Optional
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .database import Database
from .scheduler import WeatherScheduler, FetchResult

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# =============================================================================
# Pydantic Models
# =============================================================================

class CityForecast(BaseModel):
    id: int
    city: str
    forecast_date: str
    data_date: Optional[str]
    temp_min: int
    temp_max: int
    conditions: Optional[str]
    conditions_code: Optional[str]
    fetched_at: str


class WeatherAlert(BaseModel):
    id: int
    title: str
    description: Optional[str]
    published_at: Optional[str]
    link: Optional[str]
    alert_level: Optional[str]
    affected_zones: Optional[str]
    time_range: Optional[str]
    fetched_at: str


class SourceHealth(BaseModel):
    source_url: str
    source_type: str
    source_name: Optional[str]
    status: str
    data_quality: str
    is_fresh: bool
    last_fetch_at: Optional[str]
    last_success_at: Optional[str]
    fetch_count: int
    success_count: int
    error_count: int
    reliability_percent: float
    avg_response_time_ms: int
    consecutive_failures: int
    entries_count: int
    last_error: Optional[str]


class SystemStatus(BaseModel):
    status: str
    uptime: str
    scheduler_running: bool
    data_quality: str
    forecast_healthy: bool
    alert_healthy: bool
    cities_available: int
    active_alerts: int
    total_forecasts: int
    source_health: List[SourceHealth]
    risks: List[str]


class FetchResultModel(BaseModel):
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


class HealthResponse(BaseModel):
    status: str
    timestamp: str
    database: str
    scheduler: str
    risks: List[str]


# =============================================================================
# Global State
# =============================================================================

db: Optional[Database] = None
scheduler: Optional[WeatherScheduler] = None
start_time: Optional[datetime] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    global db, scheduler, start_time
    
    logger.info("Starting Weather RSS Feed Application...")
    start_time = datetime.utcnow()
    
    # Initialize database
    db = Database()
    logger.info("Database initialized")
    
    # Initialize scheduler
    scheduler = WeatherScheduler(database=db)
    
    # Initial data fetch
    try:
        logger.info("Performing initial data fetch...")
        scheduler.trigger_immediate_fetch()
    except Exception as e:
        logger.warning(f"Initial fetch warning: {e}")
    
    # Start scheduler
    scheduler.start()
    logger.info("Scheduler started")
    
    yield
    
    # Shutdown
    logger.info("Shutting down...")
    if scheduler:
        scheduler.stop()
    if db:
        db.close()
    logger.info("Shutdown complete")


# =============================================================================
# FastAPI Application
# =============================================================================

app = FastAPI(
    title="Romania Weather API",
    description="Weather forecasts and alerts from ANM (Romanian National Meteorology)",
    version="2.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# Utility Functions
# =============================================================================

def detect_risks() -> List[str]:
    """Detect system risks."""
    risks = []
    
    if not db or not scheduler:
        return ["System not initialized"]
    
    sync_status = scheduler.get_sync_status()
    if sync_status:
        if not sync_status.forecast_healthy:
            risks.append("Forecast source unavailable")
        if not sync_status.alert_healthy:
            risks.append("Alert source unavailable")
    
    source_statuses = db.get_system_status()
    for source in source_statuses:
        if source.get("consecutive_failures", 0) >= 3:
            risks.append(f"Source failing repeatedly: {source.get('source_name', 'unknown')}")
        if source.get("reliability_percent", 100) < 80:
            risks.append(f"Low reliability: {source.get('source_name', 'unknown')}")
    
    return risks


def get_uptime() -> str:
    """Get formatted uptime string."""
    if not start_time:
        return "N/A"
    delta = datetime.utcnow() - start_time
    hours, remainder = divmod(int(delta.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours}h {minutes}m {seconds}s"


# =============================================================================
# API Endpoints - Info
# =============================================================================

@app.get("/", tags=["Info"])
async def root():
    """API information."""
    return {
        "name": "Romania Weather API",
        "version": "2.0.0",
        "description": "Weather data from ANM Romania",
        "sources": {
            "forecasts": "ANM XML - State-based city forecasts",
            "alerts": "ANM RSS - Event-based weather warnings"
        }
    }


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Health check endpoint."""
    risks = detect_risks()
    
    return HealthResponse(
        status="healthy" if not risks else "degraded",
        timestamp=datetime.utcnow().isoformat(),
        database="connected" if db else "disconnected",
        scheduler="running" if scheduler and scheduler.is_running else "stopped",
        risks=risks
    )


# =============================================================================
# API Endpoints - Cities & Forecasts
# =============================================================================

@app.get("/cities", tags=["Cities"])
async def get_cities():
    """Get list of all available Romanian cities."""
    if not db:
        raise HTTPException(status_code=503, detail="Database not available")
    
    cities = db.get_all_cities()
    return {
        "cities": cities,
        "count": len(cities)
    }


@app.get("/cities/search", tags=["Cities"])
async def search_cities(q: str = Query(..., min_length=1, description="Search query")):
    """Search cities by name prefix."""
    if not db:
        raise HTTPException(status_code=503, detail="Database not available")
    
    cities = db.search_cities(q)
    return {
        "query": q,
        "cities": cities,
        "count": len(cities)
    }


@app.get("/forecast/{city}", response_model=List[CityForecast], tags=["Forecasts"])
async def get_city_forecast(city: str):
    """Get 5-day forecast for a specific city."""
    if not db:
        raise HTTPException(status_code=503, detail="Database not available")
    
    forecasts = db.get_city_forecast(city)
    
    if not forecasts:
        # Try case-insensitive search
        all_cities = db.get_all_cities()
        matching = [c for c in all_cities if c.lower() == city.lower()]
        if matching:
            forecasts = db.get_city_forecast(matching[0])
    
    if not forecasts:
        raise HTTPException(status_code=404, detail=f"No forecast found for city: {city}")
    
    return [CityForecast(**f) for f in forecasts]


@app.get("/forecast", tags=["Forecasts"])
async def get_default_forecast():
    """Get forecast for Bucharest (default city)."""
    if not db:
        raise HTTPException(status_code=503, detail="Database not available")
    
    forecasts = db.get_city_forecast("Bucuresti")
    
    if not forecasts:
        raise HTTPException(status_code=404, detail="Bucharest forecast not available")
    
    return {
        "city": "Bucuresti",
        "forecasts": [CityForecast(**f) for f in forecasts]
    }


# =============================================================================
# API Endpoints - Alerts
# =============================================================================

@app.get("/alerts", response_model=List[WeatherAlert], tags=["Alerts"])
async def get_alerts(
    limit: int = Query(default=50, ge=1, le=100),
    level: Optional[str] = Query(default=None, description="Filter by level: YELLOW, ORANGE, RED")
):
    """Get active weather alerts for Romania."""
    if not db:
        raise HTTPException(status_code=503, detail="Database not available")
    
    if level:
        alerts = db.get_alerts_by_level(level.upper())
    else:
        alerts = db.get_active_alerts(limit)
    
    return [WeatherAlert(**a) for a in alerts]


@app.get("/alerts/count", tags=["Alerts"])
async def get_alert_count():
    """Get count of active alerts by level."""
    if not db:
        raise HTTPException(status_code=503, detail="Database not available")
    
    all_alerts = db.get_active_alerts(100)
    
    counts = {"YELLOW": 0, "ORANGE": 0, "RED": 0, "OTHER": 0}
    for alert in all_alerts:
        level = alert.get("alert_level", "OTHER")
        if level in counts:
            counts[level] += 1
        else:
            counts["OTHER"] += 1
    
    return {
        "total": len(all_alerts),
        "by_level": counts
    }


# =============================================================================
# API Endpoints - System Status
# =============================================================================

@app.get("/status", response_model=SystemStatus, tags=["Status"])
async def get_system_status():
    """Get comprehensive system status."""
    if not db or not scheduler:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    sync_status = scheduler.get_sync_status()
    source_health = db.get_system_status()
    data_summary = db.get_data_summary()
    risks = detect_risks()
    
    # Determine overall status
    if not risks:
        status = "healthy"
    elif len(risks) <= 1:
        status = "degraded"
    else:
        status = "unhealthy"
    
    return SystemStatus(
        status=status,
        uptime=get_uptime(),
        scheduler_running=scheduler.is_running,
        data_quality=sync_status.overall_quality if sync_status else "unknown",
        forecast_healthy=sync_status.forecast_healthy if sync_status else False,
        alert_healthy=sync_status.alert_healthy if sync_status else False,
        cities_available=data_summary["city_count"],
        active_alerts=data_summary["alert_entries"],
        total_forecasts=data_summary["forecast_entries"],
        source_health=[SourceHealth(**s) for s in source_health],
        risks=risks
    )


@app.get("/sources", tags=["Status"])
async def get_sources():
    """Get health status of data sources."""
    if not db:
        raise HTTPException(status_code=503, detail="Database not available")
    
    return {
        "sources": db.get_system_status(),
        "summary": db.get_data_summary()
    }


# =============================================================================
# API Endpoints - Admin
# =============================================================================

@app.post("/fetch", response_model=List[FetchResultModel], tags=["Admin"])
async def trigger_fetch():
    """Manually trigger data fetch from all sources."""
    if not scheduler:
        raise HTTPException(status_code=503, detail="Scheduler not available")
    
    try:
        results = scheduler.trigger_immediate_fetch()
        return [FetchResultModel(**r.__dict__) for r in results]
    except Exception as e:
        logger.error(f"Fetch error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/fetch/results", response_model=List[FetchResultModel], tags=["Admin"])
async def get_fetch_results():
    """Get results from last fetch operation."""
    if not scheduler:
        raise HTTPException(status_code=503, detail="Scheduler not available")
    
    results = scheduler.get_last_results()
    return [FetchResultModel(**r.__dict__) for r in results]


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "backend.api:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8000")),
        reload=os.getenv("DEBUG", "false").lower() == "true"
    )
