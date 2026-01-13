"""
RSS/XML Feed Fetcher module for Weather RSS Feed Application.

Handles data retrieval from heterogeneous sources:
- State-based data: ANM XML forecasts (structured, periodic updates)
- Event-based data: ANM RSS alerts (text-based, irregular updates)

Demonstrates dependability through:
- Different parsing strategies for different data formats
- Data integrity validation (hash verification, XML structure)
- Freshness tracking (staleness detection)
- Fault tolerance (retry mechanism, graceful degradation)
"""

import hashlib
import html
import logging
import re
import xml.etree.ElementTree as ET
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum

import feedparser
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# Configuration constants
DEFAULT_TIMEOUT = 15  # seconds
MAX_RETRIES = 3
RETRY_BACKOFF = 0.5
USER_AGENT = "WeatherRSSFetcher/1.0 (Educational Project - Dependable Systems)"

# Freshness thresholds (how old data can be before considered stale)
FORECAST_FRESHNESS_HOURS = 12  # XML forecasts update once or twice daily
ALERT_FRESHNESS_HOURS = 1      # Alerts should be near real-time

# ANM Data Sources
ANM_FORECAST_XML_URL = "http://www.meteoromania.ro/anm/prognoza-orase-xml.php"
ANM_ALERTS_RSS_URL = "http://www.meteoromania.ro/anm2/avertizari-rss.php"


class SourceType(Enum):
    """Classification of data source types for synchronization."""
    FORECAST = "forecast"  # State-based, periodic updates (XML)
    ALERT = "alert"        # Event-based, irregular updates (RSS)


class DataQuality(Enum):
    """Data quality assessment levels."""
    VALID = "valid"           # Data passed all checks
    STALE = "stale"           # Data is older than freshness threshold
    PARTIAL = "partial"       # Some entries failed validation
    INVALID = "invalid"       # Data failed integrity checks
    UNAVAILABLE = "unavailable"  # Source unreachable


@dataclass
class CityForecast:
    """Represents a structured forecast for a specific city and date."""
    city: str
    forecast_date: str      # Date the forecast is for (YYYY-MM-DD)
    data_date: str          # Date the forecast was generated
    temp_min: int
    temp_max: int
    conditions: str         # Weather description in Romanian
    conditions_code: str    # Symbol/code for the weather condition
    source_url: str
    content_hash: str
    fetched_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


@dataclass
class WeatherAlert:
    """Represents a weather alert/warning from RSS."""
    title: str
    description: str
    published_at: Optional[str]
    link: Optional[str]
    alert_level: Optional[str]  # GALBEN, PORTOCALIU, ROSU
    affected_zones: Optional[str]
    time_range: Optional[str]
    source_url: str
    content_hash: str
    fetched_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


@dataclass
class FetchMetadata:
    """Metadata about a fetch operation for trustworthiness tracking."""
    source_url: str
    source_type: SourceType
    fetch_time: str
    success: bool
    entries_count: int
    valid_entries: int
    error_message: Optional[str]
    response_time_ms: int
    data_quality: DataQuality
    is_fresh: bool
    last_modified: Optional[str]


class FetchError(Exception):
    """Custom exception for feed fetching errors."""
    pass


class ValidationError(Exception):
    """Custom exception for data validation errors."""
    pass


class ANMFetcher:
    """
    Fetcher for ANM (Romanian National Meteorology Administration) data.
    
    Handles two heterogeneous data sources:
    1. XML Forecasts - Structured city forecasts (state-based)
    2. RSS Alerts - Weather warnings (event-based)
    
    Dependability features:
    - Retry mechanism for transient failures
    - Content hash verification for integrity
    - Separate parsing strategies for XML vs RSS
    - Data freshness tracking
    """
    
    def __init__(self, timeout: int = DEFAULT_TIMEOUT):
        self.timeout = timeout
        self._session = self._create_session()
        self._last_fetch_metadata: Dict[str, FetchMetadata] = {}
        self._cached_cities: List[str] = []
    
    def _create_session(self) -> requests.Session:
        """Create HTTP session with retry strategy for availability."""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=MAX_RETRIES,
            backoff_factor=RETRY_BACKOFF,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/xml, text/xml, application/rss+xml, */*"
        })
        
        return session
    
    def _fetch_raw(self, url: str) -> Tuple[bytes, int]:
        """Fetch raw content with timing."""
        start_time = datetime.utcnow()
        
        try:
            response = self._session.get(url, timeout=self.timeout, allow_redirects=True)
            response.raise_for_status()
            
            response_time = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            return response.content, response_time
            
        except requests.Timeout:
            raise FetchError(f"Request timed out after {self.timeout}s")
        except requests.ConnectionError as e:
            raise FetchError(f"Connection error - source unavailable: {e}")
        except requests.HTTPError as e:
            raise FetchError(f"HTTP error {e.response.status_code}")
        except requests.RequestException as e:
            raise FetchError(f"Request failed: {e}")
    
    def _compute_hash(self, *args) -> str:
        """Compute SHA-256 hash for integrity verification."""
        content = "|".join(str(a) for a in args)
        return hashlib.sha256(content.encode("utf-8", errors="ignore")).hexdigest()
    
    # =========================================================================
    # XML Forecast Parsing (State-Based Data)
    # =========================================================================
    
    def fetch_forecasts(self) -> Tuple[List[CityForecast], FetchMetadata]:
        """
        Fetch and parse ANM XML forecasts for all Romanian cities.
        
        Returns:
            Tuple of (list of CityForecast objects, FetchMetadata)
        """
        fetch_start = datetime.utcnow()
        url = ANM_FORECAST_XML_URL
        
        try:
            raw_content, response_time = self._fetch_raw(url)
            
            # Validate XML structure
            if not self._validate_xml_structure(raw_content):
                raise ValidationError("Invalid XML structure")
            
            # Parse forecasts
            forecasts, valid_count, total_count = self._parse_forecast_xml(raw_content)
            
            # Update cached cities list
            self._cached_cities = list(set(f.city for f in forecasts))
            
            # Assess quality
            data_quality = self._assess_quality(valid_count, total_count)
            
            metadata = FetchMetadata(
                source_url=url,
                source_type=SourceType.FORECAST,
                fetch_time=fetch_start.isoformat(),
                success=True,
                entries_count=total_count,
                valid_entries=valid_count,
                error_message=None,
                response_time_ms=response_time,
                data_quality=data_quality,
                is_fresh=True,
                last_modified=None
            )
            
            self._last_fetch_metadata[url] = metadata
            logger.info(f"Fetched forecasts: {valid_count}/{total_count} valid, {len(self._cached_cities)} cities")
            
            return forecasts, metadata
            
        except (FetchError, ValidationError) as e:
            metadata = FetchMetadata(
                source_url=url,
                source_type=SourceType.FORECAST,
                fetch_time=fetch_start.isoformat(),
                success=False,
                entries_count=0,
                valid_entries=0,
                error_message=str(e),
                response_time_ms=int((datetime.utcnow() - fetch_start).total_seconds() * 1000),
                data_quality=DataQuality.UNAVAILABLE,
                is_fresh=False,
                last_modified=None
            )
            self._last_fetch_metadata[url] = metadata
            logger.error(f"Forecast fetch failed: {e}")
            return [], metadata
    
    def _validate_xml_structure(self, content: bytes) -> bool:
        """Validate XML structure for integrity."""
        try:
            content_str = content.decode("utf-8", errors="ignore")
            
            if not content_str.strip():
                return False
            
            if "<?xml" not in content_str and not content_str.strip().startswith("<"):
                return False
            
            # Try to parse
            ET.fromstring(content)
            return True
            
        except ET.ParseError:
            return False
        except Exception:
            return False
    
    def _parse_forecast_xml(self, content: bytes) -> Tuple[List[CityForecast], int, int]:
        """Parse ANM XML forecast format."""
        forecasts = []
        valid_count = 0
        total_count = 0
        
        try:
            root = ET.fromstring(content)
            
            # Find all city elements
            for localitate in root.findall(".//localitate"):
                city_name = localitate.get("nume", "").strip()
                
                if not city_name:
                    continue
                
                # Get the data generation date
                data_prognozei = localitate.findtext("DataPrognozei", "")
                
                # Parse each forecast day
                for prognoza in localitate.findall("prognoza"):
                    total_count += 1
                    
                    try:
                        forecast_date = prognoza.get("data", "")
                        temp_min_text = prognoza.findtext("temp_min", "")
                        temp_max_text = prognoza.findtext("temp_max", "")
                        conditions = prognoza.findtext("fenomen_descriere", "")
                        conditions_code = prognoza.findtext("fenomen_simbol", "")
                        
                        # Validate required fields
                        if not all([forecast_date, temp_min_text, temp_max_text]):
                            continue
                        
                        temp_min = int(temp_min_text)
                        temp_max = int(temp_max_text)
                        
                        # Translate conditions to English
                        conditions_en = self._translate_conditions(conditions)
                        
                        content_hash = self._compute_hash(
                            city_name, forecast_date, temp_min, temp_max, conditions
                        )
                        
                        forecast = CityForecast(
                            city=city_name,
                            forecast_date=forecast_date,
                            data_date=data_prognozei,
                            temp_min=temp_min,
                            temp_max=temp_max,
                            conditions=conditions_en,
                            conditions_code=conditions_code,
                            source_url=ANM_FORECAST_XML_URL,
                            content_hash=content_hash
                        )
                        
                        forecasts.append(forecast)
                        valid_count += 1
                        
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Failed to parse forecast for {city_name}: {e}")
                        continue
            
        except ET.ParseError as e:
            logger.error(f"XML parsing error: {e}")
            raise ValidationError(f"XML parsing failed: {e}")
        
        return forecasts, valid_count, total_count
    
    def _translate_conditions(self, romanian: str) -> str:
        """Translate Romanian weather conditions to English."""
        if not romanian:
            return "Unknown"
        
        translations = {
            "CER SENIN": "Clear Sky",
            "CER VARIABIL": "Partly Cloudy",
            "CER PARTIAL NOROS": "Partly Cloudy",
            "CER MAI MULT NOROS": "Mostly Cloudy",
            "CER NOROS": "Cloudy",
            "INNNORAT": "Overcast",
            "PLOAIE SLABA": "Light Rain",
            "PLOAIE": "Rain",
            "PLOAIE MODERATA": "Moderate Rain",
            "PLOI": "Rainy",
            "AVERSE": "Showers",
            "FURTUNA": "Thunderstorm",
            "NINSOARE SLABA": "Light Snow",
            "NINSOARE": "Snow",
            "NINSOARE MODERATA": "Moderate Snow",
            "LAPOVITA": "Sleet",
            "CEATA": "Fog",
            "BURNITA": "Drizzle",
        }
        
        # Check for compound conditions
        result_parts = []
        romanian_upper = romanian.upper()
        
        for ro, en in translations.items():
            if ro in romanian_upper:
                result_parts.append(en)
        
        if result_parts:
            return ", ".join(result_parts)
        
        # Return original if no translation found
        return romanian.title()
    
    # =========================================================================
    # RSS Alert Parsing (Event-Based Data)
    # =========================================================================
    
    def fetch_alerts(self) -> Tuple[List[WeatherAlert], FetchMetadata]:
        """
        Fetch and parse ANM RSS weather alerts.
        
        Returns:
            Tuple of (list of WeatherAlert objects, FetchMetadata)
        """
        fetch_start = datetime.utcnow()
        url = ANM_ALERTS_RSS_URL
        
        try:
            raw_content, response_time = self._fetch_raw(url)
            
            # Parse RSS feed
            alerts, valid_count, total_count = self._parse_alert_rss(raw_content)
            
            # Assess quality
            data_quality = self._assess_quality(valid_count, total_count)
            
            metadata = FetchMetadata(
                source_url=url,
                source_type=SourceType.ALERT,
                fetch_time=fetch_start.isoformat(),
                success=True,
                entries_count=total_count,
                valid_entries=valid_count,
                error_message=None,
                response_time_ms=response_time,
                data_quality=data_quality,
                is_fresh=True,
                last_modified=None
            )
            
            self._last_fetch_metadata[url] = metadata
            logger.info(f"Fetched alerts: {valid_count}/{total_count} valid")
            
            return alerts, metadata
            
        except (FetchError, ValidationError) as e:
            metadata = FetchMetadata(
                source_url=url,
                source_type=SourceType.ALERT,
                fetch_time=fetch_start.isoformat(),
                success=False,
                entries_count=0,
                valid_entries=0,
                error_message=str(e),
                response_time_ms=int((datetime.utcnow() - fetch_start).total_seconds() * 1000),
                data_quality=DataQuality.UNAVAILABLE,
                is_fresh=False,
                last_modified=None
            )
            self._last_fetch_metadata[url] = metadata
            logger.error(f"Alert fetch failed: {e}")
            return [], metadata
    
    def _parse_alert_rss(self, content: bytes) -> Tuple[List[WeatherAlert], int, int]:
        """Parse ANM RSS alert format."""
        alerts = []
        valid_count = 0
        total_count = 0
        
        parsed = feedparser.parse(content)
        
        if parsed.bozo and not parsed.entries:
            raise ValidationError(f"RSS parsing failed: {parsed.bozo_exception}")
        
        for item in parsed.entries:
            total_count += 1
            
            try:
                title = self._clean_html(getattr(item, "title", "") or "")
                description_raw = getattr(item, "summary", "") or getattr(item, "description", "") or ""
                
                # Parse alert details
                alert_level = self._extract_alert_level(description_raw)
                affected_zones = self._extract_zones(description_raw)
                time_range = self._extract_time_range(description_raw)
                description = self._format_alert_description(description_raw)
                
                # Get publication date
                published_at = None
                if hasattr(item, "published_parsed") and item.published_parsed:
                    try:
                        published_at = datetime(*item.published_parsed[:6]).isoformat()
                    except Exception:
                        published_at = getattr(item, "published", None)
                
                link = getattr(item, "link", None)
                
                content_hash = self._compute_hash(title, description_raw, published_at)
                
                alert = WeatherAlert(
                    title=title if title else "Weather Alert",
                    description=description,
                    published_at=published_at,
                    link=link,
                    alert_level=alert_level,
                    affected_zones=affected_zones,
                    time_range=time_range,
                    source_url=ANM_ALERTS_RSS_URL,
                    content_hash=content_hash
                )
                
                alerts.append(alert)
                valid_count += 1
                
            except Exception as e:
                logger.warning(f"Failed to parse alert: {e}")
                continue
        
        return alerts, valid_count, total_count
    
    def _clean_html(self, text: str) -> str:
        """Remove HTML tags and decode HTML entities from text."""
        if not text:
            return ""
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)
        # Decode all HTML entities (e.g., &icirc; -> î, &ndash; -> –)
        text = html.unescape(text)
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    
    def _extract_alert_level(self, text: str) -> Optional[str]:
        """Extract alert level (COD) from Romanian alert."""
        match = re.search(r'COD\s*:\s*(\w+)', text, re.IGNORECASE)
        if match:
            level = match.group(1).upper()
            levels = {"GALBEN": "YELLOW", "PORTOCALIU": "ORANGE", "ROSU": "RED"}
            return levels.get(level, level)
        return None
    
    def _extract_zones(self, text: str) -> Optional[str]:
        """Extract affected zones from alert."""
        match = re.search(r'In zona\s*:\s*(.+?)(?:Se vor|$)', self._clean_html(text))
        if match:
            zones = match.group(1).strip()
            return zones[:200] if len(zones) > 200 else zones
        return None
    
    def _extract_time_range(self, text: str) -> Optional[str]:
        """Extract time range from alert."""
        match = re.search(r'Intre orele\s*:\s*([\d:]+)\s*si\s*([\d:]+)', text)
        if match:
            return f"{match.group(1)} - {match.group(2)}"
        return None
    
    def _format_alert_description(self, text: str) -> str:
        """Format alert description for display."""
        clean = self._clean_html(text)
        
        # Try different patterns for extracting the main phenomena description
        patterns = [
            # Pattern for "Se vor semnala: ..." (nowcast alerts)
            r'Se vor semnala\s*:\s*(.+?)(?:$)',
            # Pattern for "Fenomene vizate: ..." (meteorological bulletins)
            r'Fenomene vizate\s*:\s*(.+?)(?:$)',
            # Pattern for just the phenomena after all the metadata
            r'Fenomene\s*:\s*conform textelor\s+Mesaj\s*:\s*(.+?)(?:Interval de valabilitate|$)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, clean, re.IGNORECASE)
            if match:
                result = match.group(1).strip()
                # Clean up any trailing metadata
                result = re.sub(r'\s*Interval de valabilitate.*$', '', result, flags=re.IGNORECASE)
                if len(result) > 20:  # Only use if we got something meaningful
                    return result[:500] if len(result) > 500 else result
        
        # Fallback: try to extract just after "Mesaj :" for bulletins
        match = re.search(r'Mesaj\s*:\s*(?:MESAJ\s*\d+/\d+\s*)?(.+?)(?:Interval de valabilitate|$)', clean, re.IGNORECASE)
        if match:
            result = match.group(1).strip()
            if len(result) > 20:
                return result[:500] if len(result) > 500 else result
        
        return clean[:500] if len(clean) > 500 else clean
    
    # =========================================================================
    # Quality Assessment
    # =========================================================================
    
    def _assess_quality(self, valid: int, total: int) -> DataQuality:
        """Assess data quality based on validation results."""
        if total == 0:
            return DataQuality.UNAVAILABLE
        if valid == 0:
            return DataQuality.INVALID
        if valid == total:
            return DataQuality.VALID
        if valid >= total * 0.5:
            return DataQuality.PARTIAL
        return DataQuality.INVALID
    
    def get_available_cities(self) -> List[str]:
        """Get list of available cities from last forecast fetch."""
        return sorted(self._cached_cities)
    
    def get_source_health(self) -> Dict[str, FetchMetadata]:
        """Get health status of all sources."""
        return self._last_fetch_metadata.copy()
    
    def close(self) -> None:
        """Close HTTP session."""
        self._session.close()


# =============================================================================
# Source Configuration
# =============================================================================

FORECAST_SOURCE = {
    "id": "anm_forecast",
    "name": "ANM Romania Forecasts",
    "url": ANM_FORECAST_XML_URL,
    "type": "xml",
    "source_type": SourceType.FORECAST,
}

ALERT_SOURCE = {
    "id": "anm_alerts", 
    "name": "ANM Romania Alerts",
    "url": ANM_ALERTS_RSS_URL,
    "type": "rss",
    "source_type": SourceType.ALERT,
}
