# Romania Weather Dashboard

**A Dependable Systems Project** demonstrating heterogeneous data integration, synchronization, and trustworthiness monitoring using ANM (Romanian National Meteorology) data.

## Overview

This project integrates weather data from two heterogeneous sources:

| Source | Format | Type | Update Interval | Data |
|--------|--------|------|-----------------|------|
| **ANM Forecasts** | XML | State-based | ~hourly | 5-day forecasts for 10 Romanian regions |
| **ANM Alerts** | RSS | Event-based | ~10 min | Weather warnings (Yellow/Orange/Red) |

## Key Features

### Data Integration
- **XML Parsing**: Structured forecast data with min/max temperatures and conditions
- **RSS Parsing**: Event-based alerts with severity levels and affected zones
- **Different update patterns**: Forecasts update periodically, alerts update irregularly

### Dependability Features
- **Retry mechanism**: 3 retries with exponential backoff
- **Content hash verification**: SHA-256 integrity checks
- **Data quality assessment**: Valid, Partial, Stale, Invalid, Unavailable
- **Source health monitoring**: Reliability %, response time, consecutive failures

### Risk Detection
- Source unavailability detection
- Stale data warnings
- Low reliability alerts
- Consecutive failure tracking

## Architecture

```
ANM XML Forecasts ──┐
(State-based)       ├──► ANMFetcher ──► SQLite ──► FastAPI ──► React Dashboard
ANM RSS Alerts ─────┘                              │
(Event-based)                                      └──► Trustworthiness Metrics
```

## Running the Application

### Backend
```bash
cd weather-rss-feed-app
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn backend.api:app --host 0.0.0.0 --port 8000
```

### Frontend
```bash
cd frontend/weather-dashboard
npm install
npm run dev
```

### Access
- **Dashboard**: http://localhost:3000
- **API Docs**: http://localhost:8000/docs

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /forecast/{city}` | 5-day forecast for a city |
| `GET /alerts` | Active weather alerts |
| `GET /cities` | Available cities |
| `GET /status` | System health status |
| `POST /fetch` | Trigger manual data refresh |

## Available Regions

Forecasts are available for 10 regional weather stations:
- București (Capital)
- Cluj-Napoca (Northwest)
- Iași (Northeast)
- Constanța (Southeast Coast)
- Craiova (Southwest)
- Arad (West)
- Sibiu (Central)
- Botoșani (North)
- Rm. Vâlcea (South Central)
- Sulina (Danube Delta)

## Project Structure

```
weather-rss-feed-app/
├── backend/
│   ├── api.py          # FastAPI REST endpoints
│   ├── database.py     # SQLite persistence
│   ├── fetcher.py      # ANM XML/RSS fetching
│   └── scheduler.py    # Periodic data fetching
├── frontend/
│   └── weather-dashboard/
│       └── src/
│           └── App.jsx # React dashboard
├── requirements.txt
└── README.md
```

## Academic Value

This project demonstrates:
1. **Heterogeneous data integration** - Same institution (ANM), different formats (XML vs RSS)
2. **Synchronization challenges** - State-based vs event-based data
3. **Fault tolerance** - Graceful degradation when sources fail
4. **Trustworthiness monitoring** - Transparent system health reporting

## License

Educational project for Dependable Systems course.
