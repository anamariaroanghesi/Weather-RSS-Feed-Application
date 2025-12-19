import { useState, useEffect, useCallback } from 'react';

/**
 * Romania Weather Dashboard - Dependable Systems Demonstration
 * 
 * Data Integration:
 * - ANM XML Forecasts (state-based) - 5-day forecasts for all Romanian cities
 * - ANM RSS Alerts (event-based) - Weather warnings and alerts
 */

const POLL_INTERVAL = 60000; // 1 minute

// =============================================================================
// Quality Badge Component
// =============================================================================

function QualityBadge({ quality }) {
  const config = {
    valid: { label: 'Valid', color: '#22c55e', bg: 'rgba(34, 197, 94, 0.15)' },
    partial: { label: 'Partial', color: '#f59e0b', bg: 'rgba(245, 158, 11, 0.15)' },
    unavailable: { label: 'Unavailable', color: '#6b7280', bg: 'rgba(107, 114, 128, 0.15)' },
  }[quality?.toLowerCase()] || { label: 'Unknown', color: '#9ca3af', bg: 'rgba(156, 163, 175, 0.15)' };

  return (
    <span style={{
      display: 'inline-flex', alignItems: 'center', gap: '4px',
      padding: '4px 10px', borderRadius: '4px', fontSize: '0.75rem',
      fontWeight: 600, color: config.color, backgroundColor: config.bg,
      textTransform: 'uppercase',
    }}>
      <span style={{ width: '6px', height: '6px', borderRadius: '50%', backgroundColor: config.color }} />
      {config.label}
    </span>
  );
}

// =============================================================================
// City Selector Component (Dropdown)
// =============================================================================

// ANM provides forecasts for these 10 regional cities
const AVAILABLE_CITIES = [
  { id: 'Bucuresti', name: 'București', region: 'Capital' },
  { id: 'Cluj-Napoca', name: 'Cluj-Napoca', region: 'Northwest' },
  { id: 'Iasi', name: 'Iași', region: 'Northeast' },
  { id: 'Constanta', name: 'Constanța', region: 'Southeast Coast' },
  { id: 'Craiova', name: 'Craiova', region: 'Southwest' },
  { id: 'Arad', name: 'Arad', region: 'West' },
  { id: 'Sibiu', name: 'Sibiu', region: 'Central' },
  { id: 'Botosani', name: 'Botoșani', region: 'North' },
  { id: 'Rm. Valcea', name: 'Rm. Vâlcea', region: 'South Central' },
  { id: 'Sulina', name: 'Sulina', region: 'Danube Delta' },
];

function CitySelector({ selectedCity, onCitySelect }) {
  return (
    <div style={{ marginBottom: '20px' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '16px', flexWrap: 'wrap' }}>
        <label style={{ 
          fontSize: '0.9rem', 
          fontWeight: 600, 
          color: '#374151',
          display: 'flex',
          alignItems: 'center',
          gap: '8px',
        }}>
          📍 Select Region:
        </label>
        
        <select
          value={selectedCity}
          onChange={(e) => onCitySelect(e.target.value)}
          style={{
            padding: '10px 40px 10px 16px',
            fontSize: '1rem',
            fontWeight: 500,
            border: '2px solid #e5e7eb',
            borderRadius: '10px',
            backgroundColor: 'white',
            color: '#111827',
            cursor: 'pointer',
            outline: 'none',
            appearance: 'none',
            backgroundImage: `url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 12 12'%3E%3Cpath fill='%236b7280' d='M6 8L1 3h10z'/%3E%3C/svg%3E")`,
            backgroundRepeat: 'no-repeat',
            backgroundPosition: 'right 12px center',
            minWidth: '200px',
          }}
        >
          {AVAILABLE_CITIES.map(city => (
            <option key={city.id} value={city.id}>
              {city.name} ({city.region})
            </option>
          ))}
        </select>
        
        <span style={{ 
          fontSize: '0.8rem', 
          color: '#6b7280',
          backgroundColor: '#f3f4f6',
          padding: '6px 12px',
          borderRadius: '6px',
        }}>
          10 regional weather stations from ANM
        </span>
      </div>
    </div>
  );
}

// =============================================================================
// Forecast Card Component
// =============================================================================

function ForecastCard({ forecast }) {
  const formatDate = (dateStr) => {
    try {
      const date = new Date(dateStr);
      return date.toLocaleDateString('en-GB', { weekday: 'short', day: 'numeric', month: 'short' });
    } catch {
      return dateStr;
    }
  };

  const getWeatherIcon = (conditions) => {
    const c = (conditions || '').toLowerCase();
    if (c.includes('clear') || c.includes('sunny')) return '☀️';
    if (c.includes('partly')) return '⛅';
    if (c.includes('cloudy') || c.includes('overcast')) return '☁️';
    if (c.includes('rain') || c.includes('shower')) return '🌧️';
    if (c.includes('thunder') || c.includes('storm')) return '⛈️';
    if (c.includes('snow')) return '❄️';
    if (c.includes('fog') || c.includes('mist')) return '🌫️';
    return '🌤️';
  };

  return (
    <div style={{
      backgroundColor: 'white',
      borderRadius: '12px',
      padding: '20px',
      boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
      textAlign: 'center',
      minWidth: '140px',
    }}>
      <div style={{ fontSize: '0.9rem', fontWeight: 600, color: '#374151', marginBottom: '8px' }}>
        {formatDate(forecast.forecast_date)}
      </div>
      <div style={{ fontSize: '2.5rem', marginBottom: '8px' }}>
        {getWeatherIcon(forecast.conditions)}
      </div>
      <div style={{ display: 'flex', justifyContent: 'center', gap: '8px', marginBottom: '8px' }}>
        <span style={{ fontSize: '1.3rem', fontWeight: 700, color: '#1e40af' }}>
          {forecast.temp_max}°
        </span>
        <span style={{ fontSize: '1.1rem', color: '#6b7280' }}>
          {forecast.temp_min}°
        </span>
      </div>
      <div style={{ fontSize: '0.8rem', color: '#6b7280' }}>
        {forecast.conditions || 'N/A'}
      </div>
    </div>
  );
}

// =============================================================================
// Alert Card Component
// =============================================================================

function AlertCard({ alert }) {
  const levelConfig = {
    YELLOW: { emoji: '🟡', color: '#ca8a04', bg: 'rgba(202, 138, 4, 0.1)' },
    ORANGE: { emoji: '🟠', color: '#ea580c', bg: 'rgba(234, 88, 12, 0.1)' },
    RED: { emoji: '🔴', color: '#dc2626', bg: 'rgba(220, 38, 38, 0.1)' },
  }[alert.alert_level] || { emoji: '⚠️', color: '#6b7280', bg: 'rgba(107, 114, 128, 0.1)' };

  const formatTime = (timeStr) => {
    if (!timeStr) return '';
    try {
      return new Date(timeStr).toLocaleString('en-GB', {
        day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit'
      });
    } catch {
      return timeStr;
    }
  };

  return (
    <div style={{
      backgroundColor: levelConfig.bg,
      border: `1px solid ${levelConfig.color}33`,
      borderRadius: '12px',
      padding: '16px',
      marginBottom: '12px',
    }}>
      <div style={{
        display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start',
        marginBottom: '12px',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <span style={{ fontSize: '1.5rem' }}>{levelConfig.emoji}</span>
          <div>
            <div style={{ fontWeight: 600, color: levelConfig.color }}>
              {alert.alert_level || 'Weather'} Alert
            </div>
            {alert.time_range && (
              <div style={{ fontSize: '0.8rem', color: '#6b7280' }}>
                ⏰ {alert.time_range}
              </div>
            )}
          </div>
        </div>
        <span style={{
          fontSize: '0.7rem', padding: '2px 8px', borderRadius: '4px',
          backgroundColor: `${levelConfig.color}22`, color: levelConfig.color, fontWeight: 600,
        }}>
          ALERT
        </span>
      </div>
      
      {alert.description && (
        <p style={{
          margin: '0 0 12px 0', fontSize: '0.9rem', color: '#374151', lineHeight: 1.6,
        }}>
          {alert.description}
        </p>
      )}
      
      {alert.affected_zones && (
        <div style={{
          fontSize: '0.8rem', color: '#6b7280', padding: '8px 12px',
          backgroundColor: 'rgba(255,255,255,0.5)', borderRadius: '6px',
        }}>
          📍 <strong>Zones:</strong> {alert.affected_zones}
        </div>
      )}
      
      {alert.published_at && (
        <div style={{ marginTop: '8px', fontSize: '0.75rem', color: '#9ca3af' }}>
          Published: {formatTime(alert.published_at)}
        </div>
      )}
    </div>
  );
}

// =============================================================================
// Source Health Panel Component
// =============================================================================

function SourceHealthPanel({ sources }) {
  if (!sources || sources.length === 0) return null;

  return (
    <div style={{
      backgroundColor: 'white',
      borderRadius: '12px',
      padding: '20px',
      boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
    }}>
      <h3 style={{
        margin: '0 0 16px 0', fontSize: '1rem', fontWeight: 600,
        display: 'flex', alignItems: 'center', gap: '8px',
      }}>
        📡 Data Sources
      </h3>
      
      {sources.map((source, idx) => (
        <div key={idx} style={{
          padding: '12px',
          backgroundColor: source.status === 'ok' ? 'rgba(34, 197, 94, 0.05)' : 'rgba(239, 68, 68, 0.05)',
          borderRadius: '8px',
          marginBottom: '8px',
        }}>
          <div style={{
            display: 'flex', justifyContent: 'space-between', alignItems: 'center',
            marginBottom: '8px',
          }}>
            <span style={{ fontWeight: 600, fontSize: '0.9rem' }}>
              {source.source_type === 'forecast' ? '📊' : '⚠️'} {source.source_name || source.source_type}
            </span>
            <QualityBadge quality={source.data_quality} />
          </div>
          <div style={{
            display: 'flex', gap: '16px', fontSize: '0.8rem', color: '#6b7280',
          }}>
            <span>Entries: {source.entries_count}</span>
            <span>Reliability: {source.reliability_percent?.toFixed(0) || 0}%</span>
            <span>Response: {source.avg_response_time_ms}ms</span>
          </div>
        </div>
      ))}
      
      <div style={{
        marginTop: '16px', padding: '12px', backgroundColor: '#f9fafb',
        borderRadius: '8px', fontSize: '0.75rem', color: '#6b7280',
      }}>
        <div><strong>Forecast (XML)</strong>: State-based, updates hourly</div>
        <div><strong>Alerts (RSS)</strong>: Event-based, updates every 10min</div>
      </div>
    </div>
  );
}

// =============================================================================
// System Status Banner Component
// =============================================================================

function SystemBanner({ status }) {
  if (!status) return null;

  const statusConfig = {
    healthy: { label: 'System Healthy', color: '#22c55e', bg: 'rgba(34, 197, 94, 0.1)' },
    degraded: { label: 'System Degraded', color: '#f59e0b', bg: 'rgba(245, 158, 11, 0.1)' },
    unhealthy: { label: 'System Unhealthy', color: '#ef4444', bg: 'rgba(239, 68, 68, 0.1)' },
  }[status.status] || { label: 'Unknown', color: '#6b7280', bg: 'rgba(107, 114, 128, 0.1)' };

  return (
    <div style={{
      backgroundColor: statusConfig.bg,
      border: `1px solid ${statusConfig.color}33`,
      borderRadius: '12px',
      padding: '16px 20px',
      marginBottom: '20px',
    }}>
      <div style={{
        display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '12px',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          <div style={{
            width: '12px', height: '12px', borderRadius: '50%',
            backgroundColor: statusConfig.color,
            animation: status.status === 'healthy' ? 'pulse 2s infinite' : 'none',
          }} />
          <span style={{ fontWeight: 600, color: statusConfig.color }}>{statusConfig.label}</span>
          <QualityBadge quality={status.data_quality} />
        </div>
        
        <div style={{ display: 'flex', gap: '20px', fontSize: '0.85rem', color: '#4b5563' }}>
          <span>🏙️ {status.cities_available} cities</span>
          <span>⚠️ {status.active_alerts} alerts</span>
          <span>⏱️ {status.uptime}</span>
        </div>
      </div>
      
      {status.risks && status.risks.length > 0 && (
        <div style={{
          marginTop: '12px', padding: '8px 12px',
          backgroundColor: 'rgba(239, 68, 68, 0.1)',
          borderRadius: '6px', fontSize: '0.8rem', color: '#dc2626',
        }}>
          ⚠️ {status.risks.join(' • ')}
        </div>
      )}
    </div>
  );
}

// =============================================================================
// Main App Component
// =============================================================================

function App() {
  const [selectedCity, setSelectedCity] = useState('Bucuresti');
  const [forecasts, setForecasts] = useState([]);
  const [alerts, setAlerts] = useState([]);
  const [systemStatus, setSystemStatus] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [lastUpdate, setLastUpdate] = useState(null);
  const [activeTab, setActiveTab] = useState('forecast');

  const fetchData = useCallback(async () => {
    try {
      setError(null);
      
      const [forecastRes, alertRes, statusRes] = await Promise.all([
        fetch(`/forecast/${encodeURIComponent(selectedCity)}`),
        fetch('/alerts?limit=20'),
        fetch('/status'),
      ]);

      if (forecastRes.ok) {
        const forecastData = await forecastRes.json();
        setForecasts(forecastData);
      }

      if (alertRes.ok) {
        const alertData = await alertRes.json();
        setAlerts(alertData);
      }

      if (statusRes.ok) {
        const statusData = await statusRes.json();
        setSystemStatus(statusData);
      }

      setLastUpdate(new Date());
      setLoading(false);
    } catch (err) {
      console.error('Fetch error:', err);
      setError(err.message);
      setLoading(false);
    }
  }, [selectedCity]);

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, POLL_INTERVAL);
    return () => clearInterval(interval);
  }, [fetchData]);

  const handleCitySelect = (city) => {
    setSelectedCity(city);
    setLoading(true);
  };

  const handleRefresh = async () => {
    setLoading(true);
    try {
      await fetch('/fetch', { method: 'POST' });
      await new Promise(r => setTimeout(r, 2000));
      await fetchData();
    } catch (err) {
      setError('Refresh failed: ' + err.message);
    }
    setLoading(false);
  };

  if (loading && !systemStatus) {
    return (
      <div style={{
        minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center',
        backgroundColor: '#f8fafc', fontFamily: "'Inter', -apple-system, sans-serif",
      }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{
            width: '48px', height: '48px', border: '3px solid #e5e7eb',
            borderTopColor: '#3b82f6', borderRadius: '50%',
            animation: 'spin 1s linear infinite', margin: '0 auto 16px',
          }} />
          <p style={{ color: '#6b7280' }}>Loading weather data...</p>
        </div>
      </div>
    );
  }

  return (
    <div style={{
      minHeight: '100vh', backgroundColor: '#f8fafc',
      fontFamily: "'Inter', -apple-system, sans-serif",
    }}>
      {/* Header */}
      <header style={{
        backgroundColor: 'white', borderBottom: '1px solid #e5e7eb',
        padding: '16px 24px', position: 'sticky', top: 0, zIndex: 100,
      }}>
        <div style={{
          maxWidth: '1200px', margin: '0 auto',
          display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        }}>
          <div>
            <h1 style={{
              margin: 0, fontSize: '1.5rem', fontWeight: 700,
              display: 'flex', alignItems: 'center', gap: '12px',
            }}>
              🇷🇴 Romania Weather
              <span style={{
                fontSize: '0.7rem', fontWeight: 400, color: '#6b7280',
                backgroundColor: '#f3f4f6', padding: '4px 8px', borderRadius: '4px',
              }}>ANM Data</span>
            </h1>
            <p style={{ margin: '4px 0 0', fontSize: '0.85rem', color: '#6b7280' }}>
              XML Forecasts + RSS Alerts from Meteoromania
            </p>
          </div>
          
          <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
            {lastUpdate && (
              <span style={{ fontSize: '0.8rem', color: '#6b7280' }}>
                Updated: {lastUpdate.toLocaleTimeString()}
              </span>
            )}
            <button
              onClick={handleRefresh}
              disabled={loading}
              style={{
                padding: '8px 16px', backgroundColor: loading ? '#9ca3af' : '#3b82f6',
                color: 'white', border: 'none', borderRadius: '8px',
                fontWeight: 500, cursor: loading ? 'not-allowed' : 'pointer',
              }}
            >
              {loading ? '...' : '🔄 Refresh'}
            </button>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main style={{ maxWidth: '1200px', margin: '0 auto', padding: '24px' }}>
        {/* Error */}
        {error && (
          <div style={{
            backgroundColor: 'rgba(239, 68, 68, 0.1)', border: '1px solid rgba(239, 68, 68, 0.3)',
            borderRadius: '8px', padding: '12px 16px', marginBottom: '16px', color: '#dc2626',
          }}>
            ❌ {error}
          </div>
        )}

        {/* System Status */}
        <SystemBanner status={systemStatus} />

        {/* Main Grid */}
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 320px', gap: '24px' }}>
          {/* Left Panel */}
          <div>
            {/* Tabs */}
            <div style={{
              display: 'flex', gap: '4px', marginBottom: '16px',
              backgroundColor: '#f3f4f6', padding: '4px', borderRadius: '10px', width: 'fit-content',
            }}>
              <button
                onClick={() => setActiveTab('forecast')}
                style={{
                  padding: '10px 20px', border: 'none', borderRadius: '8px',
                  backgroundColor: activeTab === 'forecast' ? 'white' : 'transparent',
                  color: activeTab === 'forecast' ? '#111827' : '#6b7280',
                  fontWeight: 600, cursor: 'pointer',
                  boxShadow: activeTab === 'forecast' ? '0 1px 3px rgba(0,0,0,0.1)' : 'none',
                }}
              >
                📊 5-Day Forecast
              </button>
              <button
                onClick={() => setActiveTab('alerts')}
                style={{
                  padding: '10px 20px', border: 'none', borderRadius: '8px',
                  backgroundColor: activeTab === 'alerts' ? 'white' : 'transparent',
                  color: activeTab === 'alerts' ? '#111827' : '#6b7280',
                  fontWeight: 600, cursor: 'pointer',
                  boxShadow: activeTab === 'alerts' ? '0 1px 3px rgba(0,0,0,0.1)' : 'none',
                  position: 'relative',
                }}
              >
                ⚠️ Alerts ({alerts.length})
                {alerts.length > 0 && (
                  <span style={{
                    position: 'absolute', top: '-4px', right: '-4px',
                    width: '20px', height: '20px', backgroundColor: '#ef4444',
                    color: 'white', borderRadius: '50%', fontSize: '0.7rem',
                    display: 'flex', alignItems: 'center', justifyContent: 'center', fontWeight: 700,
                  }}>{alerts.length}</span>
                )}
              </button>
            </div>

            {/* Content */}
            {activeTab === 'forecast' ? (
              <div>
                {/* Region Selector - only for forecasts */}
                <CitySelector selectedCity={selectedCity} onCitySelect={handleCitySelect} />
                
                <h2 style={{ margin: '0 0 16px', fontSize: '1.2rem', fontWeight: 600 }}>
                  📍 {selectedCity} - 5 Day Forecast
                </h2>
                {forecasts.length > 0 ? (
                  <div style={{
                    display: 'flex', gap: '12px', overflowX: 'auto', paddingBottom: '8px',
                  }}>
                    {forecasts.map((f, idx) => (
                      <ForecastCard key={idx} forecast={f} />
                    ))}
                  </div>
                ) : (
                  <div style={{
                    textAlign: 'center', padding: '40px',
                    backgroundColor: 'white', borderRadius: '12px',
                  }}>
                    <div style={{ fontSize: '2rem', marginBottom: '12px' }}>🔍</div>
                    <p style={{ color: '#6b7280' }}>No forecast data for {selectedCity}</p>
                  </div>
                )}
              </div>
            ) : (
              <div>
                <h2 style={{ margin: '0 0 16px', fontSize: '1.2rem', fontWeight: 600 }}>
                  ⚠️ Active Weather Alerts - Romania
                </h2>
                {alerts.length > 0 ? (
                  <div style={{ maxHeight: '60vh', overflowY: 'auto' }}>
                    {alerts.map((alert, idx) => (
                      <AlertCard key={idx} alert={alert} />
                    ))}
                  </div>
                ) : (
                  <div style={{
                    textAlign: 'center', padding: '40px',
                    backgroundColor: 'white', borderRadius: '12px',
                  }}>
                    <div style={{ fontSize: '2rem', marginBottom: '12px' }}>✅</div>
                    <p style={{ color: '#6b7280' }}>No active weather alerts</p>
                  </div>
                )}
              </div>
            )}
          </div>

          {/* Right Panel - Source Health */}
          <SourceHealthPanel sources={systemStatus?.source_health} />
        </div>
      </main>

      {/* CSS */}
      <style>{`
        @keyframes spin { to { transform: rotate(360deg); } }
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
      `}</style>
    </div>
  );
}

export default App;
