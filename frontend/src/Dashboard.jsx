import React, { useState, useEffect } from 'react';
import { LineChart, Line, BarChart, Bar, RadarChart, Radar, PolarGrid, PolarAngleAxis, PolarRadiusAxis, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { AlertCircle, Activity, Droplets, Wind, Thermometer } from 'lucide-react';

const Dashboard = () => {
  const [activeTab, setActiveTab] = useState('dashboard');
  const [sensorData, setSensorData] = useState([]);
  const [alerts, setAlerts] = useState([]);
  const [alertFrequency, setAlertFrequency] = useState({});
  const [stats, setStats] = useState({});
  const [cropInput, setCropInput] = useState({
    nitrogen: '',
    phosphorus: '',
    potassium: '',
    temperature: '',
    humidity: '',
    ph: '',
    rainfall: ''
  });
  const [cropRecommendations, setCropRecommendations] = useState([]);
  const [isConnected, setIsConnected] = useState(false);

  useEffect(() => {
    // Fetch initial data
    fetchAllData();
    
    // Poll for updates every 2 seconds
    const interval = setInterval(() => {
      fetchAllData();
    }, 2000);

    return () => clearInterval(interval);
  }, []);

  const fetchAllData = async () => {
    try {
      // Fetch sensor data
      const sensorResponse = await fetch('http://localhost:5000/api/sensor-data');
      if (sensorResponse.ok) {
        const sensorJson = await sensorResponse.json();
        setSensorData(sensorJson);
        setIsConnected(true);
      }

      // Fetch alerts
      const alertsResponse = await fetch('http://localhost:5000/api/alerts');
      if (alertsResponse.ok) {
        const alertsJson = await alertsResponse.json();
        setAlerts(alertsJson);
      }

      // Fetch stats
      const statsResponse = await fetch('http://localhost:5000/api/stats');
      if (statsResponse.ok) {
        const statsJson = await statsResponse.json();
        setStats(statsJson);
      }

      // Fetch alert frequency
      const freqResponse = await fetch('http://localhost:5000/api/alert-frequency');
      if (freqResponse.ok) {
        const freqJson = await freqResponse.json();
        setAlertFrequency(freqJson);
      }
    } catch (error) {
      console.error('Error fetching data:', error);
      setIsConnected(false);
    }
  };

  const handleCropRecommendation = async () => {
    try {
      const response = await fetch('http://localhost:5000/api/recommend-crop', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(cropInput)
      });
      const data = await response.json();
      setCropRecommendations(data.recommendations);
    } catch (error) {
      console.error('Error getting crop recommendation:', error);
    }
  };

  // Prepare chart data
  const tempChartData = sensorData.slice(-20).map((d, i) => ({
    time: i,
    temperature: d['temperature_2m (°C)'] || 0
  }));

  const humidityChartData = sensorData.slice(-20).map((d, i) => ({
    time: i,
    humidity: d['relative_humidity_2m (%)'] || 0
  }));

  const windChartData = sensorData.slice(-20).map((d, i) => ({
    time: i,
    wind: d['wind_speed_10m (km/h)'] || 0
  }));

  const alertFrequencyData = Object.entries(alertFrequency).map(([name, value]) => ({
    name,
    count: value
  }));

  const getSeverityColor = (severity) => {
    switch (severity) {
      case 'Critical': return 'bg-red-100 border-red-500 text-red-800';
      case 'Warning': return 'bg-yellow-100 border-yellow-500 text-yellow-800';
      default: return 'bg-blue-100 border-blue-500 text-blue-800';
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-green-50 to-blue-50">
      {/* Header */}
      <header className="bg-gradient-to-r from-green-600 to-green-700 text-white p-6 shadow-lg">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-4xl font-bold flex items-center gap-3">
              <Activity className="w-10 h-10" />
              Smart Agriculture Decision Support Dashboard 🌾
            </h1>
            <p className="text-green-100 mt-2">Real-time monitoring using CEP + ML</p>
          </div>
          <div className="flex items-center gap-2">
            <div className={`w-3 h-3 rounded-full ${isConnected ? 'bg-green-300 animate-pulse' : 'bg-red-300'}`}></div>
            <span className="text-sm">{isConnected ? 'Connected' : 'Disconnected'}</span>
          </div>
        </div>
      </header>

      {/* Navigation Tabs */}
      <div className="bg-white shadow">
        <div className="max-w-7xl mx-auto px-6">
          <div className="flex gap-4 border-b">
            <button
              onClick={() => setActiveTab('dashboard')}
              className={`px-6 py-4 font-semibold transition-colors ${
                activeTab === 'dashboard'
                  ? 'border-b-4 border-green-600 text-green-600'
                  : 'text-gray-600 hover:text-green-600'
              }`}
            >
              Real-Time Dashboard
            </button>
            <button
              onClick={() => setActiveTab('crops')}
              className={`px-6 py-4 font-semibold transition-colors ${
                activeTab === 'crops'
                  ? 'border-b-4 border-green-600 text-green-600'
                  : 'text-gray-600 hover:text-green-600'
              }`}
            >
              Crop Recommendation
            </button>
          </div>
        </div>
      </div>

      <div className="max-w-7xl mx-auto p-6">
        {/* Dashboard Tab */}
        {activeTab === 'dashboard' && (
          <div className="space-y-6">
            {/* Stats Cards */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
              <div className="bg-white rounded-lg shadow p-6 border-l-4 border-red-500">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-gray-600 text-sm">Active Alerts</p>
                    <p className="text-3xl font-bold text-red-600">{stats.active_alerts || 0}</p>
                  </div>
                  <AlertCircle className="w-12 h-12 text-red-500" />
                </div>
              </div>

              <div className="bg-white rounded-lg shadow p-6 border-l-4 border-blue-500">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-gray-600 text-sm">Total Readings</p>
                    <p className="text-3xl font-bold text-blue-600">{stats.total_readings || 0}</p>
                  </div>
                  <Activity className="w-12 h-12 text-blue-500" />
                </div>
              </div>

              <div className="bg-white rounded-lg shadow p-6 border-l-4 border-green-500">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-gray-600 text-sm">CEP Status</p>
                    <p className="text-2xl font-bold text-green-600">{stats.cep_status || 'Active'} ✅</p>
                  </div>
                  <Activity className="w-12 h-12 text-green-500" />
                </div>
              </div>

              <div className="bg-white rounded-lg shadow p-6 border-l-4 border-purple-500">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-gray-600 text-sm">Last Event</p>
                    <p className="text-sm font-semibold text-purple-600">
                      {stats.last_event ? new Date(stats.last_event).toLocaleTimeString() : 'N/A'}
                    </p>
                  </div>
                  <Activity className="w-12 h-12 text-purple-500" />
                </div>
              </div>
            </div>

            {/* Live Sensor Charts */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div className="bg-white rounded-lg shadow p-6">
                <h3 className="text-xl font-bold mb-4 flex items-center gap-2">
                  <Thermometer className="w-6 h-6 text-red-500" />
                  Temperature (°C)
                </h3>
                <ResponsiveContainer width="100%" height={250}>
                  <LineChart data={tempChartData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="time" />
                    <YAxis />
                    <Tooltip />
                    <Line type="monotone" dataKey="temperature" stroke="#ef4444" strokeWidth={2} dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              </div>

              <div className="bg-white rounded-lg shadow p-6">
                <h3 className="text-xl font-bold mb-4 flex items-center gap-2">
                  <Droplets className="w-6 h-6 text-blue-500" />
                  Humidity (%)
                </h3>
                <ResponsiveContainer width="100%" height={250}>
                  <LineChart data={humidityChartData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="time" />
                    <YAxis />
                    <Tooltip />
                    <Line type="monotone" dataKey="humidity" stroke="#3b82f6" strokeWidth={2} dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              </div>

              <div className="bg-white rounded-lg shadow p-6">
                <h3 className="text-xl font-bold mb-4 flex items-center gap-2">
                  <Wind className="w-6 h-6 text-purple-500" />
                  Wind Speed (km/h)
                </h3>
                <ResponsiveContainer width="100%" height={250}>
                  <LineChart data={windChartData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="time" />
                    <YAxis />
                    <Tooltip />
                    <Line type="monotone" dataKey="wind" stroke="#8b5cf6" strokeWidth={2} dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              </div>

              {/* Alert Frequency */}
              <div className="bg-white rounded-lg shadow p-6">
                <h3 className="text-xl font-bold mb-4">Alert Frequency</h3>
                <ResponsiveContainer width="100%" height={250}>
                  <BarChart data={alertFrequencyData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="name" angle={-45} textAnchor="end" height={80} />
                    <YAxis />
                    <Tooltip />
                    <Bar dataKey="count" fill="#10b981" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>

            {/* Alerts Table */}
            <div className="bg-white rounded-lg shadow overflow-hidden">
              <div className="p-6 border-b">
                <h3 className="text-xl font-bold">Recent Alerts</h3>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Timestamp</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Alert Type</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Parameter</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Value</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Condition</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Severity</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Action</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-200">
                    {alerts.slice(0, 10).map((alert, idx) => (
                      <tr key={idx} className="hover:bg-gray-50">
                        <td className="px-6 py-4 text-sm">
                          {new Date(alert.timestamp).toLocaleTimeString()}
                        </td>
                        <td className="px-6 py-4 text-sm font-medium">{alert.alert_type || alert.parameter}</td>
                        <td className="px-6 py-4 text-sm">{alert.parameter}</td>
                        <td className="px-6 py-4 text-sm">{alert.value}</td>
                        <td className="px-6 py-4 text-sm">{alert.condition}</td>
                        <td className="px-6 py-4">
                          <span className={`px-3 py-1 rounded-full text-xs font-semibold border ${getSeverityColor(alert.severity)}`}>
                            {alert.severity}
                          </span>
                        </td>
                        <td className="px-6 py-4 text-sm text-gray-600">{alert.action}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}

        {/* Crop Recommendation Tab */}
        {activeTab === 'crops' && (
          <div className="space-y-6">
            {/* Input Form */}
            <div className="bg-white rounded-lg shadow p-6">
              <h3 className="text-xl font-bold mb-4">Enter Soil & Environmental Parameters</h3>
              <p className="text-sm text-gray-600 mb-4">Fill in all parameters to get personalized crop recommendations</p>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Nitrogen (N)</label>
                  <input
                    type="number"
                    placeholder="e.g., 90"
                    className="border rounded px-4 py-2 w-full"
                    value={cropInput.nitrogen}
                    onChange={(e) => setCropInput({...cropInput, nitrogen: e.target.value})}
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Phosphorus (P)</label>
                  <input
                    type="number"
                    placeholder="e.g., 42"
                    className="border rounded px-4 py-2 w-full"
                    value={cropInput.phosphorus}
                    onChange={(e) => setCropInput({...cropInput, phosphorus: e.target.value})}
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Potassium (K)</label>
                  <input
                    type="number"
                    placeholder="e.g., 43"
                    className="border rounded px-4 py-2 w-full"
                    value={cropInput.potassium}
                    onChange={(e) => setCropInput({...cropInput, potassium: e.target.value})}
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Temperature (°C)</label>
                  <input
                    type="number"
                    step="0.1"
                    placeholder="e.g., 20.87"
                    className="border rounded px-4 py-2 w-full"
                    value={cropInput.temperature}
                    onChange={(e) => setCropInput({...cropInput, temperature: e.target.value})}
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Humidity (%)</label>
                  <input
                    type="number"
                    step="0.1"
                    placeholder="e.g., 82.00"
                    className="border rounded px-4 py-2 w-full"
                    value={cropInput.humidity}
                    onChange={(e) => setCropInput({...cropInput, humidity: e.target.value})}
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">pH Level</label>
                  <input
                    type="number"
                    step="0.1"
                    placeholder="e.g., 6.5"
                    className="border rounded px-4 py-2 w-full"
                    value={cropInput.ph}
                    onChange={(e) => setCropInput({...cropInput, ph: e.target.value})}
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Rainfall (mm)</label>
                  <input
                    type="number"
                    step="0.1"
                    placeholder="e.g., 202.93"
                    className="border rounded px-4 py-2 w-full"
                    value={cropInput.rainfall}
                    onChange={(e) => setCropInput({...cropInput, rainfall: e.target.value})}
                  />
                </div>
              </div>
              <button
                onClick={handleCropRecommendation}
                className="mt-6 bg-green-600 text-white px-8 py-3 rounded-lg hover:bg-green-700 font-semibold transition-colors shadow-md"
                style={{ display: 'block', width: 'auto' }}
              >
                🌾 Get Crop Recommendations
              </button>
            </div>

            {/* Top 3 Recommendations */}
            {cropRecommendations.length > 0 && (
              <div className="bg-white rounded-lg shadow p-6">
                <h3 className="text-xl font-bold mb-4">Top 3 Recommended Crops</h3>
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart data={cropRecommendations}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="crop" />
                    <YAxis />
                    <Tooltip />
                    <Bar dataKey="confidence" fill="#10b981" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            )}

            {/* Recommendation Cards */}
            {cropRecommendations.length > 0 && (
              <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                {cropRecommendations.map((rec, idx) => (
                  <div key={idx} className="bg-white rounded-lg shadow p-6 border-l-4 border-green-500">
                    <h4 className="text-2xl font-bold mb-2">🌾 {rec.crop}</h4>
                    <p className="text-3xl font-bold text-green-600 mb-2">{rec.confidence}%</p>
                    <p className="text-sm text-gray-600 mb-3">Confidence</p>
                    <p className="text-sm text-gray-700">{rec.suitable_conditions}</p>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default Dashboard;
