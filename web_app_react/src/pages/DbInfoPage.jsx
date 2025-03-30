import React, { useState, useEffect, useCallback } from 'react';
import { getDbInfo } from '../services/api';
import LoadingSpinner from '../components/LoadingSpinner';
import './DbInfoPage.css'; // Create specific CSS

function DbInfoPage() {
  const [dbInfo, setDbInfo] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  const fetchDbInfo = useCallback(async () => {
    setLoading(true);
    setError('');
    try {
      const response = await getDbInfo();
      if (response.data?.status === 'success') {
        setDbInfo(response.data.data);
      } else {
        throw new Error(response.data?.message || 'Failed to fetch database info');
      }
    } catch (err) {
      setError(err.message);
      setDbInfo(null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchDbInfo();
  }, [fetchDbInfo]);

  const formatKey = (key) => {
    // Simple formatting: replace underscores with spaces and capitalize words
    return key.split('_').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' ');
  };

  const renderValue = (key, value) => {
      if (value === true) return 'Yes';
      if (value === false) return 'No';
      if (key === 'size_mb' && typeof value === 'number') return `${value.toFixed(4)} MB`;
      // Add more specific formatting if needed
      return String(value);
  }

  return (
    <div className="db-info-page">
      <h1>Database Information</h1>
      {error && <p className="error-message">{error}</p>}
      {loading ? <LoadingSpinner /> : (
        dbInfo ? (
          <ul className="info-list">
            {Object.entries(dbInfo).map(([key, value]) => (
              <li key={key}>
                <strong>{formatKey(key)}:</strong> {renderValue(key, value ?? 'N/A')}
              </li>
            ))}
          </ul>
        ) : (
          !error && <p>Could not load database information.</p>
        )
      )}
      <button onClick={fetchDbInfo} disabled={loading} style={{marginTop: '20px'}}>
        {loading ? <LoadingSpinner /> : 'Refresh Info'}
      </button>
    </div>
  );
}

export default DbInfoPage;