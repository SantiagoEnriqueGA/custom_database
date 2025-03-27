// src/pages/QueryBuilderPage.jsx
import React, { useState, useEffect, useCallback } from 'react';
import {
  listTables, queryTable,
  listViews, queryView,
  listMaterializedViews, queryMaterializedView
} from '../services/api';
import DataTable from '../components/DataTable';
import LoadingSpinner from '../components/LoadingSpinner';
import './QueryBuilderPage.css'; // Renamed CSS import

const SOURCE_TYPES = {
  TABLE: 'table',
  VIEW: 'view',
  MATERIALIZED_VIEW: 'materialized_view'
};

function QueryBuilderPage() {
  const [sourceType, setSourceType] = useState(SOURCE_TYPES.TABLE);
  const [sourcesList, setSourcesList] = useState([]);
  const [selectedSource, setSelectedSource] = useState('');
  const [filterCondition, setFilterCondition] = useState('');
  const [queryResult, setQueryResult] = useState(null);
  const [loadingSources, setLoadingSources] = useState(true);
  const [loadingQuery, setLoadingQuery] = useState(false);
  const [error, setError] = useState('');

  const fetchSources = useCallback(async () => {
    setLoadingSources(true);
    setError('');
    setSourcesList([]); // Clear previous list
    setSelectedSource(''); // Reset selection
    setQueryResult(null); // Clear results
    try {
      let response;
      if (sourceType === SOURCE_TYPES.TABLE) {
        response = await listTables();
      } else if (sourceType === SOURCE_TYPES.VIEW) {
        response = await listViews();
      } else if (sourceType === SOURCE_TYPES.MATERIALIZED_VIEW) {
        response = await listMaterializedViews();
      } else {
        throw new Error("Invalid source type selected");
      }

      if (response.data?.status === 'success') {
          // Filter out internal tables like _users for clarity in UI
          const dataList = response.data.data || [];
          if (sourceType === SOURCE_TYPES.TABLE) {
              setSourcesList(dataList.filter(t => !t.startsWith('_')));
          } else {
              setSourcesList(dataList);
          }
      } else {
        throw new Error(response.data?.message || `Failed to fetch ${sourceType}s`);
      }
    } catch (err) {
      setError(`Error fetching sources: ${err.message}`);
      setSourcesList([]);
    } finally {
      setLoadingSources(false);
    }
  }, [sourceType]); // Re-run fetchSources when sourceType changes

  useEffect(() => {
    fetchSources();
  }, [fetchSources]); // Initial fetch and fetch on type change

  const handleQuery = async (e) => {
    e.preventDefault();
    if (!selectedSource) {
      setError('Please select a source to query.');
      return;
    }

    setLoadingQuery(true);
    setError('');
    setQueryResult(null); // Clear previous results
    try {
      let response;
      const isTable = sourceType === SOURCE_TYPES.TABLE;
      // Pass null if filter is empty or not applicable, otherwise pass the string
      const filterToSend = isTable && filterCondition.trim() !== '' ? filterCondition.trim() : null;

      if (sourceType === SOURCE_TYPES.TABLE) {
        response = await queryTable(selectedSource, filterToSend);
      } else if (sourceType === SOURCE_TYPES.VIEW) {
        response = await queryView(selectedSource);
      } else if (sourceType === SOURCE_TYPES.MATERIALIZED_VIEW) {
        response = await queryMaterializedView(selectedSource);
      } else {
          throw new Error("Invalid source type for query");
      }

      if (response.data?.status === 'success') {
        setQueryResult({
          columns: response.data.columns || (response.data.data?.length > 0 ? Object.keys(response.data.data[0]).filter(k => k !== '_record_id') : []), // Exclude _record_id from default columns if needed
          rows: response.data.data || []
        });
      } else {
        throw new Error(response.data?.message || `Failed to query ${sourceType} ${selectedSource}`);
      }
    } catch (err) {
      setError(`Query Error: ${err.message}`);
    } finally {
      setLoadingQuery(false);
    }
  };

  const handleSourceTypeChange = (e) => {
      setSourceType(e.target.value);
      // Fetching is handled by the useEffect hook reacting to sourceType change
  };

  const isFilterEnabled = sourceType === SOURCE_TYPES.TABLE;

  return (
    <div className="query-builder-page"> {/* Renamed class */}
      <h1>Query Builder</h1> {/* Renamed title */}

      <form onSubmit={handleQuery} className="query-form">
        {error && <p className="error-message">{error}</p>}

        <div className="form-group">
          <label htmlFor="sourceType">Source Type:</label>
          <select
              id="sourceType"
              value={sourceType}
              onChange={handleSourceTypeChange}
              disabled={loadingSources || loadingQuery}
          >
              <option value={SOURCE_TYPES.TABLE}>Table</option>
              <option value={SOURCE_TYPES.VIEW}>View</option>
              <option value={SOURCE_TYPES.MATERIALIZED_VIEW}>Materialized View</option>
          </select>
        </div>

        <div className="form-group">
          <label htmlFor="sourceName">Source Name:</label>
          {loadingSources ? <LoadingSpinner /> : (
            <select
              id="sourceName"
              value={selectedSource}
              onChange={(e) => setSelectedSource(e.target.value)}
              required
              disabled={loadingQuery || loadingSources || sourcesList.length === 0}
            >
              <option value="" disabled>-- Select a {sourceType.replace('_', ' ')} --</option>
              {sourcesList.map(source => (
                <option key={source} value={source}>{source}</option>
              ))}
              {sourcesList.length === 0 && <option disabled>No {sourceType.replace('_', ' ')}s available</option>}
            </select>
          )}
        </div>

        <div className="form-group">
          <label htmlFor="filterCondition">Filter Condition (Tables Only):</label>
          <input
            type="text"
            id="filterCondition"
            value={filterCondition}
            onChange={(e) => setFilterCondition(e.target.value)}
            disabled={loadingQuery || loadingSources || !isFilterEnabled} // Disable if not a table
            placeholder={isFilterEnabled ? "e.g., lambda record: record.data['category'] == 'Electronics'" : "Filtering not applicable for views/mvs"}
          />
          {isFilterEnabled && <small style={{color: '#aaa'}}>Warning: Backend executes filter strings. Use with caution.</small>}
        </div>

        <button type="submit" disabled={loadingQuery || loadingSources || !selectedSource}>
          {loadingQuery ? <LoadingSpinner /> : 'Run Query'}
        </button>
      </form>

      <div className="query-results">
        <h2>Results {selectedSource ? `for ${sourceType.replace('_', ' ')}: ${selectedSource}` : ''}</h2>
        {loadingQuery ? <LoadingSpinner /> : (
          queryResult ? (
            <>
                <p>Found {queryResult.rows.length} record(s).</p>
                <DataTable columns={queryResult.columns} data={queryResult.rows} />
                {/* Explicit message if needed, DataTable handles internal empty message now */}
                {/* {queryResult.rows.length === 0 && <p>Query returned no matching records.</p>} */}
            </>
          ) : (
            <p>Run a query to see results.</p>
          )
        )}
      </div>
    </div>
  );
}

export default QueryBuilderPage;