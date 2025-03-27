import React, { useState, useEffect, useCallback } from 'react';
import { listTables, queryTable } from '../services/api';
import DataTable from '../components/DataTable';
import LoadingSpinner from '../components/LoadingSpinner';
import './QueryTablePage.css'; // Create specific CSS if needed, or reuse Forms.css

function QueryTablePage() {
  const [tables, setTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState('');
  const [filterCondition, setFilterCondition] = useState('');
  const [queryResult, setQueryResult] = useState(null);
  const [loadingTables, setLoadingTables] = useState(true);
  const [loadingQuery, setLoadingQuery] = useState(false);
  const [error, setError] = useState('');

   const fetchTables = useCallback(async () => {
    setLoadingTables(true);
    setError('');
    try {
      const response = await listTables();
      if (response.data?.status === 'success') {
        setTables(response.data.data?.filter(t => !t.startsWith('_')) || []);
         if (response.data.data?.length > 0) {
           // Optionally pre-select
           // setSelectedTable(response.data.data[0]);
         }
      } else {
        throw new Error(response.data?.message || 'Failed to fetch tables');
      }
    } catch (err) {
      setError(err.message);
      setTables([]);
    } finally {
      setLoadingTables(false);
    }
  }, []);

  useEffect(() => {
    fetchTables();
  }, [fetchTables]);

  const handleQuery = async (e) => {
    e.preventDefault();
    if (!selectedTable) {
      setError('Please select a table.');
      return;
    }

    setLoadingQuery(true);
    setError('');
    setQueryResult(null); // Clear previous results
    try {
      // Pass null if filter is empty, otherwise pass the string
      const filterToSend = filterCondition.trim() === '' ? null : filterCondition.trim();
      const response = await queryTable(selectedTable, filterToSend);
      if (response.data?.status === 'success') {
         setQueryResult({
             columns: response.data.columns || (response.data.data?.length > 0 ? Object.keys(response.data.data[0]) : []),
             rows: response.data.data || []
         });
      } else {
        throw new Error(response.data?.message || `Failed to query table ${selectedTable}`);
      }
    } catch (err) {
      setError(`Query Error: ${err.message}`);
    } finally {
      setLoadingQuery(false);
    }
  };

  return (
    <div className="query-table-page">
      <h1>Query Table</h1>

      <form onSubmit={handleQuery} className="query-form">
        {error && <p className="error-message">{error}</p>}

        <div className="form-group">
          <label htmlFor="tableName">Table:</label>
           {loadingTables ? <LoadingSpinner /> : (
            <select
              id="tableName"
              value={selectedTable}
              onChange={(e) => setSelectedTable(e.target.value)}
              required
              disabled={loadingQuery || loadingTables || tables.length === 0}
            >
              <option value="" disabled>-- Select a Table --</option>
              {tables.map(table => (
                <option key={table} value={table}>{table}</option>
              ))}
              {tables.length === 0 && <option disabled>No tables available</option>}
            </select>
          )}
        </div>

        <div className="form-group">
          <label htmlFor="filterCondition">Filter Condition (Optional):</label>
          <input
            type="text"
            id="filterCondition"
            value={filterCondition}
            onChange={(e) => setFilterCondition(e.target.value)}
            disabled={loadingQuery || loadingTables}
            placeholder="e.g., lambda record: record['price'] > 100"
          />
           <small style={{color: '#aaa'}}>Warning: The backend currently executes filter strings. Use with caution.</small>
        </div>

        <button type="submit" disabled={loadingQuery || loadingTables || !selectedTable}>
          {loadingQuery ? <LoadingSpinner /> : 'Run Query'}
        </button>
      </form>

      <div className="query-results">
        <h2>Results {selectedTable ? `for ${selectedTable}` : ''}</h2>
        {loadingQuery ? <LoadingSpinner /> : (
          queryResult ? (
             <>
                {/* Display count? */}
                <p>Found {queryResult.rows.length} record(s).</p>
                <DataTable columns={queryResult.columns} data={queryResult.rows} />
                {queryResult.rows.length === 0 && <p>Query returned no matching records.</p>}
            </>
          ) : (
            <p>Run a query to see results.</p>
          )
        )}
      </div>
    </div>
  );
}

export default QueryTablePage;