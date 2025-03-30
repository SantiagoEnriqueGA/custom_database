import React, { useState, useEffect, useCallback } from 'react';
import { listTables, insertRecord } from '../services/api';
import LoadingSpinner from '../components/LoadingSpinner';
import './Forms.css'; // Create a shared CSS for forms

function InsertRecordPage() {
  const [tables, setTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState('');
  const [recordData, setRecordData] = useState('{\n  "column1": "value1",\n  "column2": "value2"\n}'); // Use controlled textarea for JSON
  const [loadingTables, setLoadingTables] = useState(true);
  const [loadingInsert, setLoadingInsert] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');

  const fetchTables = useCallback(async () => {
    setLoadingTables(true);
    setError('');
    try {
      const response = await listTables();
      if (response.data?.status === 'success') {
        // Filter out internal tables like _users if desired
        setTables(response.data.data?.filter(t => !t.startsWith('_')) || []);
        if (response.data.data?.length > 0) {
           // Optionally pre-select the first table
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

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!selectedTable) {
      setError('Please select a table.');
      return;
    }

    let parsedRecord;
    console.log("Raw recordData from textarea:", recordData); 

    try {
      parsedRecord = JSON.parse(recordData);
      console.log("Parsed record:", parsedRecord); // <<< ADD THIS LINE
      if (typeof parsedRecord !== 'object' || parsedRecord === null) {
        // This check might be redundant if JSON.parse throws for non-objects, but safe to keep
        throw new Error('Input must be a valid JSON object.');
      }
    } catch (jsonError) {
      console.error("JSON parsing failed:", jsonError); 
      setError(`Invalid JSON data: ${jsonError.message}`);
      return; // Error handled
    }

    setLoadingInsert(true);
    setError('');
    setSuccess('');
    try {
      const response = await insertRecord(selectedTable, parsedRecord);
      if (response.data?.status === 'success') {
        setSuccess(`Record inserted into '${selectedTable}' successfully!`);
        // Optionally clear the form or navigate
        // setRecordData('{\n  "column1": "value1",\n  "column2": "value2"\n}');
        // navigate(`/tables`); // Or view the table?
      } else {
        throw new Error(response.data?.message || 'Failed to insert record');
      }
    } catch (err) {
      setError(err.message || 'An error occurred during insertion.');
    } finally {
      setLoadingInsert(false);
    }
  };

  return (
    <div className="form-container">
      <h1>Insert Record</h1>
      <form onSubmit={handleSubmit}>
        {error && <p className="error-message">{error}</p>}
        {success && <p className="success-message">{success}</p>}

        <div className="form-group">
          <label htmlFor="tableName">Table:</label>
          {loadingTables ? <LoadingSpinner /> : (
            <select
              id="tableName"
              value={selectedTable}
              onChange={(e) => setSelectedTable(e.target.value)}
              required
              disabled={loadingInsert || loadingTables || tables.length === 0}
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
          <label htmlFor="recordData">Record Data (JSON):</label>
          <textarea
            id="recordData"
            value={recordData}
            onChange={(e) => setRecordData(e.target.value)}
            rows={10}
            required
            disabled={loadingInsert}
            placeholder='Enter record data as a JSON object, e.g., {"name": "Alice", "age": 30}'
          />
        </div>

        <button type="submit" disabled={loadingInsert || loadingTables || !selectedTable}>
          {loadingInsert ? <LoadingSpinner /> : 'Insert Record'}
        </button>
      </form>
    </div>
  );
}

export default InsertRecordPage;