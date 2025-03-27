import React, { useState } from 'react';
import { createTable } from '../services/api'; // Path is correct from here
import { useNavigate } from 'react-router-dom';
import LoadingSpinner from '../components/LoadingSpinner'; // Path is correct from here

function CreateTablePage() {
  const [tableName, setTableName] = useState('');
  const [columns, setColumns] = useState(''); // Input as comma-separated string
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const navigate = useNavigate();

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError('');
    setSuccess('');
    try {
      // Assuming API expects comma-separated string for columns
      const response = await createTable(tableName, columns);
      if (response.data?.status === 'success') {
        setSuccess(`Table '${tableName}' created successfully!`);
        setTableName('');
        setColumns('');
        // Optionally navigate away or refresh table list elsewhere
        // navigate('/tables');
      } else {
        throw new Error(response.data?.message || 'Failed to create table');
      }
    } catch (err) {
      setError(err.message || 'An error occurred.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div>
      <h1>Create New Table</h1>
      <form onSubmit={handleSubmit}>
        {error && <p style={{ color: 'red' }}>Error: {error}</p>}
        {success && <p style={{ color: 'green' }}>{success}</p>}
        <div>
          <label htmlFor="tableName">Table Name:</label>
          <input
            type="text"
            id="tableName"
            value={tableName}
            onChange={(e) => setTableName(e.target.value)}
            required
            disabled={loading}
          />
        </div>
        <div>
          <label htmlFor="columns">Columns (comma-separated):</label>
          <input
            type="text"
            id="columns"
            value={columns}
            onChange={(e) => setColumns(e.target.value)}
            placeholder="e.g., id, name, email"
            required
            disabled={loading}
          />
        </div>
        <button type="submit" disabled={loading}>
          {loading ? <LoadingSpinner /> : 'Create Table'}
        </button>
      </form>
    </div>
  );
}

export default CreateTablePage;