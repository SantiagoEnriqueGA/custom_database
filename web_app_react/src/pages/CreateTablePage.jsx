import React, { useState } from 'react';
import { createTable } from '../services/api';
import { useNavigate } from 'react-router-dom';
import LoadingSpinner from '../components/LoadingSpinner';
import './Forms.css'; // Import the shared form styles

function CreateTablePage() {
  const [tableName, setTableName] = useState('');
  const [columns, setColumns] = useState(''); // Input as comma-separated string
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const navigate = useNavigate();

  const handleSubmit = async (e) => {
    e.preventDefault();

    // Basic client-side validation for table name
    if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(tableName)) {
        setError('Table name must start with a letter or underscore, and contain only letters, numbers, or underscores.');
        return;
    }
    // Basic validation for columns (ensure not empty after splitting)
    const parsedColumns = columns.split(',').map(c => c.trim()).filter(c => c);
    if (parsedColumns.length === 0) {
        setError('Please provide at least one valid column name.');
        return;
    }

    setLoading(true);
    setError('');
    setSuccess('');
    try {
      // API expects comma-separated string, pass it directly
      const response = await createTable(tableName, columns);
      if (response.data?.status === 'success') {
        setSuccess(`Table '${tableName}' created successfully!`);
        setTableName('');
        setColumns('');
        // Optionally navigate away
        // setTimeout(() => navigate('/tables'), 1500); // Example: Navigate after 1.5s
      } else {
        throw new Error(response.data?.message || 'Failed to create table');
      }
    } catch (err) {
      // Use err.response.data.message if available from Axios error
      setError(err.response?.data?.message || err.message || 'An error occurred.');
    } finally {
      setLoading(false);
    }
  };

  return (
    // Use the standard form container div
    <div className="form-container">
      <h1>Create New Table</h1>
      <form onSubmit={handleSubmit}>
        {/* Use standard message classes */}
        {error && <p className="error-message">{error}</p>}
        {success && <p className="success-message">{success}</p>}

        {/* Use standard form group divs */}
        <div className="form-group">
          <label htmlFor="tableName">Table Name:</label>
          <input
            type="text"
            id="tableName"
            value={tableName}
            onChange={(e) => setTableName(e.target.value)}
            required
            disabled={loading}
            placeholder="e.g., users, products"
          />
           <small>Must be a valid identifier (letters, numbers, _, not starting with number).</small>
        </div>

        <div className="form-group">
          <label htmlFor="columns">Columns (comma-separated):</label>
          <input
            type="text"
            id="columns"
            value={columns}
            onChange={(e) => setColumns(e.target.value)}
            placeholder="e.g., user_id, name, email, created_at"
            required
            disabled={loading}
          />
          <small>Separate column names with commas.</small>
        </div>

        <button type="submit" disabled={loading}>
          {/* Use LoadingSpinner inside the button */}
          {loading ? <LoadingSpinner /> : 'Create Table'}
        </button>
      </form>
    </div>
  );
}

export default CreateTablePage;