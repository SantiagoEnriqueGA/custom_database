import React, { useState } from 'react';
import { createProcedure } from '../services/api';
import LoadingSpinner from '../components/LoadingSpinner';
import './Forms.css'; // Reuse form styles

function CreateProcedurePage() {
  const [procedureName, setProcedureName] = useState('');
  const [procedureCode, setProcedureCode] = useState('"""\nDocstring for the procedure.\nArgs:\n    db: The database instance.\n    param1: Description of param1.\n"""\n# Your Python code here\n# Example: return db.get_table("some_table")\npass');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError('');
    setSuccess('');
    try {
       // Basic validation
       if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(procedureName)) {
           throw new Error('Procedure name must be a valid Python identifier (letters, numbers, underscores, not starting with a number).');
       }

      const response = await createProcedure(procedureName, procedureCode);
      if (response.data?.status === 'success') {
        setSuccess(`Stored procedure '${procedureName}' created successfully!`);
        // Optionally clear the form
        // setProcedureName('');
        // setProcedureCode('');
      } else {
        throw new Error(response.data?.message || 'Failed to create procedure');
      }
    } catch (err) {
      setError(`Error: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="form-container">
      <h1>Create Stored Procedure</h1>
      <p style={{color: '#ffcc00', border: '1px solid #ffcc00', padding: '10px', borderRadius: '4px', backgroundColor: '#332e00'}}>
        <strong>Security Warning:</strong> Creating procedures executes arbitrary Python code on the server.
        This feature is highly insecure if the server is exposed. Use with extreme caution.
      </p>
      <form onSubmit={handleSubmit}>
        {error && <p className="error-message">{error}</p>}
        {success && <p className="success-message">{success}</p>}

        <div className="form-group">
          <label htmlFor="procedureName">Procedure Name:</label>
          <input
            type="text"
            id="procedureName"
            value={procedureName}
            onChange={(e) => setProcedureName(e.target.value)}
            required
            disabled={loading}
            placeholder="e.g., get_active_users"
          />
        </div>

        <div className="form-group">
          <label htmlFor="procedureCode">Procedure Code (Python function body):</label>
          <textarea
            id="procedureCode"
            value={procedureCode}
            onChange={(e) => setProcedureCode(e.target.value)}
            rows={15} // More rows for code
            required
            disabled={loading}
            placeholder="Enter the Python code for the procedure function body. It will receive 'db' as the first argument."
            spellCheck="false" // Disable spellcheck for code
          />
          <small>The function signature will be `def {procedureName}(db, *args, **kwargs):`</small>
        </div>

        <button type="submit" disabled={loading}>
          {loading ? <LoadingSpinner /> : 'Create Procedure'}
        </button>
      </form>
    </div>
  );
}

export default CreateProcedurePage;