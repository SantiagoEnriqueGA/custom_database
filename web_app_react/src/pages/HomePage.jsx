// src/pages/HomePage.jsx
import React from 'react';
import useAuth from '../hooks/useAuth'; // Optional: Use auth info if needed

function HomePage() {
  const { isAuthenticated } = useAuth(); // Example usage
  const username = sessionStorage.getItem('username'); // Or get from context if stored there

  return (
    <div>
      <h1>Welcome to SegaDB Admin</h1>
      {isAuthenticated ? (
        <p>You are logged in{username ? ` as ${username}` : ''}.</p>
        // Add links or information relevant to the home page
      ) : (
        <p>Please log in to manage the database.</p>
      )}
      {/* Add dashboard elements, stats, quick links etc. */}
    </div>
  );
}

export default HomePage;