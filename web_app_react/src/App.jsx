import React from 'react';
import { Routes, Route } from 'react-router-dom';
import Navbar from './components/Navbar';
import ProtectedRoute from './components/ProtectedRoute';

// Import Pages
import HomePage from './pages/HomePage';
import LoginPage from './pages/LoginPage';
import TablesPage from './pages/TablesPage';
import CreateTablePage from './pages/CreateTablePage';
import QueryTablePage from './pages/QueryTablePage';
import InsertRecordPage from './pages/InsertRecordPage';
import CreateProcedurePage from './pages/CreateProcedurePage';
import DbInfoPage from './pages/DbInfoPage';
import NotFoundPage from './pages/NotFoundPage'; // Create this

function App() {
  return (
    <div className="App">
      <Navbar />
      <main className="container"> {/* Add some basic layout */}
        <Routes>
          {/* Public Route */}
          <Route path="/login" element={<LoginPage />} />

          {/* Protected Routes */}
          <Route element={<ProtectedRoute />}>
            <Route path="/" element={<HomePage />} />
            <Route path="/tables" element={<TablesPage />} />
            <Route path="/create-table" element={<CreateTablePage />} />
            <Route path="/query-table" element={<QueryTablePage />} />
             {/* Dynamic route for specific table query/insert */}
            <Route path="/tables/:tableName/query" element={<QueryTablePage />} />
            <Route path="/tables/:tableName/insert" element={<InsertRecordPage />} />
            <Route path="/insert-record" element={<InsertRecordPage />} /> {/* Generic insert */}
            <Route path="/create-procedure" element={<CreateProcedurePage />} />
            <Route path="/db-info" element={<DbInfoPage />} />
          </Route>

          {/* Catch All Not Found Route */}
          <Route path="*" element={<NotFoundPage />} />
        </Routes>
      </main>
    </div>
  );
}

export default App;