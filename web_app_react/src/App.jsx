import React from 'react';
import { Routes, Route } from 'react-router-dom';
import Navbar from './components/Navbar';
import ProtectedRoute from './components/ProtectedRoute';

// Import Pages
import HomePage from './pages/HomePage';
import LoginPage from './pages/LoginPage';
import TablesPage from './pages/TablesPage';
import CreateTablePage from './pages/CreateTablePage';
import QueryTablePage from './pages/QueryTablePage';       // <-- Import
import InsertRecordPage from './pages/InsertRecordPage';     // <-- Import
import CreateProcedurePage from './pages/CreateProcedurePage'; // <-- Import
import DbInfoPage from './pages/DbInfoPage';             // <-- Import
import NotFoundPage from './pages/NotFoundPage';           // <-- Import (Optional)

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
            {/* Add routes for other pages */}
            <Route path="/query-table" element={<QueryTablePage />} />       {/* <-- Add Route */}
            {/* Specific table query? Maybe later: <Route path="/tables/:tableName/query" element={<QueryTablePage />} /> */}
            <Route path="/insert-record" element={<InsertRecordPage />} />     {/* <-- Add Route */}
            {/* Specific table insert? Maybe later: <Route path="/tables/:tableName/insert" element={<InsertRecordPage />} /> */}
            <Route path="/create-procedure" element={<CreateProcedurePage />} />{/* <-- Add Route */}
            <Route path="/db-info" element={<DbInfoPage />} />             {/* <-- Add Route */}
          </Route>

          {/* Catch All Not Found Route */}
          <Route path="*" element={<NotFoundPage />} /> {/* <-- Add Route (Optional) */}
        </Routes>
      </main>
    </div>
  );
}

export default App;