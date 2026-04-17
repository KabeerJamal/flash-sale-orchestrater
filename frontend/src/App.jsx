import React, { useState, useEffect } from 'react';
import { useDispatch, useSelector } from 'react-redux';
import { buyPhone, getUsers, getPhones, clearFlashMessage } from './features/saleSlice';
import { fetchPhoneStatus } from './features/pollingSlice';
import './App.css';

function App() {
  const [selectedUserUUID, setSelectedUserUUID] = useState('');

  const dispatch = useDispatch();
  const { loading, error, success, flashMessage, users, usersLoading, usersError, phones, phonesLoading, phonesError } = useSelector((state) => state.sale);
  const { statuses } = useSelector((state) => state.polling);

  useEffect(() => {
    dispatch(getUsers());
    dispatch(getPhones());
  }, [dispatch]);

  useEffect(() => {
    if (users && users.length > 0 && !selectedUserUUID) {
      setSelectedUserUUID(users[0].UserUUID || users[0].userUUID); // safeguard depending on case
    }
  }, [users, selectedUserUUID]);

  // Set up polling interval for phone statuses
  useEffect(() => {
    if (phones && phones.length > 0) {
      // Function to fetch status for all available phones
      const fetchAllStatuses = () => {
        phones.forEach(phone => {
          dispatch(fetchPhoneStatus(phone.id));
        });
      };

      fetchAllStatuses(); // initial fetch immediately when available
      const interval = setInterval(fetchAllStatuses, 3000); // Poll every 3 seconds

      return () => clearInterval(interval);
    }
  }, [phones, dispatch]);

  useEffect(() => {
    if (flashMessage) {
      const timer = setTimeout(() => {
        dispatch(clearFlashMessage());
      }, 5000); // clear toast after 5 seconds
      return () => clearTimeout(timer);
    }
  }, [flashMessage, dispatch]);

  const handleBuy = (id) => {
    if (!selectedUserUUID) {
      alert("Please select a user first");
      return;
    }
    dispatch(buyPhone({ phoneUUID: id, userUUID: selectedUserUUID }));
  };

  return (
    <div className="app-container">
      <header className="header">
        <h1>⚡ Flash Sale Master ⚡</h1>
        <p>Hurry! Inventory is dropping fast. Grab your device.</p>

        {flashMessage && <div className="status-message info">{flashMessage}</div>}
        {loading && !flashMessage && <div className="status-message loading">Processing your order...</div>}
        {error && <div className="status-message error">{error}</div>}
        {success && !flashMessage && <div className="status-message success">Request sent successfully!</div>}

        <div className="user-selector">
          <label htmlFor="user-select">Buying as:</label>
          <select
            id="user-select"
            value={selectedUserUUID}
            onChange={(e) => setSelectedUserUUID(e.target.value)}
          >
            {usersLoading && <option value="" disabled>Loading users...</option>}
            {!usersLoading && (!users || users.length === 0) && <option value="" disabled>No users found</option>}
            {users.map(u => (
              <option key={u.userUUID} value={u.userUUID}>
                {u.userName}
              </option>
            ))}
          </select>
        </div>
      </header>

      <main className="grid-container">
        {phonesLoading && <div className="status-message loading">Loading phones...</div>}
        {phonesError && <div className="status-message error">Failed to load phones!</div>}
        {!phonesLoading && phones.length === 0 && <div className="status-message">No phones available.</div>}
        {phones.map(phone => {
          const currentStatus = statuses[phone.id] || 'AVAILABLE'; // Default to AVAILABLE

          let buttonText = 'Buy Now';
          let isDisabled = false;

          if (currentStatus === 'RESERVED') {
            buttonText = 'Reserved';
            isDisabled = true;
          } else if (currentStatus === 'PAID' || currentStatus === 'SOLD_OUT') {
            buttonText = 'Sold Out';
            isDisabled = true;
          } else if (currentStatus === 'CANCELLED') {
            buttonText = 'Buy Now';
            isDisabled = false;
          }

          return (
            <div key={phone.id} className="phone-card">
              <div className="phone-image-placeholder">
                <span>{phone.icon}</span>
              </div>
              <div className="phone-details">
                <h2>{phone.name}</h2>
                {/* <p className="uuid-text">{phone.id}</p> */}
                <span className={`status status-${currentStatus.toLowerCase()}`}>
                  {currentStatus}
                </span>
                <div className="price-row">
                  <span className="price">{phone.price}</span>
                  <button
                    className={`buy-button ${(currentStatus === 'PAID' || currentStatus === 'SOLD_OUT') ? 'btn-soldout' : currentStatus === 'RESERVED' ? 'btn-reserved' : ''}`}
                    onClick={() => handleBuy(phone.id)}
                    disabled={loading || isDisabled}
                  >
                    {buttonText}
                  </button>
                </div>
              </div>
            </div>
          )
        })}
      </main>
    </div>
  );
}

export default App;
