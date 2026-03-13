import { createSlice, createAsyncThunk } from '@reduxjs/toolkit';

// Async thunk for fetching phones
export const getPhones = createAsyncThunk(
  'sale/getPhones',
  async (_, { rejectWithValue }) => {
    try {
      const response = await fetch('http://localhost:8080/phones');
      if (!response.ok) {
        throw new Error('Network response was not ok');
      }
      const data = await response.json();
      return data;
    } catch (error) {
      return rejectWithValue(error.message);
    }
  }
);

// Async thunk for fetching users
export const getUsers = createAsyncThunk(
  'sale/getUsers',
  async (_, { rejectWithValue }) => {
    try {
      const response = await fetch('http://localhost:8080/users');
      if (!response.ok) {
        throw new Error('Network response was not ok');
      }
      const data = await response.json();
      return data;
    } catch (error) {
      return rejectWithValue(error.message);
    }
  }
);

// Async thunk for sending a buy request, polling ticket status, and initiating payment
export const buyPhone = createAsyncThunk(
  'sale/buyPhone',
  async ({ phoneUUID, userUUID }, { rejectWithValue, dispatch }) => {
    try {
      // 1. Initial buy request
      const response = await fetch('http://localhost:8080/buy-request', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ phoneUUID, userUUID }),
      });

      if (!response.ok) {
        throw new Error('Buy request failed');
      }

      const data = await response.json().catch(() => ({}));
      const ticketUUID = data.ticketUUID;

      if (!ticketUUID) {
        throw new Error('No ticket received');
      }

      // 2. Poll for SUCCESSFUL_RESERVATION
      let currentStatus = data.status || 'PENDING';
      let notifiedWaitingList = false;
      
      while (currentStatus !== 'SUCCESSFUL_RESERVATION' && currentStatus !== 'SOLD_OUT') {
        // Wait 1 second between polls
        await new Promise(resolve => setTimeout(resolve, 1000));
        
        try {
          const statusRes = await fetch(`http://localhost:8080/status/${ticketUUID}`);
          if (statusRes.ok) {
            const statusData = await statusRes.json();
            currentStatus = statusData.status;

            if (currentStatus === 'WAITING_LIST' && !notifiedWaitingList) {
              dispatch(setFlashMessage('Hey, you are in the waiting list. We will update you shortly!'));
              notifiedWaitingList = true;
            }
          }
          // If statusRes.status === 404, we continue polling
        } catch (err) {
          console.error("Polling error:", err);
        }
      }

      if (currentStatus === 'SOLD_OUT') {
        dispatch(setFlashMessage('Sorry, this phone is officially sold out!'));
        return { status: 'SOLD_OUT' };
      }

      dispatch(setFlashMessage('Reservation successful! Redirecting to payment...'));

      // 3. Initiate payment
      const payRes = await fetch('http://localhost:8080/pay', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ phoneUUID, ticketUUID, userUUID }),
      });

      if (!payRes.ok) {
        throw new Error('Payment request failed');
      }

      const payData = await payRes.json();

      // 4. Redirect to payment URL
      if (payData.url) {
        window.location.href = payData.url;
      }

      return payData;
    } catch (error) {
      return rejectWithValue(error.message);
    }
  }
);

const saleSlice = createSlice({
  name: 'sale',
  initialState: {
    loading: false,
    error: null,
    success: null,
    flashMessage: null,
    users: [],
    usersLoading: false,
    usersError: null,
    phones: [],
    phonesLoading: false,
    phonesError: null,
  },
  reducers: {
    resetState: (state) => {
      state.loading = false;
      state.error = null;
      state.success = null;
      state.flashMessage = null;
    },
    setFlashMessage: (state, action) => {
      state.flashMessage = action.payload;
    },
    clearFlashMessage: (state) => {
      state.flashMessage = null;
    }
  },
  extraReducers: (builder) => {
    builder
      .addCase(buyPhone.pending, (state) => {
        state.loading = true;
        state.error = null;
        state.success = null;
      })
      .addCase(buyPhone.fulfilled, (state, action) => {
        state.loading = false;
        state.success = action.payload || true;
        state.error = null;
      })
      .addCase(buyPhone.rejected, (state, action) => {
        state.loading = false;
        state.error = action.payload || 'Failed to buy phone';
        state.success = null;
      })
      .addCase(getUsers.pending, (state) => {
        state.usersLoading = true;
        state.usersError = null;
      })
      .addCase(getUsers.fulfilled, (state, action) => {
        state.usersLoading = false;
        state.users = action.payload || [];
        state.usersError = null;
      })
      .addCase(getUsers.rejected, (state, action) => {
        state.usersLoading = false;
        state.usersError = action.payload || 'Failed to fetch users';
      })
      .addCase(getPhones.pending, (state) => {
        state.phonesLoading = true;
        state.phonesError = null;
      })
      .addCase(getPhones.fulfilled, (state, action) => {
        state.phonesLoading = false;
        const icons = ['📱', '📲', '🔋', '📞', '📸'];
        state.phones = (action.payload || []).map((phone, index) => ({
          id: phone.phoneUUID,
          name: phone.phoneName,
          price: `$${(499 + index * 100).toFixed(2)}`,
          icon: icons[index % icons.length]
        }));
        state.phonesError = null;
      })
      .addCase(getPhones.rejected, (state, action) => {
        state.phonesLoading = false;
        state.phonesError = action.payload || 'Failed to fetch phones';
      });
  },
});

export const { resetState, setFlashMessage, clearFlashMessage } = saleSlice.actions;
export default saleSlice.reducer;
