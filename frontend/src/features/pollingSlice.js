import { createSlice, createAsyncThunk } from '@reduxjs/toolkit';

// Async thunk to fetch individual phone status
export const fetchPhoneStatus = createAsyncThunk(
  'polling/fetchPhoneStatus',
  async (phoneUUID, { rejectWithValue }) => {
    try {
      const response = await fetch(`http://localhost:8080/phones/${phoneUUID}/status`);
      if (!response.ok) {
        throw new Error('Network response was not ok');
      }
      const data = await response.json();
      return data; // { phoneUUID, status }
    } catch (error) {
      return rejectWithValue(error.message);
    }
  }
);

const pollingSlice = createSlice({
  name: 'polling',
  initialState: {
    statuses: {}, // Object mapping phoneUUID to its status
  },
  reducers: {},
  extraReducers: (builder) => {
    builder
      .addCase(fetchPhoneStatus.fulfilled, (state, action) => {
        if (action.payload) {
          const { phoneUUID, status } = action.payload;
          state.statuses[phoneUUID] = status;
        }
      });
  },
});

export default pollingSlice.reducer;
