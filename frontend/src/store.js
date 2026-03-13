import { configureStore } from '@reduxjs/toolkit';
import saleReducer from './features/saleSlice';
import pollingReducer from './features/pollingSlice';

export const store = configureStore({
  reducer: {
    sale: saleReducer,
    polling: pollingReducer,
  },
});
