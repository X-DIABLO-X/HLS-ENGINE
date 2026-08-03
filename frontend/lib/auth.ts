'use client';

import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import axios from 'axios';
import { useSyncExternalStore } from 'react';
import { AuthUser, LoginResponse, RegisterResponse, RefreshResponse } from '@/types/auth';

const authApi = axios.create({
  baseURL: '/api/v1/auth',
  headers: { 'Content-Type': 'application/json' },
  timeout: 30000,
});

interface AuthState {
  token: string | null;
  refreshToken: string | null;
  user: AuthUser | null;
  isAuthenticated: boolean;
  login: (email: string, password: string) => Promise<void>;
  register: (email: string, username: string, password: string) => Promise<void>;
  logout: () => void;
  refresh: () => Promise<boolean>;
  getToken: () => string | null;
}

export const useAuthStore = create<AuthState>()(
  persist(
    (set, get) => ({
      token: null,
      refreshToken: null,
      user: null,
      isAuthenticated: false,

      login: async (email: string, password: string) => {
        const { data } = await authApi.post<LoginResponse>('/login', {
          email,
          password,
        });
        set({
          token: data.access_token,
          refreshToken: data.refresh_token,
          user: data.user,
          isAuthenticated: true,
        });
      },

      register: async (email: string, username: string, password: string) => {
        await authApi.post<RegisterResponse>('/register', {
          email,
          username,
          password,
        });
        const { data } = await authApi.post<LoginResponse>('/login', {
          email,
          password,
        });
        set({
          token: data.access_token,
          refreshToken: data.refresh_token,
          user: data.user,
          isAuthenticated: true,
        });
      },

      logout: () => {
        set({
          token: null,
          refreshToken: null,
          user: null,
          isAuthenticated: false,
        });
      },

      refresh: async () => {
        const currentRefreshToken = get().refreshToken;
        if (!currentRefreshToken) {
          set({ token: null, refreshToken: null, user: null, isAuthenticated: false });
          return false;
        }
        try {
          const { data } = await authApi.post<RefreshResponse>('/refresh', {
            refresh_token: currentRefreshToken,
          });
          set({ token: data.access_token });
          return true;
        } catch {
          set({ token: null, refreshToken: null, user: null, isAuthenticated: false });
          return false;
        }
      },

      getToken: () => get().token,
    }),
    {
      name: 'hls-engine-auth',
      partialize: (state) => ({
        token: state.token,
        refreshToken: state.refreshToken,
        user: state.user,
        isAuthenticated: state.isAuthenticated,
      }),
    }
  )
);

function subscribeToAuthHydration(onStoreChange: () => void) {
  const unsubscribeHydrate = useAuthStore.persist.onHydrate(onStoreChange);
  const unsubscribeFinishHydration =
    useAuthStore.persist.onFinishHydration(onStoreChange);

  return () => {
    unsubscribeHydrate();
    unsubscribeFinishHydration();
  };
}

export function useAuthHydrated(): boolean {
  return useSyncExternalStore(
    subscribeToAuthHydration,
    () => useAuthStore.persist.hasHydrated(),
    () => false
  );
}
