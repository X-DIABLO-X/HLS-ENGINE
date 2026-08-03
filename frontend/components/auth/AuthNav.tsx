'use client';

import Link from 'next/link';
import { useAuthHydrated, useAuthStore } from '@/lib/auth';
import { LogOut, User } from 'lucide-react';

export function AuthNav() {
  const { user, isAuthenticated, logout } = useAuthStore();
  const hasHydrated = useAuthHydrated();

  if (!hasHydrated) {
    return null;
  }

  if (!isAuthenticated) {
    return (
      <Link
        href="/login"
        className="rounded-lg bg-accent px-4 py-1.5 text-sm font-medium text-white transition-colors hover:bg-accent/90"
      >
        Sign in
      </Link>
    );
  }

  return (
    <div className="flex items-center gap-3">
      <div className="flex items-center gap-2 text-sm text-muted-foreground">
        <User className="h-4 w-4" />
        <span className="font-medium text-foreground">{user?.username || user?.email}</span>
      </div>
      <button
        onClick={() => {
          logout();
          window.location.href = '/login';
        }}
        className="flex items-center gap-1 rounded-lg border border-border px-3 py-1.5 text-sm text-muted-foreground transition-colors hover:bg-white/5 hover:text-foreground"
      >
        <LogOut className="h-4 w-4" />
        Logout
      </button>
    </div>
  );
}
