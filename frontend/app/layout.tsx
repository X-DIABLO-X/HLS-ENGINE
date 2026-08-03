import type { Metadata } from 'next';
import Link from 'next/link';
import './globals.css';
import { AuthNav } from '@/components/auth/AuthNav';

export const metadata: Metadata = {
  title: 'HLS Engine',
  description: 'Adaptive bitrate streaming dashboard',
  icons: {
    icon: '/favicon.ico',
  },
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className="min-h-screen">
        <header className="sticky top-0 z-50 border-b border-border bg-background/80 backdrop-blur-md">
          <div className="mx-auto flex h-16 max-w-7xl items-center justify-between px-4 sm:px-6 lg:px-8">
            <Link href="/" className="text-xl font-bold tracking-tight">
              HLS Engine
            </Link>
            <nav className="flex items-center gap-4">
              <Link
                href="/"
                className="text-sm font-medium text-muted-foreground transition-colors hover:text-foreground"
              >
                Upload
              </Link>
              <Link
                href="/videos"
                className="text-sm font-medium text-muted-foreground transition-colors hover:text-foreground"
              >
                Library
              </Link>
              <Link
                href="/settings"
                className="text-sm font-medium text-muted-foreground transition-colors hover:text-foreground"
              >
                Settings
              </Link>
              <AuthNav />
            </nav>
          </div>
        </header>
        <main className="mx-auto max-w-7xl px-4 py-8 sm:px-6 lg:px-8">
          {children}
        </main>
      </body>
    </html>
  );
}
