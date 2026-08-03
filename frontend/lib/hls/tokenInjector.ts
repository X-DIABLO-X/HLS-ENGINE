import { HlsConfig } from 'hls.js';

/**
 * Creates an hls.js config fragment that injects an Authorization header
 * into every XHR request made by the player.
 *
 * The token can be provided explicitly or read from a function (for refresh).
 */
export interface TokenInjectorOptions {
  getToken: () => string | undefined | null;
  headerName?: string;
}

export function createTokenInjector(
  options: TokenInjectorOptions
): Partial<HlsConfig> {
  const headerName = options.headerName || 'Authorization';

  return {
    xhrSetup: (xhr: XMLHttpRequest, url: string) => {
      const token = options.getToken();
      if (token) {
        const value = token.startsWith('Bearer ') ? token : `Bearer ${token}`;
        xhr.setRequestHeader(headerName, value);
      }
    },
  };
}

/**
 * Convenience helper for a static token string.
 */
export function createStaticTokenInjector(
  token: string | undefined | null,
  headerName?: string
): Partial<HlsConfig> {
  return createTokenInjector({
    getToken: () => token,
    headerName,
  });
}
