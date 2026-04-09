/**
 * HTTP client for the Flask evidence API: base URL, CSRF (Flask-WTF), and common verbs.
 */

export const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:5003';

export function apiUrl(relPath) {
  const p = String(relPath).replace(/^\/+/, '');
  return `${API_URL}/${p}`;
}

export function buildEvidenceScreenshotUrl(companySymbol, requestedQuarter = 'Q1_2025') {
  return `${API_URL}/api/evidence/${companySymbol}.png?quarter=${requestedQuarter || 'Q1_2025'}&t=${Date.now()}`;
}

let csrfTokenPromise = null;

export async function getCsrfToken() {
  if (!csrfTokenPromise) {
    csrfTokenPromise = fetch(apiUrl('api/csrf-token'))
      .then((r) => {
        if (!r.ok) throw new Error(`CSRF token HTTP ${r.status}`);
        return r.json();
      })
      .then((j) => j.csrf_token);
  }
  return csrfTokenPromise;
}

export async function withCsrfHeaders(headersInit) {
  const token = await getCsrfToken();
  const h = new Headers(headersInit || {});
  h.set('X-CSRFToken', token);
  return h;
}

export async function postJsonWithCsrf(relPath) {
  const res = await fetch(apiUrl(relPath), {
    method: 'POST',
    headers: await withCsrfHeaders(),
  });
  let data = {};
  try {
    data = await res.json();
  } catch {
    /* non-JSON body */
  }
  return { res, data };
}

export async function postCsrfOptionalLog(relPath, logFn, logTag) {
  try {
    await fetch(apiUrl(relPath), { method: 'POST', headers: await withCsrfHeaders() });
  } catch (e) {
    if (typeof logFn === 'function') logFn(logTag, e);
  }
}
