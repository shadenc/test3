/**
 * API layer: URL building, CSRF-aware fetch, and job status polling.
 */

export {
  API_URL,
  apiUrl,
  buildEvidenceScreenshotUrl,
  getCsrfToken,
  postCsrfOptionalLog,
  postJsonWithCsrf,
  withCsrfHeaders,
} from './apiClient';
export { startJobStatusPoll } from './statusPolling';
