/**
 * Interval polling for long-running backend jobs (PDF pipeline, net-profit scrape).
 * Keeps UI state in sync with /api/pdfs/status and /api/net_profit/status.
 */

import { apiUrl } from './apiClient';

/**
 * Polls GET `path` until `isDone(payload)` is true; then clears the interval,
 * runs optional `beforeReload`, after 300ms runs `reloadFns` and `onComplete`.
 */
export function startJobStatusPoll(options) {
  const {
    isAlreadyPolling,
    setPollIntervalId,
    path,
    setStatus,
    isDone,
    beforeReload,
    reloadFns,
    onComplete,
    logLabel,
    log,
    intervalMs = 1500,
  } = options;

  if (isAlreadyPolling()) return;
  const id = setInterval(async () => {
    try {
      const res = await fetch(apiUrl(path));
      const data = await res.json();
      setStatus(data);
      if (isDone(data)) {
        clearInterval(id);
        setPollIntervalId(null);
        if (typeof beforeReload === 'function') beforeReload();
        setTimeout(() => {
          (reloadFns || []).forEach((fn) => {
            if (typeof fn === 'function') fn();
          });
          if (typeof onComplete === 'function') onComplete();
        }, 300);
      }
    } catch (e) {
      log(logLabel, e);
    }
  }, intervalMs);
  setPollIntervalId(id);
}
