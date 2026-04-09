/**
 * Pure helpers for the investment dashboard: CSV parsing, row merging, export messages.
 * Depends only on `services/apiClient` for URL building—no React.
 */

import Papa from 'papaparse';
import { apiUrl } from '../services/apiClient';

export const devLog =
  process.env.NODE_ENV === 'development'
    ? (...args) => {
        console.log(...args);
      }
    : () => {};

export const CONTENT_DISPOSITION_FILENAME_RE = /filename="(.+)"/;

export function parseFilenameFromContentDisposition(contentDisposition) {
  if (!contentDisposition) return 'dashboard_table.xlsx';
  const m = CONTENT_DISPOSITION_FILENAME_RE.exec(contentDisposition);
  return m ? m[1] : 'dashboard_table.xlsx';
}

export function trimCsvRowCells(row) {
  const cleanedRow = {};
  for (const key of Object.keys(row)) {
    const cleanKey = key.trim();
    cleanedRow[cleanKey] = row[key] ? String(row[key]).trim() : '';
  }
  return cleanedRow;
}

export function flowSignedTypographyParts(numValue) {
  const isPositive = numValue >= 0;
  const color = isPositive ? '#2e7d32' : '#d32f2f';
  let sign = '';
  if (numValue > 0) sign = '+';
  return { color, sign };
}

const GRID_EMPTY_VALUE_SENTINELS = new Set(['', 'null', 'undefined']);

export function isDataGridCellEmpty(value) {
  return !value || GRID_EMPTY_VALUE_SENTINELS.has(value);
}

export function mergeCorrectionIntoRows(prevRows, updated) {
  return prevRows.map((row) => {
    if (!row.symbol || !updated.company_symbol) return row;
    if (row.symbol.toString() !== updated.company_symbol.toString()) return row;
    return {
      ...row,
      retained_earnings: updated.retained_earnings || updated.value || '',
      reinvested_earnings: updated.reinvested_earnings || '',
      year: updated.year || '',
      error: updated.error || '',
    };
  });
}

export const QUARTERS_Q1_Q4 = ['Q1', 'Q2', 'Q3', 'Q4'];

export function buildFlowMapFromQuarterlyRows(quarterlyFlowData) {
  const flowMap = {};
  quarterlyFlowData.forEach((row) => {
    const symbol = row.company_symbol ? row.company_symbol.toString().trim() : '';
    const quarter = row.quarter ? row.quarter.toString().trim() : '';
    if (!symbol || !quarter) {
      return;
    }
    if (!flowMap[symbol]) {
      flowMap[symbol] = {};
    }
    flowMap[symbol][quarter] = {
      previous_value: row.previous_value || '',
      current_value: row.current_value || '',
      flow: row.flow || '',
      flow_formula: row.flow_formula || '',
      year: row.year || '',
      foreign_investor_flow: row.reinvested_earnings_flow || '',
      net_profit_foreign_investor: row.net_profit_foreign_investor || '',
      distributed_profits_foreign_investor: row.distributed_profits_foreign_investor || '',
    };
    devLog(`Mapped flow data for ${symbol} ${quarter}:`, {
      ...flowMap[symbol][quarter],
      net_profit_foreign_investor: row.net_profit_foreign_investor,
      distributed_profits_foreign_investor: row.distributed_profits_foreign_investor,
    });
  });
  return flowMap;
}

export function mergeOwnershipWithQuarterlyFlow(foreignOwnershipData, flowMap, onEvidenceClick) {
  const mergedData = [];
  foreignOwnershipData.forEach((row, idx) => {
    const symbol = row.symbol ? row.symbol.toString().trim() : '';
    const flowData = flowMap[symbol] || {};
    QUARTERS_Q1_Q4.forEach((quarter) => {
      const quarterData = flowData[quarter] || {};
      const mergedRow = {
        ...row,
        company_symbol: symbol,
        previous_quarter_value: quarterData.previous_value || '',
        current_quarter_value: quarterData.current_value || '',
        flow: quarterData.flow || '',
        flow_formula: quarterData.flow_formula || '',
        year: quarterData.year || '',
        foreign_investor_flow: quarterData.foreign_investor_flow || '',
        net_profit_foreign_investor: quarterData.net_profit_foreign_investor || '',
        distributed_profits_foreign_investor: quarterData.distributed_profits_foreign_investor || '',
        quarter,
        id: `${symbol}_${quarter}_${idx}`,
        onEvidenceClick,
      };
      if (mergedData.length < 5 || symbol === '2222') {
        devLog(`Row ${mergedData.length} (${symbol} ${quarter}):`, {
          symbol: mergedRow.symbol,
          company_name: mergedRow.company_name,
          quarter: mergedRow.quarter,
          flow: mergedRow.flow,
          flow_formula: mergedRow.flow_formula,
        });
      }
      mergedData.push(mergedRow);
    });
  });
  return mergedData;
}

export function combineDashboardRows(foreignOwnershipData, quarterlyFlowData, onEvidenceClick) {
  const flowMap = buildFlowMapFromQuarterlyRows(quarterlyFlowData);
  return mergeOwnershipWithQuarterlyFlow(foreignOwnershipData, flowMap, onEvidenceClick);
}

export function parseQuarterlyFlowCsvText(csvText) {
  return new Promise((resolve) => {
    Papa.parse(csvText, {
      header: true,
      complete: (result) => {
        devLog('CSV parsing result:', result);
        if (result.data && result.data.length > 0) {
          const cleanedData = result.data
            .filter((row) => row.company_symbol && row.company_symbol.trim() !== '')
            .map(trimCsvRowCells);
          devLog('Cleaned CSV data:', cleanedData);
          resolve(cleanedData);
        } else {
          devLog('No CSV data found');
          resolve([]);
        }
      },
      error: (error) => {
        console.error('Error parsing CSV data:', error);
        resolve([]);
      },
    });
  });
}

/** Shared GET + JSON list loader (snapshots sidebar, exports list, refresh after download). */
export function fetchBackendJsonList(relPath, options) {
  const {
    setLoading,
    setData,
    setError,
    errorMessage,
    devLog,
    logTag,
    initialLog,
  } = options;
  if (initialLog) devLog(initialLog);
  setLoading(true);
  return fetch(apiUrl(relPath))
    .then((res) => {
      devLog(`${logTag} status:`, res.status);
      if (!res.ok) {
        throw new Error(`HTTP error! status: ${res.status}`);
      }
      return res.json();
    })
    .then((data) => {
      devLog(`${logTag} data:`, data);
      setData(data);
      setLoading(false);
    })
    .catch((err) => {
      console.error(`Error fetching ${relPath}:`, err);
      setError(errorMessage);
      setLoading(false);
    });
}

export function buildCustomDateExportErrorMessage(error) {
  const msg = error?.message || '';
  const base = '❌ فشل في تصدير ملف Excel\n\n';
  if (msg.includes('404')) {
    return `${base}🔍 السبب: لم يتم العثور على البيانات المطلوبة\n💡 الحل: تأكد من وجود البيانات للربع المحدد`;
  }
  if (msg.includes('500')) {
    return `${base}🔧 السبب: خطأ في الخادم\n💡 الحل: حاول مرة أخرى أو اتصل بالدعم الفني`;
  }
  if (msg.includes('fetch')) {
    return `${base}🌐 السبب: مشكلة في الاتصال\n💡 الحل: تأكد من تشغيل الخادم`;
  }
  return `${base}🔍 السبب: ${msg}\n💡 الحل: حاول مرة أخرى`;
}
