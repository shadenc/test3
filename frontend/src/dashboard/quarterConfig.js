/**
 * Maps UI quarter filter (Q1–Q4) to evidence keys, net-profit keys, and grid headers.
 */

export const DASHBOARD_QUARTER_CONFIG = {
  Q1: {
    evidencePrev: 'Annual_2024',
    evidenceCurr: 'Q1_2025',
    netProfitKey: 'Q1 2025',
    previousHeader: '2024Q4',
    currentHeader: '2025Q1',
  },
  Q2: {
    evidencePrev: 'Q1_2025',
    evidenceCurr: 'Q2_2025',
    netProfitKey: 'Q2 2025',
    previousHeader: '2025Q1',
    currentHeader: '2025Q2',
  },
  Q3: {
    evidencePrev: 'Q2_2025',
    evidenceCurr: 'Q3_2025',
    netProfitKey: 'Q3 2025',
    previousHeader: '2025Q2',
    currentHeader: '2025Q3',
  },
  Q4: {
    evidencePrev: 'Q3_2025',
    evidenceCurr: 'Q4_2025',
    netProfitKey: 'Q4 2025',
    previousHeader: '2025Q3',
    currentHeader: '2025Q4',
  },
};

export function quarterDashboardConfig(quarterFilter) {
  return DASHBOARD_QUARTER_CONFIG[quarterFilter] || DASHBOARD_QUARTER_CONFIG.Q1;
}

export function evidenceQuarterForPreviousColumn(quarterFilter) {
  return quarterDashboardConfig(quarterFilter).evidencePrev;
}

export function evidenceQuarterForCurrentColumn(quarterFilter) {
  return quarterDashboardConfig(quarterFilter).evidenceCurr;
}

export function netProfitQuarterKeyFromFilter(quarterFilter) {
  return quarterDashboardConfig(quarterFilter).netProfitKey;
}

export function previousQuarterHeaderLabel(qf) {
  return quarterDashboardConfig(qf).previousHeader;
}

export function currentQuarterHeaderLabel(qf) {
  return quarterDashboardConfig(qf).currentHeader;
}
