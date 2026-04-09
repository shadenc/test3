/**
 * MUI DataGrid column model and cell renderers for the foreign-investment dashboard.
 */

import React from 'react';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import Tooltip from '@mui/material/Tooltip';
import IconButton from '@mui/material/IconButton';
import VisibilityIcon from '@mui/icons-material/Visibility';
import {
  currentQuarterHeaderLabel,
  evidenceQuarterForCurrentColumn,
  evidenceQuarterForPreviousColumn,
  netProfitQuarterKeyFromFilter,
  previousQuarterHeaderLabel,
} from './quarterConfig';
import { flowSignedTypographyParts, isDataGridCellEmpty } from './dataUtils';

const EVIDENCE_EYE_ICON_SX = {
  color: '#1e6641',
  '&:hover': { bgcolor: '#e8f5ee' },
  padding: '8px',
  minWidth: '40px',
  width: '40px',
  height: '40px',
};

export function renderSignedSarFlowGridCell(params) {
  const value = params.value;
  if (isDataGridCellEmpty(value)) {
    return 'لايوجد';
  }
  const numValue = Number.parseFloat(value);
  if (!Number.isNaN(numValue)) {
    const { color, sign } = flowSignedTypographyParts(numValue);
    return (
      <Typography sx={{ color, fontWeight: 'bold' }}>
        {sign}{numValue.toLocaleString('en-US')} SAR
      </Typography>
    );
  }
  return value;
}

export function renderRetainedQuarterGridCell(
  params,
  quarterFilter,
  fetchEvidenceData,
  setEvidenceModalOpen,
  evidenceQuarterKeyFn,
) {
  const value = params.value;
  if (isDataGridCellEmpty(value)) {
    return 'لايوجد';
  }
  const numValue = Number.parseFloat(value);
  if (!Number.isNaN(numValue)) {
    return (
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
        <Typography>{numValue.toLocaleString('en-US')}</Typography>
        <Tooltip title="عرض دليل الاستخراج - انقر لرؤية المستند الأصلي" arrow placement="top">
          <IconButton
            size="small"
            onClick={(e) => {
              e.stopPropagation();
              fetchEvidenceData(params.row.symbol, evidenceQuarterKeyFn(quarterFilter));
              setEvidenceModalOpen(true);
            }}
            sx={EVIDENCE_EYE_ICON_SX}
          >
            <VisibilityIcon sx={{ fontSize: '16px' }} />
          </IconButton>
        </Tooltip>
      </Box>
    );
  }
  return value;
}

export function buildDataGridColumns(quarterFilter, netProfitData, fetchEvidenceData, setEvidenceModalOpen) {
  return [
    { field: 'symbol', headerName: 'رمز الشركة', width: 120, align: 'right', headerAlign: 'right' },
    { field: 'company_name', headerName: 'الشركة', width: 200, align: 'right', headerAlign: 'right' },
    { field: 'foreign_ownership', headerName: 'ملكية جميع المستثمرين الأجانب', width: 220, align: 'right', headerAlign: 'right' },
    { field: 'max_allowed', headerName: 'الملكية الحالية', width: 150, align: 'right', headerAlign: 'right' },
    { field: 'investor_limit', headerName: 'ملكية المستثمر الاستراتيجي الأجنبي', width: 220, align: 'right', headerAlign: 'right' },
    {
      field: 'previous_quarter_value',
      headerName: `الأرباح المبقاة للربع السابق (${previousQuarterHeaderLabel(quarterFilter)})`,
      width: 250,
      align: 'right',
      headerAlign: 'right',
      renderCell: (params) =>
        renderRetainedQuarterGridCell(
          params,
          quarterFilter,
          fetchEvidenceData,
          setEvidenceModalOpen,
          evidenceQuarterForPreviousColumn,
        ),
    },
    {
      field: 'current_quarter_value',
      headerName: `الأرباح المبقاة للربع الحالي (${currentQuarterHeaderLabel(quarterFilter)})`,
      width: 250,
      align: 'right',
      headerAlign: 'right',
      renderCell: (params) =>
        renderRetainedQuarterGridCell(
          params,
          quarterFilter,
          fetchEvidenceData,
          setEvidenceModalOpen,
          evidenceQuarterForCurrentColumn,
        ),
    },
    {
      field: 'flow',
      headerName: 'حجم الزيادة أو النقص في الأرباح المبقاة (التدفق)',
      width: 280,
      align: 'right',
      headerAlign: 'right',
      renderCell: renderSignedSarFlowGridCell,
    },
    {
      field: 'foreign_investor_flow',
      headerName: 'تدفق الأرباح المبقاة للمستثمر الأجنبي',
      width: 250,
      align: 'right',
      headerAlign: 'right',
      renderCell: renderSignedSarFlowGridCell,
    },
    {
      field: 'net_profit',
      headerName: 'صافي الربح',
      width: 150,
      align: 'right',
      headerAlign: 'right',
      renderCell: (params) => {
        const companySymbol = params.row.company_symbol;
        const companyNetProfit = netProfitData[companySymbol];
        if (companyNetProfit?.quarterly_net_profit) {
          const quarterKey = netProfitQuarterKeyFromFilter(quarterFilter);
          const npValue = companyNetProfit.quarterly_net_profit[quarterKey];
          if (npValue !== undefined && npValue !== null) {
            return npValue.toLocaleString('en-US');
          }
        }
        return 'لايوجد';
      },
    },
    {
      field: 'net_profit_foreign_investor',
      headerName: 'صافي الربح للمستثمر الأجنبي',
      width: 220,
      align: 'right',
      headerAlign: 'right',
      renderCell: renderSignedSarFlowGridCell,
    },
    {
      field: 'distributed_profits_foreign_investor',
      headerName: 'الأرباح الموزعة للمستثمر الأجنبي',
      width: 250,
      align: 'right',
      headerAlign: 'right',
      renderCell: renderSignedSarFlowGridCell,
    },
  ];
}
