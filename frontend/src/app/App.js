import React, { useState, useEffect, useMemo, useCallback } from "react";
import { DataGrid } from "@mui/x-data-grid";
import Box from "@mui/material/Box";
import TextField from "@mui/material/TextField";
import Paper from "@mui/material/Paper";
import RefreshIcon from "@mui/icons-material/Refresh";
import Typography from "@mui/material/Typography";
import Modal from "@mui/material/Modal";
import Fade from "@mui/material/Fade";
import IconButton from "@mui/material/IconButton";
import CircularProgress from "@mui/material/CircularProgress";
import Alert from "@mui/material/Alert";
import Tooltip from '@mui/material/Tooltip';
import FileDownloadIcon from '@mui/icons-material/FileDownload';
import DownloadIcon from '@mui/icons-material/Download';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';
import Dialog from '@mui/material/Dialog';
import DialogTitle from '@mui/material/DialogTitle';
import DialogContent from '@mui/material/DialogContent';
import DialogActions from '@mui/material/DialogActions';
import Button from '@mui/material/Button';
import Drawer from '@mui/material/Drawer';
import List from '@mui/material/List';
import ListItem from '@mui/material/ListItem';
import Collapse from '@mui/material/Collapse';
import MenuIcon from '@mui/icons-material/Menu';
import FolderOpenIcon from '@mui/icons-material/FolderOpen';
import ArrowBackIosNewIcon from '@mui/icons-material/ArrowBackIosNew';
import MenuItem from '@mui/material/MenuItem';
import Add from '@mui/icons-material/Add';
import {
  API_URL,
  apiUrl,
  postCsrfOptionalLog,
  postJsonWithCsrf,
  withCsrfHeaders,
} from '../services/apiClient';
import { startJobStatusPoll } from '../services/statusPolling';
import {
  EvidenceModal,
  JobProgressShell,
  buildCustomDateExportErrorMessage,
  buildDataGridColumns,
  combineDashboardRows,
  devLog,
  fetchBackendJsonList,
  mergeCorrectionIntoRows,
  parseFilenameFromContentDisposition,
  parseQuarterlyFlowCsvText,
} from '../dashboard';

function App() { // NOSONAR
  const [rows, setRows] = useState([]);
  const [search, setSearch] = useState("");
  const [quarterFilter, setQuarterFilter] = useState("Q1"); // Default to Q1 instead of "all"
  const [loading, setLoading] = useState(false);
  const [evidenceModalOpen, setEvidenceModalOpen] = useState(false);
  const [evidenceData, setEvidenceData] = useState(null);
  const [evidenceLoading, setEvidenceLoading] = useState(false);
  const [evidenceError, setEvidenceError] = useState(null);
  const [snapshots, setSnapshots] = useState([]);
  const [snapshotsLoading, setSnapshotsLoading] = useState(false);
  const [snapshotsError, setSnapshotsError] = useState(null);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [userExports, setUserExports] = useState([]);
  const [userExportsLoading, setUserExportsLoading] = useState(false);
  const [userExportsError, setUserExportsError] = useState(null);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [fileToDelete, setFileToDelete] = useState(null);
  const [netProfitData, setNetProfitData] = useState({});
  const [customExportDate, setCustomExportDate] = useState("");
  const [customFileName, setCustomFileName] = useState("");
  const [customExportExpanded, setCustomExportExpanded] = useState(false);
  // Background jobs state
  const [pdfJobStatus, setPdfJobStatus] = useState({ status: 'idle' });
  const [netJobStatus, setNetJobStatus] = useState({ status: 'idle' });
  const [pdfPollId, setPdfPollId] = useState(null);
  const [netPollId, setNetPollId] = useState(null);
  const [pdfProgressOpen, setPdfProgressOpen] = useState(false);
  const [netProgressOpen, setNetProgressOpen] = useState(false);
  // Unified update modal state
  const [updateModalOpen, setUpdateModalOpen] = useState(false);
  const [selectPdf, setSelectPdf] = useState(false);
  const [selectNet, setSelectNet] = useState(false);
  // Combined progress modal when running both (net profit is merged inside PDF downloader — no second browser job)
  const [bothProgressOpen, setBothProgressOpen] = useState(false);
  const [bothIsStopping, setBothIsStopping] = useState(false);

  const PIPELINE_BUSY_FALLBACK_AR = 'يوجد مهمة متصفح أخرى قيد التشغيل.';

  const alertPipelineStartFailure = (res, data, messagePrefix = '❌ لم يتم بدء العملية: ') => {
    if (res.status === 409) {
      alert(`❌ ${data.hint_ar || data.hint || data.message || PIPELINE_BUSY_FALLBACK_AR}`);
      return;
    }
    alert(messagePrefix + (data.message || ''));
  };

  const startPollPdf = (onComplete) => {
    startJobStatusPoll({
      isAlreadyPolling: () => pdfPollId,
      setPollIntervalId: setPdfPollId,
      path: 'api/pdfs/status',
      setStatus: setPdfJobStatus,
      isDone: (d) => d.status === 'completed' || d.status === 'idle' || d.status === 'blocked_by_waf',
      beforeReload: () => setPdfProgressOpen(false),
      reloadFns: [fetchData, fetchNetProfitData],
      onComplete,
      logLabel: 'pdf status poll',
      log: devLog,
    });
  };

  const startPollNet = (onComplete) => {
    startJobStatusPoll({
      isAlreadyPolling: () => netPollId,
      setPollIntervalId: setNetPollId,
      path: 'api/net_profit/status',
      setStatus: setNetJobStatus,
      isDone: (d) => d.status === 'completed' || d.status === 'idle',
      beforeReload: () => setNetProgressOpen(false),
      reloadFns: [fetchNetProfitData, fetchData],
      onComplete,
      logLabel: 'net profit status poll',
      log: devLog,
    });
  };

  const startPdfPipelineOnly = async () => {
    try {
      const { res, data } = await postJsonWithCsrf('api/run_pdfs_pipeline');
      if (res.status === 202) {
        setPdfJobStatus({ status: 'running' });
        setPdfProgressOpen(true);
        startPollPdf();
        return;
      }
      alertPipelineStartFailure(res, data);
    } catch (e) {
      alert(`❌ خطأ في الاتصال بالخادم: ${e.message}`);
    }
  };

  const startNetScrapeOnly = async () => {
    try {
      const { res, data } = await postJsonWithCsrf('api/run_net_profit_scrape');
      if (res.status === 202) {
        setNetJobStatus({ status: 'running' });
        setNetProgressOpen(true);
        startPollNet();
        return;
      }
      alertPipelineStartFailure(res, data);
    } catch (e) {
      alert(`❌ خطأ في الاتصال بالخادم: ${e.message}`);
    }
  };

  const startBothViaPdfPipeline = async () => {
    try {
      const { res: resPdf, data: dataPdf } = await postJsonWithCsrf('api/run_pdfs_pipeline');
      if (resPdf.status === 202) {
        setPdfJobStatus({ status: 'running' });
        setBothProgressOpen(true);
        startPollPdf(() => {
          setBothProgressOpen(false);
          setBothIsStopping(false);
        });
        return;
      }
      alertPipelineStartFailure(resPdf, dataPdf, '❌ لم يتم بدء عملية تحديث PDF: ');
    } catch (e) {
      alert(`❌ خطأ في الاتصال بالخادم: ${e.message}`);
    }
  };

  const handleUpdateModalConfirm = async () => {
    setUpdateModalOpen(false);
    if (selectPdf && !selectNet) {
      await startPdfPipelineOnly();
    } else if (!selectPdf && selectNet) {
      await startNetScrapeOnly();
    } else {
      try {
        await startBothViaPdfPipeline();
      } finally {
        setSelectPdf(false);
        setSelectNet(false);
      }
    }
  };

  useEffect(() => {
    return () => {
      if (pdfPollId) clearInterval(pdfPollId);
      if (netPollId) clearInterval(netPollId);
    };
  }, [pdfPollId, netPollId]);

  const fetchEvidenceData = useCallback(async (companySymbol, quarter) => {
    setEvidenceData(null);
    setEvidenceLoading(true);
    setEvidenceError(null);
    try {
      const response = await fetch(apiUrl(`api/extractions/${companySymbol}?quarter=${quarter}`));
      if (response.ok) {
        const data = await response.json();
        setEvidenceData(data);
        setEvidenceModalOpen(true);
      } else {
        setEvidenceError(`تعذر تحميل الأدلة (HTTP ${response.status})`);
      }
    } catch (error) {
      setEvidenceError(error?.message || 'خطأ في الاتصال');
    } finally {
      setEvidenceLoading(false);
    }
  }, []);

  // Function to fetch net profit data
  const fetchNetProfitData = async () => {
    try {
      const response = await fetch(apiUrl('api/net-profit'));
      if (response.ok) {
        const data = await response.json();
        setNetProfitData(data);
      } else {
        console.error('Failed to fetch net profit data');
      }
    } catch (error) {
      console.error('Error fetching net profit data:', error);
    }
  };

  const handleEvidenceClick = useCallback((row) => {
    if (row.current_quarter_value && row.current_quarter_value !== "لايوجد") {
      setEvidenceModalOpen(true);
      fetchEvidenceData(row.symbol, row.quarter);
    }
  }, [fetchEvidenceData]);

  // Function to handle reset (إعادة تعيين)
  const handleReset = async () => {
    setLoading(true);
    try {
      const { res: refreshRes, data } = await postJsonWithCsrf('api/refresh');
      if (refreshRes.ok && data.status === 'success') {
        // Optionally show a success message
        fetchData(); // Reload data after refresh
      } else {
        alert('حدث خطأ أثناء التحديث: ' + (data.message || ''));
        setLoading(false);
      }
    } catch (error) {
      alert('تعذر الاتصال بالخادم: ' + error.message);
      setLoading(false);
    }
  };


  // Function to handle Excel export
  const handleExcelExport = async () => {
    try {
      const response = await fetch(apiUrl(`api/export_excel?quarter=${quarterFilter}`));
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      // Get the filename from the response headers
      const contentDisposition = response.headers.get('content-disposition');
      const filename = parseFilenameFromContentDisposition(contentDisposition);
      
      // Create blob and download
      const blob = await response.blob();
      const url = globalThis.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      globalThis.URL.revokeObjectURL(url);
      a.remove();
      
      fetchBackendJsonList('api/user_exports', {
        setLoading: setUserExportsLoading,
        setData: setUserExports,
        setError: setUserExportsError,
        errorMessage: 'فشل في تحميل ملفات قام المستخدم بحفظها',
        devLog,
        logTag: 'user_exports',
      });
    } catch (error) {
      console.error('Error exporting to Excel:', error);
      alert('فشل في تصدير ملف Excel: ' + error.message);
    }
  };

  const fetchData = () => {
    setLoading(true);
    
    // Load foreign ownership data (JSON)
    const loadForeignOwnership = fetch("/foreign_ownership_data.json")
      .then((res) => res.json())
      .catch((error) => {
        console.error("Error loading foreign ownership data:", error);
        return [];
      });

    // Load quarterly flow data (CSV) from backend API
    const loadQuarterlyFlowData = fetch(apiUrl(`api/retained_earnings_flow.csv?t=${Date.now()}`))
      .then((res) => {
        if (!res.ok) {
          throw new Error(`HTTP error! status: ${res.status}`);
        }
        return res.text();
      })
      .then((csvText) => parseQuarterlyFlowCsvText(csvText))
      .catch((error) => {
        console.error("Error loading quarterly flow data:", error);
        return [];
      });

    // Combine both datasets
    Promise.all([loadForeignOwnership, loadQuarterlyFlowData])
      .then(([foreignOwnershipData, quarterlyFlowData]) => {
        devLog("Foreign ownership data count:", foreignOwnershipData.length);
        devLog("Quarterly flow data count:", quarterlyFlowData.length);
        const mergedData = combineDashboardRows(foreignOwnershipData, quarterlyFlowData, handleEvidenceClick);
        devLog("Final merged data sample:", mergedData.slice(0, 3));
        setRows(mergedData);
        setLoading(false);
      })
      .catch((error) => {
        console.error("Error combining data:", error);
        setLoading(false);
      });
  };

  // Fetch archived snapshots
  useEffect(() => {
    fetchBackendJsonList('api/ownership_snapshots', {
      initialLog: '🔄 Fetching archived snapshots...',
      setLoading: setSnapshotsLoading,
      setData: setSnapshots,
      setError: setSnapshotsError,
      errorMessage: 'فشل في تحميل ملفات الفترات السابقة',
      devLog,
      logTag: 'snapshots',
    });
  }, []);

  // Fetch user exports
  useEffect(() => {
    fetchBackendJsonList('api/user_exports', {
      initialLog: '🔄 Fetching user exports...',
      setLoading: setUserExportsLoading,
      setData: setUserExports,
      setError: setUserExportsError,
      errorMessage: 'فشل في تحميل ملفات قام المستخدم بحفظها',
      devLog,
      logTag: 'user_exports',
    });
  }, []);

  // Helper function to determine quarter from date
  const getQuarterFromDate = (dateString) => {
    if (!dateString) return '';
    
    try {
      const date = new Date(dateString);
      const month = date.getMonth() + 1; // getMonth() returns 0-11
      const year = date.getFullYear();
      
      if (month >= 1 && month <= 3) return `Q1 ${year}`;
      if (month >= 4 && month <= 6) return `Q2 ${year}`;
      if (month >= 7 && month <= 9) return `Q3 ${year}`;
      if (month >= 10 && month <= 12) return `Q4 ${year}`;
      
      return '';
    } catch (error) {
      devLog('getQuarterFromDate', error);
      return '';
    }
  };

  const columns = useMemo(
    () => buildDataGridColumns(quarterFilter, netProfitData, fetchEvidenceData, setEvidenceModalOpen),
    [quarterFilter, netProfitData, fetchEvidenceData],
  );

  useEffect(() => {
    fetchData();
    // Intentional: load once on mount; fetchData is stable for this screen
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    fetchNetProfitData();
  }, []);

  useEffect(() => {
    const applyCorrection = (updated) => {
      setRows((prevRows) => mergeCorrectionIntoRows(prevRows, updated));
    };
    globalThis.updateRowAfterCorrection = applyCorrection;
    return () => { globalThis.updateRowAfterCorrection = undefined; };
  }, []);

  // Filter rows based on search and quarter filter
  const filteredRows = useMemo(() => {
    let filtered = rows;
    
    // Filter by quarter - now all companies have rows for all quarters
    filtered = filtered.filter(row => row.quarter === quarterFilter);
    
    devLog(`Showing ${filtered.length} companies for ${quarterFilter}`);
    
    // Then apply search filter
    if (search) {
      const searchLower = search.toLowerCase();
      filtered = filtered.filter((row) =>
        (row.symbol?.toString().toLowerCase().includes(searchLower)) ||
        (row.company_name?.toLowerCase().includes(searchLower))
      );
    }
    
    return filtered;
  }, [rows, search, quarterFilter]);

  // Delete handler
  const handleDeleteExport = (file) => {
    setFileToDelete(file);
    setDeleteDialogOpen(true);
  };

  const confirmDeleteExport = async () => {
    if (!fileToDelete) return;
    try {
      await fetch(apiUrl(`api/user_exports/${fileToDelete.filename}`), {
        method: 'DELETE',
        headers: await withCsrfHeaders(),
      });
      setUserExports((prev) => prev.filter(f => f.filename !== fileToDelete.filename));
    } catch (e) {
      devLog('delete user export', e);
    }
    setDeleteDialogOpen(false);
    setFileToDelete(null);
  };

  const cancelDeleteExport = () => {
    setDeleteDialogOpen(false);
    setFileToDelete(null);
  };

  // Function to handle custom date export
  const handleCustomDateExport = async () => {
    try {
      setLoading(true);
      
      // Build the API URL with custom date and filename
      let exportUrl = apiUrl(`api/export_excel?quarter=${quarterFilter}&custom_date=${customExportDate}`);
      if (customFileName.trim()) {
        exportUrl += `&custom_filename=${encodeURIComponent(customFileName.trim())}`;
      }

      const response = await fetch(exportUrl);
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      // Get the filename from the response headers
      const contentDisposition = response.headers.get('content-disposition');
      const filename = parseFilenameFromContentDisposition(contentDisposition);
      const exportedDateLabel = customExportDate;
      const quarterLabel = getQuarterFromDate(customExportDate);
      
      // Create blob and download
      const blob = await response.blob();
      const url = globalThis.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      globalThis.URL.revokeObjectURL(url);
      a.remove();
      
      fetchBackendJsonList('api/user_exports', {
        setLoading: setUserExportsLoading,
        setData: setUserExports,
        setError: setUserExportsError,
        errorMessage: 'فشل في تحميل ملفات قام المستخدم بحفظها',
        devLog,
        logTag: 'user_exports',
      });

      const successMessage = `✅ تم التصدير بنجاح!\n\n📁 اسم الملف: ${filename}\n📅 التاريخ: ${exportedDateLabel}\n🎯 الربع: ${quarterLabel}\n\nتم حفظ الملف في مجلد التنزيلات`;
      alert(successMessage);

      setCustomExportDate("");
      setCustomFileName("");
      
    } catch (error) {
      console.error('Error exporting to Excel:', error);
      alert(buildCustomDateExportErrorMessage(error));
    } finally {
      setLoading(false);
    }
  };

  return (
    <Box dir="rtl" sx={{ minHeight: "100vh", bgcolor: "#f4f6fa", fontFamily: "'Tajawal', 'Cairo', 'Noto Sans Arabic', sans-serif", display: 'flex', flexDirection: 'column' }}>
      {/* Sidebar menu button at the top right, no background */}
      { !drawerOpen && (
        <Box sx={{ width: '100%', display: 'flex', justifyContent: 'flex-end', alignItems: 'center', pt: 1, pr: 0.2 }}>
          <IconButton
            onClick={() => setDrawerOpen(true)}
            sx={{
              bgcolor: 'transparent',
              boxShadow: 'none',
              borderRadius: 2,
              p: 0,
              '&:hover': { bgcolor: '#e3ecfa' },
            }}
            size="large"
            aria-label="فتح القائمة الجانبية"
          >
            <MenuIcon sx={{ color: '#1e6641', fontSize: 32 }} />
          </IconButton>
        </Box>
      )}

      {/* Main app container */}
      {/* Header with gradient */}
      <Box sx={{
        width: '100%',
        py: { xs: 3, md: 4 },
        px: 0,
        mb: 0,
        background: 'linear-gradient(90deg, #0d3b23 0%, #1e6641 100%)',
        boxShadow: '0 6px 24px 0 rgba(20, 83, 45, 0.18)',
        borderBottom: '4px solid #14532d',
        color: 'white',
        display: 'flex',
        alignItems: 'center',
        flexDirection: 'row', // logo on the right for RTL
        justifyContent: 'flex-start',
      }}>
        <img
          src="/sama-header.png"
          alt="Saudi Central Bank Logo"
          style={{
            height: '96px',
            width: 'auto',
            marginLeft: 0,
            marginRight: 0,
            display: 'block',
            flexShrink: 0,
            filter: 'drop-shadow(0 2px 8px rgba(0,0,0,0.08))',
            objectFit: 'contain',
          }}
        />
      </Box>
      {/* Title and subtitle below header */}
      <Box sx={{ textAlign: 'right', mt: { xs: 3, md: 5 }, mb: { xs: 3, md: 5 }, pr: { xs: 2, md: 8 } }}>
        <Typography variant="h3" fontWeight="bold" sx={{ mb: 1, fontSize: { xs: 26, md: 36 }, color: '#1e6641', display: 'inline-block' }}>
          جدول ملكية الأجانب والأرباح المبقاة
        </Typography>
        <Box sx={{ height: 4, width: 120, bgcolor: '#1e6641', mr: 0, ml: 'unset', borderRadius: 2, mb: 2 }} />
        <Typography variant="subtitle1" sx={{ color: '#37474f', fontSize: { xs: 15, md: 20 } }}>
          بيانات محدثة من تداول السعودية - ملكية الأجانب والأرباح المبقاة في الشركات المدرجة
        </Typography>
      </Box>

              {/* Main card */}
        <Paper elevation={4} sx={{
          maxWidth: '85vw',
          mx: 'auto',
          p: { xs: 2, md: 3 },
          borderRadius: 4,
          boxShadow: '0 6px 32px 0 rgba(30,102,65,0.10)',
          mb: 4,
          width: '100%',
        }}>
        {/* Search/filter area styled like the provided image */}
                  <Box sx={{
            display: 'flex',
            flexDirection: { xs: 'column', md: 'row' },
            alignItems: { xs: 'stretch', md: 'center' },
            justifyContent: 'space-between',
            bgcolor: '#f3f4f6',
            p: 2,
            mb: 2,
            borderRadius: 2,
            gap: { xs: 2, md: 0 },
          }}>
          {/* Search box in the right corner */}
          <Box sx={{ minWidth: 320, maxWidth: 400, width: '100%', textAlign: 'right' }}>
            <Typography sx={{ mb: 1, fontWeight: 'bold', color: '#37474f', fontSize: 18 }}>
              رمز / شركة بحث
            </Typography>
            <TextField
              fullWidth
              placeholder="رمز / شركة بحث"
              variant="outlined"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              sx={{ bgcolor: 'white' }}
              slotProps={{ htmlInput: { style: { textAlign: 'right' } } }}
            />
          </Box>
          {/* Reset button in the left corner */}
          <Box sx={{ 
            display: 'flex', 
            alignItems: 'center', 
            justifyContent: { xs: 'flex-start', md: 'flex-end' }, 
            width: { xs: '100%', md: 'auto' }, 
            height: '100%', 
            gap: 4 
          }}>
            {/* Secondary Reset Button */}
            <Button
              variant="text"
              onClick={handleReset}
              sx={{
                minWidth: 150,
                height: 48,
                px: 4,
                py: 2,
                borderRadius: 3,
                bgcolor: '#f8f9fa',
                color: '#6c757d',
                border: '1px solid #e9ecef',
                fontWeight: 500,
                fontSize: 14,
                textTransform: 'none',
                display: 'flex',
                alignItems: 'center',
                gap: 1.5,
                boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
                transition: 'all 0.2s ease-in-out',
                '&:hover': {
                  bgcolor: '#e9ecef',
                  borderColor: '#dee2e6',
                  transform: 'translateY(-1px)',
                  boxShadow: '0 2px 6px rgba(0,0,0,0.15)',
                },
              }}
            >
              <RefreshIcon sx={{ fontSize: 18, color: '#6c757d' }} />
              إعادة تعيين
            </Button>
            
            {/* Primary Download Button */}
            <Tooltip title="تصدير الجدول إلى Excel" arrow placement="top">
              <Button
                variant="contained"
                onClick={handleExcelExport}
                sx={{
                  minWidth: 150,
                  height: 48,
                  px: 4,
                  py: 2,
                  borderRadius: 3,
                  bgcolor: '#1e6641',
                  color: 'white',
                  fontWeight: 600,
                  fontSize: 14,
                  textTransform: 'none',
                  display: 'flex',
                  alignItems: 'center',
                  gap: 1.5,
                  boxShadow: '0 4px 12px rgba(30, 102, 65, 0.3)',
                  transition: 'all 0.2s ease-in-out',
                  '&:hover': {
                    bgcolor: '#14532d',
                    transform: 'translateY(-1px)',
                    boxShadow: '0 6px 16px rgba(30, 102, 65, 0.4)',
                  },
                }}
              >
                <FileDownloadIcon sx={{ fontSize: 18, color: 'white' }} />
                تصدير الجدول
              </Button>
            </Tooltip>

            {/* Unified Update Button */}
            <Tooltip title="تحديث البيانات" arrow placement="top">
              <Button
                variant="outlined"
                onClick={() => setUpdateModalOpen(true)}
                sx={{
                  minWidth: 150,
                  height: 48,
                  px: 3,
                  py: 2,
                  borderRadius: 3,
                  color: '#1e6641',
                  borderColor: '#1e6641',
                  fontWeight: 600,
                  fontSize: 14,
                  textTransform: 'none',
                }}
              >
                تحديث
              </Button>
            </Tooltip>

            <JobProgressShell open={pdfProgressOpen} width={420} title="تحديث PDF قيد التنفيذ" titleColor="#1e6641" progressColor="success">
              <Typography sx={{ fontSize: 13, color: '#1e6641', mt: 1 }}>
                الحالة: {pdfJobStatus?.status || 'جاري التنفيذ'} — المُنجز: {pdfJobStatus?.processed || 0}
                {pdfJobStatus?.current_symbol ? ` — الحالي: ${pdfJobStatus.current_symbol}` : ''}
              </Typography>
              <Box sx={{ display: 'flex', justifyContent: 'flex-end', mt: 2 }}>
                <Button variant="outlined" onClick={async () => { await postCsrfOptionalLog('api/pdfs/stop', devLog, 'pdfs stop'); }} sx={{ color: '#b71c1c', borderColor: '#b71c1c' }}>إيقاف</Button>
              </Box>
            </JobProgressShell>

            {/* Update Selection Modal */}
            <Dialog open={updateModalOpen} onClose={() => setUpdateModalOpen(false)}>
              <DialogTitle sx={{ fontWeight: 700, color: '#1e6641' }}>اختر نوع التحديث</DialogTitle>
              <DialogContent sx={{ minWidth: 360 }}>
                <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5 }}>
                  <label style={{ display: 'flex', alignItems: 'center', gap: 8, cursor: 'pointer' }}>
                    <input type="checkbox" checked={selectPdf} onChange={(e) => setSelectPdf(e.target.checked)} />
                    {' '}
                    تحديث الأرباح المبقاة (تنزيل واستخراج)
                  </label>
                  <label style={{ display: 'flex', alignItems: 'center', gap: 8, cursor: 'pointer' }}>
                    <input type="checkbox" checked={selectNet} onChange={(e) => setSelectNet(e.target.checked)} />
                    {' '}
                    تحديث صافي الربح
                  </label>
                  <Typography variant="caption" sx={{ color: '#666' }}>
                    يمكن اختيار واحد أو كلاهما. إذا اخترت الاثنين: صافي الربح يُحدَّث أثناء تنزيل PDF لكل شركة (زيارة واحدة، بدون تشغيل متصفح ثانٍ).
                  </Typography>
                </Box>
              </DialogContent>
              <DialogActions>
                <Button onClick={() => setUpdateModalOpen(false)} sx={{ color: '#666' }}>إلغاء</Button>
                <Button
                  variant="contained"
                  disabled={!selectPdf && !selectNet}
                  onClick={handleUpdateModalConfirm}
                  sx={{ bgcolor: '#1e6641', '&:hover': { bgcolor: '#14532d' } }}
                >
                  بدء التحديث
                </Button>
              </DialogActions>
            </Dialog>

            <JobProgressShell open={bothProgressOpen} width={520} title="التحديث قيد التنفيذ" titleColor="#1e6641" progressColor="success" titleMb={2}>
              {((pdfJobStatus?.status === 'finalizing') || (netJobStatus?.status === 'finalizing')) && (
                <Typography sx={{ mt: 1, fontSize: 13, color: '#555' }}>
                  جارٍ الإنهاء: إيقاف العمليات، حساب النتائج، وتحديث اللوحة. يرجى الانتظار حتى يكتمل التحديث.
                </Typography>
              )}
              <Typography sx={{ fontSize: 13, color: '#1e6641', mt: 1 }}>
                الأرباح المبقاة: {pdfJobStatus?.status || 'جاري التنفيذ'}{pdfJobStatus?.current_symbol ? ` — ${pdfJobStatus.current_symbol}` : ''}
              </Typography>
              <Typography sx={{ fontSize: 13, color: '#ff9800' }}>
                صافي الربح: يُحدَّث مع كل شركة ضمن خط التنزيل أعلاه (لا مهمة منفصلة).
                {pdfJobStatus?.current_symbol ? ` — آخر رمز: ${pdfJobStatus.current_symbol}` : ''}
              </Typography>
              <Box sx={{ display: 'flex', justifyContent: 'flex-end', mt: 2 }}>
                <Button
                  variant="outlined"
                  onClick={async () => {
                    setBothIsStopping(true);
                    await postCsrfOptionalLog('api/pdfs/stop', devLog, 'both stop pdfs');
                    await postCsrfOptionalLog('api/net_profit/stop', devLog, 'both stop net');
                  }}
                  disabled={bothIsStopping}
                  sx={{ color: bothIsStopping ? '#999' : '#b71c1c', borderColor: bothIsStopping ? '#ccc' : '#b71c1c' }}
                >
                  {bothIsStopping ? 'جاري الإنهاء...' : 'إيقاف'}
                </Button>
              </Box>
            </JobProgressShell>
            <JobProgressShell open={netProgressOpen} width={420} title="تحديث صافي الربح قيد التنفيذ" titleColor="#ff9800" progressColor="warning">
              <Typography sx={{ fontSize: 13, color: '#ff9800', mt: 1 }}>
                الحالة: {netJobStatus?.status || 'جاري التنفيذ'} — المُنجز: {netJobStatus?.processed || 0}
                {netJobStatus?.current_symbol ? ` — الحالي: ${netJobStatus.current_symbol}` : ''}
              </Typography>
              <Box sx={{ display: 'flex', justifyContent: 'flex-end', mt: 2 }}>
                <Button variant="outlined" onClick={async () => { await postCsrfOptionalLog('api/net_profit/stop', devLog, 'net profit stop'); }} sx={{ color: '#b71c1c', borderColor: '#b71c1c' }}>إيقاف</Button>
              </Box>
            </JobProgressShell>
          </Box>
        </Box>
        <Box sx={{ display: "flex", gap: 2, alignItems: "center", mb: 2 }}>
            {/* Quarter Filter Dropdown */}
            <TextField
              select
              label="تصفية حسب الربع"
              variant="outlined"
              size="small"
              value={quarterFilter}
              onChange={(e) => setQuarterFilter(e.target.value)}
              sx={{
                minWidth: 200,
                "& .MuiOutlinedInput-root": {
                  borderRadius: 2,
                  "& fieldset": { borderColor: "#e0e0e0" },
                  "&:hover fieldset": { borderColor: "#1e6641" },
                  "&:focus fieldset": { borderColor: "#1e6641" },
                },
                "& .MuiInputLabel-root": { color: "#666" },
                "& .MuiInputLabel-root.Mui-focused": { color: "#1e6641" },
              }}
            >
              <MenuItem value="Q1">الربع الأول (Q1)</MenuItem>
              <MenuItem value="Q2">الربع الثاني (Q2)</MenuItem>
              <MenuItem value="Q3">الربع الثالث (Q3)</MenuItem>
            </TextField>
            
            
          </Box>
        {/* DataGrid */}
        <Box sx={{ 
          width: '100%', 
          height: 560,
          overflow: 'auto',
          border: '1px solid #e0e0e0',
          borderRadius: 1,
          maxWidth: '100%',
          p: 0
        }}>
          <DataGrid
            rows={filteredRows}
            columns={columns}
            pageSize={20}
            rowsPerPageOptions={[20, 50, 100]}
            disableSelectionOnClick
            componentsProps={{
              toolbar: {
                showQuickFilter: true,
                quickFilterProps: { debounceMs: 500 },
              },
            }}
            // Optimize column rendering
            columnBuffer={1}
            columnThreshold={1}
            sx={{
              bgcolor: "white",
              fontFamily: "'Tajawal', 'Cairo', 'Noto Sans Arabic', sans-serif",
              direction: "rtl",
              borderRadius: 4,
              fontSize: 18,
              boxShadow: '0 2px 16px 0 rgba(30,102,65,0.08)',
              border: 'none',
              "& .MuiDataGrid-columnHeaders": {
                bgcolor: "#e3ecfa",
                fontWeight: "bold",
                fontSize: 18,
                position: 'sticky',
                top: 0,
                zIndex: 1,
                direction: 'rtl',
                textAlign: 'right',
                boxShadow: '0 2px 8px 0 rgba(30,102,65,0.10)',
                borderTopLeftRadius: 16,
                borderTopRightRadius: 16,
              },
              "& .MuiDataGrid-columnHeader, & .MuiDataGrid-columnHeaderTitle": {
                direction: "rtl",
                textAlign: "right",
                justifyContent: "flex-end",
                paddingRight: "12px !important",
                paddingLeft: "0 !important",
                display: 'flex',
              },
              "& .MuiDataGrid-columnHeaderTitleContainer": {
                flexDirection: "row-reverse",
                direction: 'rtl',
                display: 'flex',
                justifyContent: 'flex-end',
              },
              "& .MuiDataGrid-columnHeaderTitleContainerContent": {
                textAlign: "right",
                justifyContent: "flex-end",
                direction: 'rtl',
                display: 'flex',
              },
              "& .MuiDataGrid-row": {
                minHeight: 44,
                maxHeight: 44,
                transition: 'background 0.2s, box-shadow 0.2s',
                borderRadius: 2,
              },
              "& .MuiDataGrid-row:nth-of-type(even)": { bgcolor: "#f7fafc" },
              "& .MuiDataGrid-row:hover": {
                bgcolor: "#e3f2fd",
                boxShadow: '0 2px 8px 0 rgba(30,102,65,0.08)',
                cursor: 'pointer',
              },
              "& .MuiDataGrid-footerContainer": { 
                bgcolor: '#f4f6fa', 
                fontWeight: 'bold', 
                borderBottomLeftRadius: 16, 
                borderBottomRightRadius: 16 
              },
              "& .MuiDataGrid-virtualScroller": { minHeight: 300 },
              "& .MuiDataGrid-cell": {
                borderBottom: '1px solid #e0e0e0',
                fontWeight: 500,
                fontSize: 17,
                letterSpacing: '0.01em',
                direction: 'rtl',
                textAlign: 'right',
              },
              "& .MuiDataGrid-cell:focus, & .MuiDataGrid-columnHeader:focus": {
                outline: 'none',
              },
              "& .MuiDataGrid-root": {
                borderRadius: 4,
              },
            }}
          />
        </Box>
      </Paper>
      
      {/* Evidence Modal */}
      <EvidenceModal
        open={evidenceModalOpen}
        onClose={() => setEvidenceModalOpen(false)}
        evidenceData={evidenceData}
        loading={evidenceLoading}
        error={evidenceError}
        onDataUpdate={fetchData}
      />

      {/* Footer */}
      <Box sx={{ textAlign: 'center', color: '#888', py: 2, fontSize: 16, mt: 'auto' }}>
        © {new Date().getFullYear()} مركز الابتكار
      </Box>
      {/* Delete confirmation dialog */}
      <Dialog open={deleteDialogOpen} onClose={cancelDeleteExport}>
        <DialogTitle sx={{ fontWeight: 700, color: '#1e6641' }}>تأكيد الحذف</DialogTitle>
        <DialogContent>
          <Typography>هل أنت متأكد أنك تريد حذف هذا الملف؟ لا يمكن التراجع عن هذه العملية.</Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={cancelDeleteExport} sx={{ color: '#37474f' }}>إلغاء</Button>
          <Button onClick={confirmDeleteExport} sx={{ color: '#b71c1c', fontWeight: 700 }}>حذف</Button>
        </DialogActions>
      </Dialog>

      {/* Sidebar Drawer */}
      <Drawer
        anchor="left"
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
        slotProps={{ paper: { sx: { width: 340, bgcolor: '#f8f9fa', borderTopRightRadius: 16, borderBottomRightRadius: 16 } } }}
      >
        <Box
          sx={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            px: 3,
            py: 2.5,
            bgcolor: '#fff',
            borderBottom: '1.5px solid #e0e0e0',
            boxShadow: '0 2px 8px 0 rgba(30,102,65,0.04)',
            borderTopRightRadius: 16,
            borderTopLeftRadius: 16,
            minHeight: 72,
          }}
        >
          <Box sx={{ display: 'flex', alignItems: 'center', width: '100%', justifyContent: 'space-between' }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.2 }}>
              <FolderOpenIcon sx={{ color: '#1e6641', fontSize: 24, mb: '2px' }} />
              <Typography variant="h5" sx={{ fontWeight: 800, color: '#1e6641', fontSize: 20, lineHeight: 1.2 }}>
                الملفات المحفوظة
              </Typography>
            </Box>
            <IconButton
              aria-label="إغلاق القائمة الجانبية"
              onClick={() => setDrawerOpen(false)}
              sx={{
                color: '#1e6641',
                bgcolor: '#f4f6fa',
                borderRadius: '50%',
                boxShadow: '0 2px 8px 0 rgba(30,102,65,0.10)',
                p: 0.5,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                transition: 'background 0.2s, color 0.2s, box-shadow 0.2s',
                '&:hover': { bgcolor: '#e3ecfa', color: '#14532d', boxShadow: '0 4px 16px 0 rgba(30,102,65,0.18)' },
              }}
            >
              <ArrowBackIosNewIcon sx={{ fontSize: 18, transform: 'scaleX(-1)' }} />
            </IconButton>
          </Box>
        </Box>
        {/* Soft divider and extra space below header */}
        <Box sx={{ height: 18 }} />
        <Box sx={{ width: '100%', height: 2, bgcolor: '#f4f6fa', mb: 2, borderRadius: 2 }} />
        {/* User-Saved Exports Section */}
        <Box sx={{ mt: 2, pb: 0, px: 0 }}>
          <Box sx={{
            display: 'flex',
            alignItems: 'center',
            bgcolor: '#e9f5ee',
            borderRadius: 4,
            px: 2,
            py: 1.2,
            width: '100%',
            boxSizing: 'border-box',
            mb: 1.5,
            gap: 1.5,
          }}>
            <Box sx={{ width: 3, height: 24, bgcolor: '#1e6641', borderRadius: 6, mr: 0 }} />
            <Typography
              variant="subtitle1"
              sx={{
                fontWeight: 700,
                color: '#1e6641',
                fontSize: 18,
                letterSpacing: 0.1,
                minWidth: 0,
                pr: 1,
              }}
            >
              ملفاتك المصدّرة
            </Typography>
          </Box>
        </Box>
        <List>
          {userExportsLoading && (
            <ListItem sx={{ justifyContent: 'center' }}><CircularProgress size={22} sx={{ color: '#1e6641' }} /></ListItem>
          )}
          {!userExportsLoading && userExportsError && (
            <ListItem><Alert severity="error">{userExportsError}</Alert></ListItem>
          )}
          {!userExportsLoading && !userExportsError && userExports.length === 0 && (
            <ListItem sx={{ justifyContent: 'center', alignItems: 'center', minHeight: 80, width: '100%' }}>
              <Typography sx={{ color: '#b0b7be', fontSize: 17, textAlign: 'center', width: '100%' }}>
                لا توجد ملفات محفوظة بعد
              </Typography>
            </ListItem>
          )}
          {!userExportsLoading && !userExportsError && userExports.length > 0 && userExports.map((file) => (
              <ListItem
                key={file.download_url || file.filename || file.export_date}
                tabIndex={0}
                sx={{
                  pl: 3, pr: 3, py: 2.2,
                  mb: 1.5,
                  bgcolor: '#fff',
                  borderRadius: 2.5,
                  boxShadow: '0 1px 6px 0 rgba(30,102,65,0.06)',
                  display: 'flex',
                  alignItems: 'center',
                  '&:hover .export-delete-btn': { opacity: 1 },
                  minHeight: 56,
                  border: 'none',
                  transition: 'box-shadow 0.2s, transform 0.2s',
                  '&:hover': {
                    boxShadow: '0 4px 16px 0 rgba(30,102,65,0.10)',
                    transform: 'translateY(-2px) scale(1.01)',
                  },
                  outline: 'none',
                  '&:focus': {
                    boxShadow: '0 0 0 2px #1e664144',
                  },
                }}
              >
                <Box sx={{ flexGrow: 1 }}>
                  <Typography sx={{ fontWeight: 500, color: '#1e6641', fontSize: 15 }}>
                    {(() => {
                      // file.export_date is 'YYYY-MM-DD HH:mm:ss'
                      const datePart = file.export_date.split(' ')[0];
                      const [year, month, day] = datePart.split('-');
                      return `dashboard-${day}-${month}-${year}`;
                    })()}
                  </Typography>
                </Box>
                <Tooltip title="تحميل" arrow>
                  <IconButton
                    aria-label="تحميل"
                    href={`${API_URL}${file.download_url}`}
                    target="_blank"
                    rel="noopener noreferrer"
                    sx={{
                      color: '#1e6641',
                      bgcolor: 'transparent',
                      borderRadius: '50%',
                      p: 0.7,
                      mx: 0.5,
                      transition: 'color 0.2s, background 0.2s',
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                      fontSize: 22,
                      height: 36,
                      width: 36,
                      minWidth: 36,
                    }}
                  >
                    <DownloadIcon sx={{ fontSize: 22 }} />
                  </IconButton>
                </Tooltip>
                <Tooltip title="حذف" arrow>
                  <IconButton
                    aria-label="حذف"
                    className="export-delete-btn"
                    onClick={() => handleDeleteExport(file)}
                    sx={{
                      ml: 0.5,
                      opacity: 0,
                      color: '#7b7b7b',
                      bgcolor: 'transparent',
                      borderRadius: '50%',
                      p: 0.7,
                      transition: 'opacity 0.2s, color 0.2s, background 0.2s',
                      boxShadow: 'none',
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                      fontSize: 22,
                      height: 36,
                      width: 36,
                      minWidth: 36,
                      '&:hover': { color: '#444', bgcolor: 'rgba(120,120,120,0.07)' },
                    }}
                    size="small"
                  >
                    <DeleteOutlineIcon sx={{ fontSize: 22 }} />
                  </IconButton>
                </Tooltip>
              </ListItem>
            ))}
        </List>
        {/* Divider between sections */}
        <Box sx={{ mt: 2, pb: 0, px: 0 }}>
          <Box sx={{
            display: 'flex',
            alignItems: 'center',
            bgcolor: '#e9f5ee',
            borderRadius: 4,
            px: 2,
            py: 1.2,
            width: '100%',
            boxSizing: 'border-box',
            mb: 1.5,
            gap: 1.5,
          }}>
            <Box sx={{ width: 3, height: 24, bgcolor: '#1e6641', borderRadius: 6, mr: 0 }} />
            <Typography
              variant="subtitle1"
              sx={{
                fontWeight: 700,
                color: '#1e6641',
                fontSize: 18,
                letterSpacing: 0.1,
                minWidth: 0,
                pr: 1,
              }}
            >
              أرشيف الفترات الربعية
            </Typography>
          </Box>
        </Box>
        
        {/* Custom Export Section - Right next to Quarterly Archives */}
        <Box sx={{ px: 2, mb: 2 }}>
          <Tooltip 
            title="تصدير البيانات المالية لتاريخ محدد أو نطاق زمني معين مع إمكانية تخصيص البيانات المطلوبة"
            arrow
            placement="top"
          >
            <Box sx={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              bgcolor: '#f8f9fa',
              borderRadius: 3,
              px: 2,
              py: 1.5,
              border: '1px solid #e9ecef',
              cursor: 'pointer',
              transition: 'all 0.3s ease-in-out',
              '&:hover': {
                bgcolor: '#e9ecef',
                borderColor: '#1e6641',
                transform: 'translateY(-1px)',
                boxShadow: '0 4px 12px rgba(30, 102, 65, 0.15)',
              },
              '&:active': {
                transform: 'translateY(0px)',
              }
            }}
            onClick={() => setCustomExportExpanded(!customExportExpanded)}
            >
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
              <Box sx={{ 
                width: 20, 
                height: 20, 
                bgcolor: '#1e6641', 
                borderRadius: '50%',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                color: 'white',
                fontSize: 12,
                fontWeight: 'bold',
                transition: 'all 0.3s ease-in-out',
                animation: customExportExpanded ? 'pulse 2s infinite' : 'none',
                '@keyframes pulse': {
                  '0%': { transform: 'scale(1)' },
                  '50%': { transform: 'scale(1.1)' },
                  '100%': { transform: 'scale(1)' },
                }
              }}>
              </Box>
              <Typography
                variant="subtitle2"
                sx={{
                  fontWeight: 600,
                  color: '#495057',
                  fontSize: 14,
                  transition: 'color 0.3s ease-in-out',
                }}
              >
                تصدير مخصص
              </Typography>
              <Typography
                variant="caption"
                sx={{
                  color: '#6c757d',
                  fontSize: 11,
                  opacity: 0.8,
                  display: 'block',
                  lineHeight: 1.2
                }}
              >
                تصدير البيانات لتاريخ محدد
              </Typography>
            </Box>
            <IconButton
              size="small"
              sx={{
                color: '#1e6641',
                p: 0.5,
                transition: 'all 0.3s ease-in-out',
                transform: customExportExpanded ? 'rotate(45deg)' : 'rotate(0deg)',
                '&:hover': { 
                  color: '#14532d',
                  bgcolor: 'rgba(30, 102, 65, 0.1)',
                  transform: customExportExpanded ? 'rotate(45deg) scale(1.1)' : 'scale(1.1)',
                },
              }}
            >
              <Add />
            </IconButton>
          </Box>
          </Tooltip>
        </Box>
        
        {/* Collapsible Custom Export Controls */}
        <Modal 
          open={customExportExpanded} 
          onClose={() => setCustomExportExpanded(false)}
          closeAfterTransition
        >
          <Fade in={customExportExpanded}>
            <Box sx={{
              position: 'absolute',
              top: '50%',
              left: '50%',
              transform: 'translate(-50%, -50%)',
              width: { xs: '90vw', sm: 480 },
              bgcolor: 'background.paper',
              borderRadius: 4,
              boxShadow: '0 20px 60px rgba(0,0,0,0.15)',
              p: 0,
              outline: 'none',
              direction: 'rtl'
            }}>
              {/* Header */}
              <Box sx={{
                bgcolor: 'linear-gradient(135deg, #1e6641 0%, #2d7a4a 100%)',
                color: 'white',
                px: 3,
                py: 2.5,
                borderRadius: '16px 16px 0 0',
                position: 'relative'
              }}>
                <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                    <Box sx={{
                      width: 40,
                      height: 40,
                      bgcolor: 'rgba(255,255,255,0.2)',
                      borderRadius: '50%',
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                      fontSize: 18,
                      fontWeight: 'bold'
                    }}>
                      E
                    </Box>
                    <Box>
                      <Typography variant="h6" sx={{ fontWeight: 'bold', fontSize: 16, mb: 0.5 }}>
                        التصدير المخصص
                      </Typography>
                      <Typography variant="body2" sx={{ opacity: 0.9, fontSize: 12 }}>
                        تصدير البيانات لتاريخ معين
                      </Typography>
                    </Box>
                  </Box>
                  <IconButton
                    onClick={() => setCustomExportExpanded(false)}
                    sx={{
                      color: 'white',
                      bgcolor: 'rgba(255,255,255,0.1)',
                      '&:hover': { bgcolor: 'rgba(255,255,255,0.2)' }
                    }}
                    size="small"
                  >
                    ✕
                  </IconButton>
                </Box>
              </Box>

              {/* Content */}
              <Box sx={{ p: 3 }}>
                {/* Date Selection */}
                <Box sx={{ mb: 3 }}>
                  <Typography variant="subtitle2" sx={{ 
                    fontWeight: 600, 
                    color: '#1e6641', 
                    mb: 1.5, 
                    fontSize: 14,
                    display: 'flex',
                    alignItems: 'center',
                    gap: 1
                  }}>
                    تاريخ التصدير
                  </Typography>
                  
                  <TextField
                    type="date"
                    fullWidth
                    value={customExportDate}
                    onChange={(e) => setCustomExportDate(e.target.value)}
                    sx={{
                      "& .MuiOutlinedInput-root": {
                        borderRadius: 3,
                        fontSize: 14,
                        "& fieldset": { 
                          borderColor: "#e0e0e0",
                          borderWidth: 2
                        },
                        "&:hover fieldset": { 
                          borderColor: "#1e6641",
                          borderWidth: 2
                        },
                        "&:focus fieldset": { 
                          borderColor: "#1e6641",
                          borderWidth: 2
                        },
                      },
                      "& .MuiInputLabel-root": { 
                        color: "#666", 
                        fontSize: 13,
                        fontWeight: 500
                      },
                    }}
                    size="medium"
                  />
                  
                  {/* Quarter Preview */}
                  {customExportDate && (
                    <Box sx={{ 
                      mt: 2,
                      p: 3,
                      bgcolor: 'linear-gradient(135deg, #f0f9ff 0%, #e0f2fe 100%)',
                      borderRadius: 3,
                      border: '2px solid #0ea5e9',
                      animation: 'fadeInUp 0.4s ease-out'
                    }}>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mb: 1 }}>
                        <Box sx={{
                          width: 32,
                          height: 32,
                          bgcolor: '#0ea5e9',
                          borderRadius: '50%',
                          display: 'flex',
                          alignItems: 'center',
                          justifyContent: 'center',
                          color: 'white',
                          fontSize: 14,
                          fontWeight: 'bold'
                        }}>
                          Q
                        </Box>
                        <Typography variant="subtitle2" sx={{ 
                          color: '#0369a1', 
                          fontWeight: 'bold',
                          fontSize: 14
                        }}>
                          الربع المحتوي على التاريخ
                        </Typography>
                      </Box>
                      <Typography variant="h6" sx={{ 
                        color: '#0369a1', 
                        fontWeight: 'bold',
                        fontSize: 18,
                        textAlign: 'center'
                      }}>
                        {getQuarterFromDate(customExportDate)}
                      </Typography>
                    </Box>
                  )}
                </Box>

                {/* File Name */}
                <Box sx={{ mb: 3 }}>
                  <Typography variant="subtitle2" sx={{ 
                   fontWeight: 600, 
                    color: '#1e6641', 
                    mb: 1.5, 
                    fontSize: 14,
                    display: 'flex',
                    alignItems: 'center',
                    gap: 1
                  }}>
                    اسم الملف المخصص (اختياري)
                  </Typography>
                  
                  <TextField
                    fullWidth
                    placeholder="مثال: تقرير_ديسمبر_2024"
                    value={customFileName}
                    onChange={(e) => setCustomFileName(e.target.value)}
                    sx={{
                      "& .MuiOutlinedInput-root": {
                        borderRadius: 3,
                        fontSize: 14,
                        "& fieldset": { 
                          borderColor: "#e0e0e0",
                          borderWidth: 2
                        },
                        "&:hover fieldset": { 
                          borderColor: "#1e6641",
                          borderWidth: 2
                        },
                        "&:focus fieldset": { 
                          borderColor: "#1e6641",
                          borderWidth: 2
                        },
                      },
                      "& .MuiInputLabel-root": { 
                        color: "#666", 
                        fontSize: 13,
                        fontWeight: 500
                      },
                    }}
                    size="medium"
                  />
                  <Typography variant="caption" sx={{ 
                    color: '#666', 
                    fontSize: 11, 
                    mt: 0.5, 
                    display: 'block',
                    fontStyle: 'italic'
                  }}>
                    سيتم إضافة التاريخ والوقت تلقائياً إذا لم تحدد اسماً مخصصاً
                  </Typography>
                </Box>

                {/* Action Buttons */}
                <Box sx={{ 
                  display: 'flex', 
                  gap: 2, 
                  justifyContent: 'flex-end',
                  mt: 3
                }}>
                  <Button
                    variant="outlined"
                    onClick={() => setCustomExportExpanded(false)}
                    sx={{
                      borderRadius: 3,
                      px: 3,
                      py: 1.2,
                      borderColor: '#e0e0e0',
                      color: '#666',
                      fontWeight: 600,
                      fontSize: 13,
                      textTransform: 'none',
                      '&:hover': {
                        borderColor: '#d0d0d0',
                        bgcolor: '#f8f8f8'
                      }
                    }}
                  >
                    إلغاء
                  </Button>
                  
                  <Button
                    variant="contained"
                    onClick={handleCustomDateExport}
                    disabled={!customExportDate}
                    sx={{
                      borderRadius: 3,
                      px: 4,
                      py: 1.2,
                      bgcolor: customExportDate ? '#1e6641' : '#ccc',
                      color: 'white',
                      fontWeight: 'bold',
                      fontSize: 14,
                      textTransform: 'none',
                      minWidth: 140,
                      transition: 'all 0.3s ease-out',
                      '&:hover': {
                        bgcolor: customExportDate ? '#14532d' : '#ccc',
                        transform: customExportDate ? 'translateY(-2px)' : 'none',
                        boxShadow: customExportDate ? '0 8px 20px rgba(30, 102, 65, 0.3)' : 'none',
                      },
                      '&:disabled': {
                        bgcolor: '#ccc',
                        cursor: 'not-allowed',
                        transform: 'none'
                      }
                    }}
                    startIcon={<FileDownloadIcon />}
                  >
                    {customExportDate ? 'تصدير' : 'اختر تاريخ أولاً'}
                  </Button>
                </Box>
              </Box>

              {/* Loading Overlay */}
              {loading && (
                <Box sx={{
                  position: 'absolute',
                  top: 0,
                  left: 0,
                  right: 0,
                  bottom: 0,
                  bgcolor: 'rgba(255,255,255,0.8)',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  borderRadius: 4
                }}>
                  <Box sx={{ textAlign: 'center' }}>
                    <CircularProgress size={40} sx={{ color: '#1e6641', mb: 2 }} />
                    <Typography variant="body2" sx={{ color: '#1e6641', fontWeight: 600 }}>
                      جاري التصدير...
                    </Typography>
                  </Box>
                </Box>
              )}
            </Box>
          </Fade>
        </Modal>

        {/* Keep the old collapse structure for backward compatibility, but hide it */}
        <Collapse in={false}>
          <Box sx={{ 
            px: 2, 
            mb: 2,
            bgcolor: '#fafbfc',
            borderRadius: 3,
            border: '1px solid #e9ecef',
            py: 2
          }}>
            <Typography variant="body2" sx={{ color: '#495057', mb: 1.5, fontSize: 13, fontWeight: 600 }}>
              📅 تصدير لتاريخ مخصص
            </Typography>
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5 }}>
              <TextField
                type="date"
                size="small"
                value={customExportDate}
                onChange={(e) => setCustomExportDate(e.target.value)}
                sx={{
                  "& .MuiOutlinedInput-root": {
                    borderRadius: 2,
                    fontSize: 13,
                    "& fieldset": { borderColor: "#e0e0e0" },
                    "&:hover fieldset": { borderColor: "#1e6641" },
                    "&:focus fieldset": { borderColor: "#1e6641" },
                  },
                  "& .MuiInputLabel-root": { color: "#666", fontSize: 12 },
                }}
              />
              
              {/* Show quarter mapping for custom date */}
              {customExportDate && (
                <Box sx={{ 
                  display: 'flex', 
                  alignItems: 'center', 
                  gap: 1, 
                  px: 1.5, 
                  py: 0.8, 
                  bgcolor: '#e8f5e8', 
                  borderRadius: 2,
                  border: '1px solid #c3e6c3',
                  animation: 'fadeIn 0.3s ease-in-out'
                }}>
                  <Typography variant="caption" sx={{ color: '#1e6641', fontWeight: 600, fontSize: 11 }}>
                    🎯 الربع: {getQuarterFromDate(customExportDate)}
                  </Typography>
                </Box>
              )}
              
              <Button
                variant="contained"
                onClick={handleCustomDateExport}
                disabled={!customExportDate}
                size="small"
                sx={{
                  borderRadius: 2,
                  bgcolor: customExportDate ? '#1e6641' : '#ccc',
                  color: 'white',
                  fontWeight: 600,
                  fontSize: 12,
                  textTransform: 'none',
                  py: 1,
                  transition: 'all 0.2s ease-in-out',
                  '&:hover': {
                    bgcolor: customExportDate ? '#14532d' : '#ccc',
                    transform: customExportDate ? 'translateY(-1px)' : 'none',
                    boxShadow: customExportDate ? '0 4px 8px rgba(30, 102, 65, 0.3)' : 'none',
                  },
                  '&:disabled': {
                    bgcolor: '#ccc',
                    cursor: 'not-allowed',
                  }
                }}
                startIcon={customExportDate ? <FileDownloadIcon /> : null}
              >
                {customExportDate ? '📥 تصدير للتاريخ المحدد' : 'اختر تاريخ أولاً'}
              </Button>
            </Box>
          </Box>
          
          {/* File Naming Customization */}
          <Box sx={{ 
            px: 2, 
            mb: 2,
            bgcolor: '#fafbfc',
            borderRadius: 3,
            border: '1px solid #e9ecef',
            py: 2
          }}>
            <Typography variant="body2" sx={{ color: '#495057', mb: 1.5, fontSize: 13, fontWeight: 600 }}>
              📝 تخصيص اسم الملف
            </Typography>
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5 }}>
              <TextField
                size="small"
                placeholder="اسم مخصص للملف (اختياري)"
                value={customFileName}
                onChange={(e) => setCustomFileName(e.target.value)}
                sx={{
                  "& .MuiOutlinedInput-root": {
                    borderRadius: 2,
                    fontSize: 13,
                    "& fieldset": { borderColor: "#e0e0e0" },
                    "&:hover fieldset": { borderColor: "#1e6641" },
                    "&:focus fieldset": { borderColor: "#1e6641" },
                  },
                  "& .MuiInputLabel-root": { color: "#666", fontSize: 12 },
                }}
              />
              <Typography variant="caption" sx={{ color: '#888', fontSize: 10, fontStyle: 'italic' }}>
                💡 سيتم إضافة التاريخ والوقت تلقائياً
              </Typography>
            </Box>
          </Box>
        </Collapse>
        
        {/* Quarterly Archives List */}
        <List>
          {snapshotsLoading && (
            <ListItem sx={{ justifyContent: 'center' }}><CircularProgress size={22} sx={{ color: '#1e6641' }} /></ListItem>
          )}
          {!snapshotsLoading && snapshotsError && (
            <ListItem><Alert severity="error">{snapshotsError}</Alert></ListItem>
          )}
          {!snapshotsLoading && !snapshotsError && snapshots.length === 0 && (
            <ListItem sx={{ justifyContent: 'center', color: '#888' }}>لا توجد ملفات محفوظة بعد</ListItem>
          )}
          {!snapshotsLoading && !snapshotsError && snapshots.length > 0 && snapshots.map((snap) => (
              <ListItem key={`${snap.year}-${snap.quarter}-${snap.snapshot_date}-${snap.download_url || ''}`} sx={{ pl: 2, pr: 2, py: 1, borderBottom: '1px solid #e0e0e0', display: 'flex', alignItems: 'center' }}>
                <Typography sx={{ fontWeight: 500, color: '#1e6641', flexGrow: 1, fontSize: 16 }}>
                  {`${snap.year} ${snap.quarter.replace('Q', 'Q')} — ${snap.snapshot_date}`}
                </Typography>
                <Tooltip title={`تاريخ الاستخراج: ${snap.snapshot_date}`} arrow>
                  <Button
                    variant="contained"
                    color="success"
                    size="small"
                    href={`${API_URL}${snap.download_url}`}
                    target="_blank"
                    rel="noopener noreferrer"
                    sx={{ minWidth: 0, px: 2, py: 1, borderRadius: 2, fontWeight: 600 }}
                    startIcon={<DownloadIcon />}
                  >
                    تحميل
                  </Button>
                </Tooltip>
              </ListItem>
            ))}
        </List>
      </Drawer>
    </Box>
  );
}

export default App;
