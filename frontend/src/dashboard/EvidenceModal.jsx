/**
 * Modal: evidence screenshot, extracted value, and manual correction POST to the API.
 */

import React, { useState } from 'react';
import PropTypes from 'prop-types';
import Modal from '@mui/material/Modal';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import IconButton from '@mui/material/IconButton';
import CloseIcon from '@mui/icons-material/Close';
import CircularProgress from '@mui/material/CircularProgress';
import Alert from '@mui/material/Alert';
import InfoOutlinedIcon from '@mui/icons-material/InfoOutlined';
import Tooltip from '@mui/material/Tooltip';
import TextField from '@mui/material/TextField';
import Button from '@mui/material/Button';
import { apiUrl, buildEvidenceScreenshotUrl, withCsrfHeaders } from '../services/apiClient';
import { devLog } from './dataUtils';

function EvidenceScalingBlurb({ evidenceData }) {
  const rawStr = String(evidenceData.value || '').replaceAll(/[^0-9,.-]/g, '');
  const raw = Number(rawStr.replaceAll(',', ''));
  const mult = Number(evidenceData.applied_multiplier || 1);
  const unit = String(evidenceData.unit_detected || 'SAR');
  let unitLabel = 'غير محدد';
  if (unit === 'million_SAR') unitLabel = 'بالملايين';
  else if (unit === 'thousand_SAR') unitLabel = 'بالآلاف';
  else if (unit === 'SAR') unitLabel = 'بالريال السعودي';
  if (!raw || mult === 1) {
    const directSar =
      unit === 'SAR' || mult === 1
        ? 'القيم بالريال السعودي مباشرة (بدون تحويل).'
        : `الوحدة: ${unitLabel}`;
    return (
      <Typography variant="body2" sx={{ color: '#4d4d4d' }}>
        {directSar}
      </Typography>
    );
  }
  const result = raw * mult;
  return (
    <>
      <Typography variant="body2" sx={{ color: '#4d4d4d' }}>
        تم اكتشاف أن القيم {unitLabel}. تم تحويل القيمة كما يلي:
      </Typography>
      <Typography variant="body2" sx={{ mt: 0.5, direction: 'ltr', fontFamily: 'monospace', color: '#1e6641' }}>
        {raw.toLocaleString('en-US')} × {mult.toLocaleString('en-US')} = {result.toLocaleString('en-US')}
      </Typography>
    </>
  );
}

EvidenceScalingBlurb.propTypes = {
  evidenceData: PropTypes.object.isRequired,
};

export function EvidenceModal({ open, onClose, evidenceData, loading, error, onDataUpdate }) {
  const [verifyMode, setVerifyMode] = useState(null);
  const [correctionValue, setCorrectionValue] = useState('');
  const [correctionFeedback, setCorrectionFeedback] = useState('');
  const [submitted, setSubmitted] = useState(false);

  return (
    <Modal
      open={open}
      onClose={onClose}
      aria-labelledby="evidence-modal-title"
      aria-describedby="evidence-modal-description"
    >
      <Box sx={{
        position: 'absolute',
        top: '50%',
        left: '50%',
        transform: 'translate(-50%, -50%)',
        width: { xs: '95%', md: '70%' },
        maxWidth: 600,
        maxHeight: '80vh',
        bgcolor: 'background.paper',
        borderRadius: 3,
        boxShadow: 24,
        p: 3,
        overflow: 'auto',
        direction: 'rtl',
      }}
      >
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
          <Typography id="evidence-modal-title" variant="h5" component="h2" sx={{ fontWeight: 'bold', color: '#1e6641' }}>
            دليل الاستخراج - الأرباح المبقاة
          </Typography>
          <IconButton onClick={onClose} sx={{ color: '#666' }}>
            <CloseIcon />
          </IconButton>
        </Box>

        {loading && (
          <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', py: 4 }}>
            <CircularProgress sx={{ color: '#1e6641' }} />
            <Typography sx={{ ml: 2, color: '#666' }}>جاري تحميل الدليل...</Typography>
          </Box>
        )}

        {error && (
          <Alert severity="error" sx={{ mb: 2 }}>
            {error}
          </Alert>
        )}

        {evidenceData && !loading && (
          <>
            {evidenceData.evidence?.has_evidence && (
              <Box sx={{ mb: 4 }}>
                <Box sx={{
                  display: 'flex',
                  justifyContent: 'center',
                  border: '2px solid #e0e0e0',
                  borderRadius: 2,
                  overflow: 'auto',
                  bgcolor: '#fafafa',
                  maxHeight: '50vh',
                }}
                >
                  <img
                    src={buildEvidenceScreenshotUrl(
                      evidenceData.company_symbol,
                      evidenceData.evidence?.requested_quarter,
                    )}
                    alt="Evidence Screenshot"
                    style={{
                      maxWidth: '100%',
                      maxHeight: 'none',
                      objectFit: 'contain',
                    }}
                    onLoad={() => {
                      devLog('Evidence image loaded with quarter:', evidenceData.evidence?.requested_quarter);
                      devLog(
                        'Full image URL:',
                        buildEvidenceScreenshotUrl(
                          evidenceData.company_symbol,
                          evidenceData.evidence?.requested_quarter,
                        ),
                      );
                    }}
                  />
                </Box>
              </Box>
            )}

            {evidenceData.numeric_value && (
              <Box sx={{ mt: 3 }}>
                <Typography variant="subtitle1" sx={{ fontWeight: 'bold', color: '#1e6641', display: 'flex', alignItems: 'center', gap: 1 }}>
                  القيمة المستخرجة:
                  {evidenceData.applied_multiplier && Number(evidenceData.applied_multiplier) > 1 && (
                    <span style={{ color: '#888', fontWeight: 400 }}>(تم تطبيق تحويل الوحدة)</span>
                  )}
                </Typography>
                <Typography variant="h6" sx={{ mt: 0.5 }}>
                  {Number(evidenceData.numeric_value).toLocaleString('en-US')} SAR
                </Typography>

                <Box sx={{ mt: 1.5, p: 1.5, bgcolor: '#f7f9f8', border: '1px solid #e0e6e4', borderRadius: 1.5 }}>
                  <EvidenceScalingBlurb evidenceData={evidenceData} />
                </Box>
              </Box>
            )}

            {evidenceData.extraction_method && (
              <Box sx={{
                p: 3,
                bgcolor: '#f8f9fa',
                borderRadius: 2,
                border: '1px solid #e0e0e0',
              }}
              >
                <Typography sx={{
                  fontSize: '1rem',
                  color: '#666',
                  fontWeight: '500',
                }}
                >
                  <strong>طريقة الاستخراج:</strong> {evidenceData.extraction_method}
                </Typography>
              </Box>
            )}

            {evidenceData.context && (
              <Box>
                <Typography variant="h6" sx={{ fontWeight: 'bold', mb: 2, color: '#1e6641' }}>
                  النص المستخرج
                </Typography>
                <Box sx={{
                  p: 2,
                  bgcolor: '#f8f9fa',
                  borderRadius: 2,
                  border: '1px solid #e0e0e0',
                  fontFamily: 'monospace',
                  fontSize: '14px',
                  whiteSpace: 'pre-wrap',
                  maxHeight: '200px',
                  overflow: 'auto',
                }}
                >
                  {evidenceData.context}
                </Box>
              </Box>
            )}
            {evidenceData?.context && !loading && (
              <Box sx={{ mt: 2, textAlign: 'left' }}>
                <Tooltip title="تعديل النتيجة" arrow>
                  <IconButton
                    size="small"
                    sx={{ color: '#1e6641', opacity: 0.7, ml: 1, '&:hover': { opacity: 1, bgcolor: '#e8f5ee' } }}
                    onClick={() => setVerifyMode(verifyMode ? null : 'form')}
                  >
                    <InfoOutlinedIcon fontSize="small" />
                  </IconButton>
                </Tooltip>
                {verifyMode === 'form' && (
                  <Box sx={{ mt: 1, display: 'flex', flexDirection: 'column', gap: 1 }}>
                    <TextField
                      size="small"
                      label="القيمة الصحيحة"
                      value={correctionValue}
                      onChange={(e) => setCorrectionValue(e.target.value)}
                      sx={{ maxWidth: 180 }}
                    />
                    <TextField
                      size="small"
                      label="ملاحظات (اختياري)"
                      value={correctionFeedback}
                      onChange={(e) => setCorrectionFeedback(e.target.value)}
                      sx={{ maxWidth: 250 }}
                    />
                    <Button
                      size="small"
                      variant="contained"
                      color="primary"
                      sx={{ fontSize: 13, px: 2, py: 0.5, mt: 1, alignSelf: 'flex-start' }}
                      onClick={async () => {
                        setSubmitted(true);
                        try {
                          const res = await fetch(apiUrl('api/correct_retained_earnings'), {
                            method: 'POST',
                            headers: await withCsrfHeaders({ 'Content-Type': 'application/json' }),
                            body: JSON.stringify({
                              company_symbol: evidenceData.company_symbol || evidenceData.symbol,
                              correct_value: correctionValue,
                              feedback: correctionFeedback,
                            }),
                          });
                          const data = await res.json();
                          if (data.status === 'success') {
                            evidenceData.value = correctionValue;
                            if (typeof onDataUpdate === 'function') {
                              onDataUpdate();
                            }
                          }
                        } catch (e) {
                          devLog('correct_retained_earnings failed', e);
                        }
                        setVerifyMode(null);
                        if (typeof onClose === 'function') onClose();
                      }}
                    >
                      إرسال التصحيح
                    </Button>
                    {submitted && (
                      <Typography sx={{ color: '#1e6641', fontSize: 14, mt: 1 }}>شكرًا لملاحظتك! تم تسجيل التصحيح.</Typography>
                    )}
                  </Box>
                )}
                {submitted && !verifyMode && (
                  <Typography sx={{ color: '#1e6641', fontSize: 14, mt: 1 }}>شكرًا لملاحظتك! تم تسجيل التصحيح.</Typography>
                )}
              </Box>
            )}
          </>
        )}
      </Box>
    </Modal>
  );
}

EvidenceModal.propTypes = {
  open: PropTypes.bool.isRequired,
  onClose: PropTypes.func.isRequired,
  evidenceData: PropTypes.object,
  loading: PropTypes.bool,
  error: PropTypes.string,
  onDataUpdate: PropTypes.func,
};
