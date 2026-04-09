import React from 'react';
import PropTypes from 'prop-types';
import Modal from '@mui/material/Modal';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import LinearProgress from '@mui/material/LinearProgress';

const paperSx = {
  position: 'absolute',
  top: '50%',
  left: '50%',
  transform: 'translate(-50%, -50%)',
  bgcolor: 'background.paper',
  borderRadius: 2,
  boxShadow: 24,
  p: 3,
  direction: 'rtl',
};

export function JobProgressShell({
  open,
  width,
  title,
  titleColor,
  progressColor = 'success',
  titleMb = 1,
  children,
}) {
  return (
    <Modal open={open} onClose={() => {}}>
      <Box sx={{ ...paperSx, width }}>
        <Typography variant="h6" sx={{ fontWeight: 'bold', color: titleColor, mb: titleMb }}>
          {title}
        </Typography>
        <LinearProgress color={progressColor} />
        {children}
      </Box>
    </Modal>
  );
}

JobProgressShell.propTypes = {
  open: PropTypes.bool.isRequired,
  width: PropTypes.number.isRequired,
  title: PropTypes.node.isRequired,
  titleColor: PropTypes.string.isRequired,
  progressColor: PropTypes.oneOf([
    'primary',
    'secondary',
    'error',
    'info',
    'success',
    'warning',
    'inherit',
  ]),
  titleMb: PropTypes.number,
  children: PropTypes.node,
};
