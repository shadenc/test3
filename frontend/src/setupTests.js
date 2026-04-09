// jest-dom adds custom jest matchers for asserting on DOM nodes.
// allows you to do things like:
// expect(element).toHaveTextContent(/react/i)
// learn more: https://github.com/testing-library/jest-dom
import '@testing-library/jest-dom';
import { TextDecoder, TextEncoder } from 'node:util';

// @mui/x-data-grid expects Web APIs in the Jest/jsdom environment
globalThis.TextEncoder = TextEncoder;
globalThis.TextDecoder = TextDecoder;
