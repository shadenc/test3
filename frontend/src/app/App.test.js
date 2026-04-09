import { render, screen, waitFor } from '@testing-library/react';
import App from './App';

function mockFetch() {
  global.fetch = jest.fn((input) => {
    const url = typeof input === 'string' ? input : String(input.url);

    if (url.includes('foreign_ownership_data.json')) {
      return Promise.resolve({
        ok: true,
        json: () => Promise.resolve([]),
      });
    }
    if (url.includes('retained_earnings_flow.csv')) {
      return Promise.resolve({
        ok: true,
        text: () => Promise.resolve('company_symbol,quarter\n'),
      });
    }
    if (url.includes('/api/net-profit')) {
      return Promise.resolve({
        ok: true,
        json: () => Promise.resolve([]),
      });
    }
    if (url.includes('/api/ownership_snapshots')) {
      return Promise.resolve({
        ok: true,
        json: () => Promise.resolve([]),
      });
    }
    if (url.includes('/api/user_exports')) {
      return Promise.resolve({
        ok: true,
        json: () => Promise.resolve([]),
      });
    }
    return Promise.resolve({ ok: false, status: 404, json: () => Promise.resolve({}) });
  });
}

beforeEach(() => {
  mockFetch();
});

test('renders dashboard shell after data load', async () => {
  render(<App />);
  await waitFor(() => {
    expect(screen.getByText(/تصدير الجدول/i)).toBeInTheDocument();
  });
});
