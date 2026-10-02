import { afterEach, describe, expect, it, vi } from 'vitest'
import { cleanup, render, screen, waitFor } from '@testing-library/react'
import { App } from '../src/App'
import { mockRun, mockService } from './fixtures'

vi.mock('../src/stream', () => ({ subscribeRun: vi.fn(() => () => {}) }))
afterEach(() => { cleanup(); vi.unstubAllGlobals(); window.history.replaceState(null, '', '/') })

describe('tokenless local dashboard', () => {
  it.each(['', 'devflow_session=expired'])('reads a direct run visit with cookie %j without login', async cookie => {
    document.cookie = cookie
    window.history.replaceState(null, '', `/runs/${mockRun.id}`)
    const fetchMock = vi.fn(async (path: string) => ({
      ok: true, status: 200,
      json: async () => {
        if (path === '/api/runs') return { runs: [mockRun] }
        if (path === '/api/service') return mockService
        if (path === `/api/runs/${mockRun.id}`) return { run: mockRun, events: [], evidence: [] }
        throw new Error(`Unexpected request ${path}`)
      },
    }))
    vi.stubGlobal('fetch', fetchMock)
    render(<App />)
    await waitFor(() => expect(screen.getByText('Run queued')).toBeTruthy())
    expect(screen.queryByLabelText(/service token/i)).toBeNull()
    expect(screen.queryByText(/sign in|connect to devflow/i)).toBeNull()
    expect(fetchMock.mock.calls.map(([path]) => path)).not.toContain('/api/session')
    expect((screen.getByRole('button', { name: 'New run' }) as HTMLButtonElement).disabled).toBe(false)
  })
})
