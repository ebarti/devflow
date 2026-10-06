import { afterEach, describe, expect, it, vi } from 'vitest'
import { act, cleanup, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { RunDetails } from '../src/RunDetails'
import { App } from '../src/App'
import { api } from '../src/api'
import { subscribeRun } from '../src/stream'
import { mockRun, mockService } from './fixtures'

vi.mock('../src/stream', () => ({ subscribeRun: vi.fn(() => () => {}) }))
afterEach(() => { cleanup(); vi.restoreAllMocks(); vi.unstubAllGlobals(); window.history.replaceState(null, '', '/') })

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

  it('restores event subscription when Retry succeeds after the first detail read failed', async () => {
    window.history.replaceState(null, '', `/runs/${mockRun.id}`)
    vi.spyOn(api, 'listRuns').mockResolvedValue([mockRun])
    vi.spyOn(api, 'getService').mockResolvedValue(mockService)
    const read = vi.spyOn(api, 'getRun').mockRejectedValueOnce(new Error('API reloading')).mockResolvedValue(mockRun)
    const subscribe = vi.mocked(subscribeRun)
    subscribe.mockClear()
    const user = userEvent.setup()
    render(<App />)
    await screen.findByText('Run unavailable')
    expect(subscribe).not.toHaveBeenCalled()
    await user.click(screen.getAllByRole('button', { name: 'Retry' })[0])
    await waitFor(() => expect(subscribe).toHaveBeenCalledTimes(1))
    expect(read).toHaveBeenCalledTimes(2)
    act(() => {
      const hooks = subscribe.mock.calls[0][2]
      hooks.onConnection('connected')
      hooks.onSnapshot({ ...mockRun, title: 'Live update after retry', goal: 'Live update after retry' })
    })
    expect(screen.getByText('Connected')).toBeTruthy()
    expect(screen.getByText('Live update after retry')).toBeTruthy()
  })
})

it('shows the actual failed QA finding above the workflow instead of relying on its optimistic summary', () => {
  render(<RunDetails run={{ ...mockRun, error: 'repair limit exhausted', phase: 'blocked',
    decisions: [], checks: { qa: { state: 'failed', detail: 'Supported checks succeeded.' } },
    roles: [{ role: 'verify', state: 'findings', summary: 'Supported checks succeeded.',
      findings: ['Dependency receipt omits source hashes and staged transformations.'] }],
  }} onRefresh={async () => {}} />)
  const blocker = screen.getByRole('region', { name: 'Blocking findings' })
  expect(blocker.textContent).toContain('Dependency receipt omits source hashes')
  expect(blocker.textContent).toContain('repair budget')
  expect(blocker.compareDocumentPosition(screen.getByRole('region', { name: 'Workflow phase gates' })) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
})

it('retains repeated review attempts without duplicate React keys', () => {
  const errors = vi.spyOn(console, 'error').mockImplementation(() => {})
  render(<RunDetails run={{ ...mockRun, roles: [
    { role: 'review', state: 'finished', iteration: 0, attempt_id: 'original', session_id: 'old-review' },
    { role: 'review', state: 'finished', iteration: 0, attempt_id: 'retry', session_id: 'fresh-review' },
  ] }} onRefresh={async () => {}} />)
  expect(screen.getByText('old-review')).toBeTruthy()
  expect(screen.getByText('fresh-review')).toBeTruthy()
  expect(errors).not.toHaveBeenCalled()
})
