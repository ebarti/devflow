import { beforeEach, afterEach, describe, expect, it, vi } from 'vitest'
import { act, cleanup, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { RunDetails } from '../src/RunDetails'
import { App } from '../src/App'
import { api } from '../src/api'
import { subscribeRun } from '../src/stream'
import { mockRun, mockService } from './fixtures'

vi.mock('../src/stream', () => ({ subscribeRun: vi.fn(() => () => {}) }))
beforeEach(() => { vi.spyOn(api, 'getProjectBoard').mockResolvedValue({ items: [], projects: [] }) })
afterEach(() => { cleanup(); vi.restoreAllMocks(); vi.unstubAllGlobals(); window.history.replaceState(null, '', '/') })

describe('tokenless local dashboard', () => {
  it('clears the board disconnect banner when automatic polling recovers', async () => {
    let poll: () => void = () => {}
    const interval = window.setInterval.bind(window)
    vi.spyOn(window, 'setInterval').mockImplementation(((handler: () => void, delay: number) => {
      if (delay === 5000) { poll = handler; return 1 }
      return interval(handler, delay)
    }) as typeof window.setInterval)
    const list = vi.spyOn(api, 'listRunsPage')
      .mockRejectedValueOnce(new Error('Merge projection failed'))
      .mockResolvedValue({ runs: [mockRun], next_cursor: null })
    vi.spyOn(api, 'getService').mockResolvedValue(mockService)
    render(<App />)
    await screen.findByText('Disconnected · showing last observed state')
    await act(async () => { poll() })
    await screen.findByRole('button', { name: /Fixture workflow/ })
    await waitFor(() => expect(screen.queryByText('Disconnected · showing last observed state')).toBeNull())
    expect(list).toHaveBeenCalledTimes(2)
    expect(screen.getByText('Connected')).toBeTruthy()
  })

  it.each(['runs', 'service'])('retries board and service reads after a failed %s read', async failed => {
    const list = vi.spyOn(api, 'listRunsPage').mockResolvedValue({ runs: [mockRun], next_cursor: null })
    const service = vi.spyOn(api, 'getService').mockResolvedValue(mockService)
    if (failed === 'runs') list.mockRejectedValueOnce(new Error('Merge projection failed'))
    else service.mockRejectedValueOnce(new Error('Service read failed'))
    render(<App />)
    await screen.findByText('Disconnected · showing last observed state')
    await userEvent.setup().click(screen.getByRole('button', { name: 'Retry' }))
    await screen.findByRole('button', { name: /Fixture workflow/ })
    await waitFor(() => expect(screen.queryByText('Disconnected · showing last observed state')).toBeNull())
    expect(list).toHaveBeenCalledTimes(2)
    expect(service).toHaveBeenCalledTimes(2)
  })

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
    const list = vi.spyOn(api, 'listRunsPage').mockResolvedValue({ runs: [mockRun], next_cursor: null })
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
    const updated = { ...mockRun, title: 'Live update after retry', goal: 'Live update after retry' }
    list.mockResolvedValue({ runs: [updated], next_cursor: null })
    await act(async () => {
      const hooks = subscribe.mock.calls[0][2]
      hooks.onConnection('connected')
      hooks.onSnapshot(updated)
    })
    expect(screen.getByText('Connected')).toBeTruthy()
    expect(screen.getAllByText('Live update after retry')).toHaveLength(2)
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

it('shows publication and directory retention separately from process settlement', () => {
  render(<RunDetails run={{ ...mockRun, decisions: [], cleanup: 'confirmed',
    retained_worktrees: { state: 'observed', count: 1, roots: [
      { path: '/owned/candidate', reason: 'blocked-run candidate and recovery source are preserved' },
    ] },
    events: [
      { sequence: 1, type: 'candidate_ready', message: 'Candidate ready for publication' },
      { sequence: 2, type: 'published', message: 'Regular pull request read back at candidate head' },
    ],
  }} onRefresh={async () => {}} />)
  expect(screen.getByText('Pull request published on GitHub')).toBeTruthy()
  expect(screen.queryByText('Regular pull request read back at candidate head')).toBeNull()
  expect(screen.getByText('Process/resource settlement')).toBeTruthy()
  expect(screen.getByText('Retained worktrees')).toBeTruthy()
  expect(screen.getByText(/blocked-run candidate and recovery source/)).toBeTruthy()
})
