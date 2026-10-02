import { afterEach, describe, expect, it, vi } from 'vitest'
import { act, cleanup, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
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
