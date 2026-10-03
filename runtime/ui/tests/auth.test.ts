import { afterEach, describe, expect, it, vi } from 'vitest'

afterEach(() => vi.unstubAllGlobals())

describe('anonymous local CSRF state', () => {
  it('uses the public tracker-only contract and preserves an uncertain command identity', async () => {
    const fetchMock = vi.fn(async (path: string) => ({
      ok: true, status: 200, json: async () => path === '/api/session'
        ? { csrf_token: 'terminal-csrf' }
        : { run_id: 'run-1', phase: 'terminal_tracker_recovery_queued', reconciliation_only: true },
    }))
    vi.stubGlobal('fetch', fetchMock)
    vi.resetModules()
    const { api } = await import('../src/api')
    const body = { command_id: 'same-readback', expected_revision: 19 }
    await api.reconcileTracker('run-1', body)
    const [path, init] = fetchMock.mock.calls[1] as unknown as [string, RequestInit]
    expect(path).toBe('/api/runs/run-1/reconcile-tracker')
    expect(init.headers).toMatchObject({ 'X-Devflow-CSRF': 'terminal-csrf' })
    expect(JSON.parse(String(init.body))).toEqual(body)
    expect(init.method).toBe('POST')
  })

  it('automatically bootstraps and renews before commands without a credential', async () => {
    const responses = [{ csrf_token: 'first-csrf' }, {}, { csrf_token: 'renewed-csrf' }, {}]
    const fetchMock = vi.fn(async () => ({ ok: true, status: 200, json: async () => responses.shift() }))
    vi.stubGlobal('fetch', fetchMock)
    vi.resetModules()
    const { api } = await import('../src/api')
    const body = { command_id: 'fixture-command', expected_revision: 4, reason: 'fixture reason' }
    await api.cancel('fixture-run', body)
    await api.cancel('fixture-run', { ...body, command_id: 'later-command' })
    expect(fetchMock).toHaveBeenCalledTimes(4)
    for (const index of [0, 2]) {
      const [path, init] = fetchMock.mock.calls[index] as unknown as [string, RequestInit]
      expect(path).toBe('/api/session')
      expect(init.body).toBeUndefined()
      expect(init.credentials).toBe('same-origin')
    }
    for (const [index, csrf] of [[1, 'first-csrf'], [3, 'renewed-csrf']] as const) {
      const [path, init] = fetchMock.mock.calls[index] as unknown as [string, RequestInit]
      expect(path).toBe('/api/runs/fixture-run/cancel')
      expect(init.headers).toMatchObject({ 'X-Devflow-CSRF': csrf })
      expect(JSON.parse(String(init.body))).not.toHaveProperty('token')
    }
  })

  it('shares first-visit bootstrap across concurrent commands', async () => {
    let resolveSession!: (value: object) => void
    const session = new Promise<object>(resolve => { resolveSession = resolve })
    const fetchMock = vi.fn(async (path: string) => ({
      ok: true, status: 200,
      json: async () => path === '/api/session' ? await session : {},
    }))
    vi.stubGlobal('fetch', fetchMock)
    vi.resetModules()
    const { api } = await import('../src/api')
    const body = { command_id: 'fixture-command', expected_revision: 4, reason: 'fixture reason' }
    const commands = [api.cancel('first-run', body), api.cancel('second-run', body)]
    expect(fetchMock).toHaveBeenCalledTimes(1)
    resolveSession({ csrf_token: 'shared-csrf' })
    await Promise.all(commands)
    expect(fetchMock).toHaveBeenCalledTimes(3)
  })

  it('does not replay a refused or transport-failed mutation', async () => {
    const fetchMock = vi.fn(async (path: string) => {
      if (path === '/api/session') return { ok: true, status: 200, json: async () => ({ csrf_token: 'csrf' }) }
      return { ok: false, status: 403, json: async () => ({ detail: 'Origin refused' }) }
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.resetModules()
    const { api } = await import('../src/api')
    const body = { command_id: 'fixture-command', expected_revision: 4, reason: 'fixture reason' }
    await expect(api.cancel('fixture-run', body)).rejects.toMatchObject({ status: 403 })
    expect(fetchMock).toHaveBeenCalledTimes(2)
    fetchMock.mockImplementation(async path => {
      if (path === '/api/session') return { ok: true, status: 200, json: async () => ({ csrf_token: 'csrf' }) }
      throw new TypeError('network failed after sending')
    })
    await expect(api.cancel('fixture-run', body)).rejects.toMatchObject({ status: 0 })
    expect(fetchMock).toHaveBeenCalledTimes(4)
  })
})
