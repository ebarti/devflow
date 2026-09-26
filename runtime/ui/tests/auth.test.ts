import { afterEach, describe, expect, it, vi } from 'vitest'

afterEach(() => vi.unstubAllGlobals())

describe('local browser session', () => {
  it('keeps the service token in the login body and uses only CSRF on commands', async () => {
    const responses = [
      { authenticated: false },
      { csrf_token: 'fixture-csrf' },
      {},
    ]
    const fetchMock = vi.fn(async () => ({ ok: true, status: 200, json: async () => responses.shift() }))
    vi.stubGlobal('fetch', fetchMock)
    vi.resetModules()
    const { api } = await import('../src/api')
    expect(await api.session()).toBe(false)
    await api.login('fixture-secret')
    await api.cancel('fixture-run', { command_id: 'fixture-command', expected_revision: 4, reason: 'fixture reason' })
    expect(fetchMock).toHaveBeenCalledTimes(3)
    const [loginPath, loginInit] = fetchMock.mock.calls[1] as unknown as [string, RequestInit]
    const [commandPath, commandInit] = fetchMock.mock.calls[2] as unknown as [string, RequestInit]
    expect(loginPath).toBe('/api/session')
    expect(JSON.parse(String(loginInit.body))).toEqual({ token: 'fixture-secret' })
    expect(commandPath).toBe('/api/runs/fixture-run/cancel')
    expect(commandPath).not.toContain('fixture-secret')
    expect(commandInit.headers).toMatchObject({ 'X-Devflow-CSRF': 'fixture-csrf' })
    expect(String(commandInit.body)).not.toContain('fixture-secret')
  })
})
