import { afterEach, describe, expect, it, vi } from 'vitest'
import { api } from '../src/api'

afterEach(() => vi.unstubAllGlobals())

describe('backend projection adapter', () => {
  it('maps the concrete service and run projection without inferring quality results', async () => {
    const service = {
      status: 'running', version: 'fixture-only',
      policy: { roles: {}, repositories: [{ key: 'fixture-repo', label: 'Fixture repo', base_ref: 'main' }] },
    }
    const run = {
      id: 'fixture-run', revision: 2, sequence: 4, phase: 'verify',
      queued: 2, cleanup: 'pending', capacity: { active: 1, limit: 3 },
      candidate: { head: 'fixture-head', base_sha: 'fixture-base', content_sha256: 'fixture-content', revision: 2 },
      checks: { local: { state: 'passed' }, ci: { state: 'pending' } },
      tracker: { state: 'pending', observed: { issue: 'open' } },
    }
    const fetchMock = vi.fn(async (path: string) => ({
      ok: true, status: 200, json: async () => path === '/api/service' ? service : { run, events: [{ sequence: 4, message: 'Fixture event' }], evidence: [] },
    }))
    vi.stubGlobal('fetch', fetchMock)
    const info = await api.getService()
    expect(info.repositories?.[0].key).toBe('fixture-repo')
    const detail = await api.getRun('fixture-run')
    expect(detail.capacity).toMatchObject({ active: 1, queued: 2, cleanup: 'pending' })
    expect(detail.candidate).toMatchObject({ head: 'fixture-head', base: 'fixture-base', content_digest: 'fixture-content' })
    expect(detail.tracker).toMatchObject({ pending: true, observed: '{"issue":"open"}' })
    expect(detail.checks?.local?.state).toBe('passed')
    expect(detail.checks?.review).toBeUndefined()
    expect(detail.checks?.qa).toBeUndefined()
    expect(detail.events?.[0].sequence).toBe(4)
  })
})
