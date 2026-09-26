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
      id: 'fixture-run', revision: 2, protocol_revision: 1, sequence: 4, phase: 'verify',
      queued: false, cleanup: 'none', capacity: { active: 1, limit: 3 },
      candidate: { head: 'fixture-head', base_sha: 'fixture-base', content_sha256: 'fixture-content', revision: 2 },
      checks: { local: { state: 'passed' }, ci: { state: 'pending' } },
      tracker: { state: 'pending', observed: { issue: 'open' } },
      usage: { 'implement:0': { input_tokens: 100, output_tokens: 40, cache_read_tokens: 20, cache_creation_tokens: 0, total_tokens: 160 }, 'verify:0': null },
    }
    const fetchMock = vi.fn(async (path: string) => ({
      ok: true, status: 200, json: async () => path === '/api/service' ? service : { run, events: [{ sequence: 4, message: 'Fixture event' }], evidence: [] },
    }))
    vi.stubGlobal('fetch', fetchMock)
    const info = await api.getService()
    expect(info.repositories?.[0].key).toBe('fixture-repo')
    const detail = await api.getRun('fixture-run')
    expect(detail.capacity).toMatchObject({ active: 1, limit: 3 })
    expect(detail.queued).toBe(false)
    expect(detail.cleanup).toBe('none')
    expect(detail.candidate).toMatchObject({ head: 'fixture-head', base: 'fixture-base', content_digest: 'fixture-content' })
    expect(detail.tracker).toMatchObject({ pending: true, observed: '{"issue":"open"}' })
    expect(detail.usage).toMatchObject({ input_tokens: 100, output_tokens: 40, cache_read_tokens: 20, cache_creation_tokens: 0, total_tokens: 160, source: 'role_attempts' })
    expect(detail.usage?.gaps).toContain('verify:0 usage')
    expect(detail.usage?.status).toBeUndefined()
    expect(detail.usage?.cached_input_tokens).toBeUndefined()
    expect(detail.checks?.local?.state).toBe('passed')
    expect(detail.checks?.review).toBeUndefined()
    expect(detail.checks?.qa).toBeUndefined()
    expect(detail.events?.[0].sequence).toBe(4)
  })

  it('keeps absent per-role token readings unknown', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => ({
      ok: true, status: 200,
      json: async () => ({ run: { id: 'fixture-run', usage: { 'implement:0': null } }, events: [], evidence: [] }),
    })))
    const detail = await api.getRun('fixture-run')
    expect(detail.usage?.source).toBe('role_attempts')
    expect(detail.usage?.status).toBeUndefined()
    expect(detail.usage?.total_tokens).toBeUndefined()
  })
})
