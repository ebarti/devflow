/** Browser QA only. Serves built UI with explicit fixture data; never use for a real run. */
import { createServer } from 'node:http'
import { readFile } from 'node:fs/promises'
import { join, resolve } from 'node:path'

const root = resolve(import.meta.dirname, '../dist')
const port = Number(process.env.DEVFLOW_FIXTURE_PORT || 5179)
const requireLogin = process.env.DEVFLOW_FIXTURE_REQUIRE_LOGIN === '1'
const run = {
  id: 'ui-fixture-run', work_id: 'ui-fixture-work', title: 'UI fixture · workflow detail',
  repository: 'fixture/repository', issue: '#1', issue_url: 'https://github.com/example/repository/issues/1',
  phase: 'ci_wait', execution_state: 'waiting', updated_at: '2026-01-01T12:04:00Z',
  observed_at: '2026-01-01T12:04:00Z', revision: 8, protocol_revision: 8, projection_revision: 12,
  iteration: 0, sequence: 4, authorized_endpoint: 'published_unmerged', queued: false, cleanup: 'none',
  phase_gates: [
    { id: 'prepare', label: 'Prepare', state: 'completed' },
    { id: 'prepublish', label: 'Before PR checks', state: 'completed' },
    { id: 'publish', label: 'Publish', state: 'completed' },
    { id: 'local_checks', label: 'Local checks', state: 'completed' },
    { id: 'required_ci', label: 'Required CI', state: 'pending' },
    { id: 'tracker', label: 'Tracker', state: 'pending' },
  ],
  candidate: { head: 'fixture-head-sha', revision: 2 },
  pull_request: { number: 1, url: 'https://github.com/example/repository/pull/1', state: 'open' },
  tracker: { state: 'consistent', observed: { state: 'consistent', claim: true, fixture: true } },
  roles: [
    { role: 'implement', iteration: 0, state: 'finished', session_id: 'fixture-impl', cleanup: 'confirmed', usage: { total_tokens: 1200 } },
    { role: 'review', iteration: 0, state: 'finished', session_id: 'fixture-review', cleanup: 'confirmed', usage: { total_tokens: 500 } },
    { role: 'verify', iteration: 0, state: 'finished', session_id: 'fixture-verify', cleanup: 'confirmed', usage: null },
  ],
  capacity: { active: 0, limit: 3 },
  checks: { review: { state: 'passed', detail: 'Fixture evidence only' }, qa: { state: 'passed', detail: 'Fixture evidence only' }, local: { state: 'passed' }, ci: { state: 'pending' } },
  usage: { 'implement:0': { total_tokens: 1200 }, 'review:0': { total_tokens: 500 }, 'verify:0': null },
  events: [
    { sequence: 4, timestamp: '2026-01-01T12:04:00Z', message: 'Fixture required CI pending' },
    { sequence: 3, timestamp: '2026-01-01T12:03:00Z', message: 'Fixture local checks passed' },
    { sequence: 2, timestamp: '2026-01-01T12:02:00Z', message: 'Fixture candidate published' },
    { sequence: 1, timestamp: '2026-01-01T12:01:00Z', message: 'Fixture prepublication gates passed' },
  ],
}

const service = {
  status: 'running', version: 'UI FIXTURE ONLY', temporal: 'fixture',
  capacity: { active: 1, queued: 0, limit: 3 },
  policy: {
    repositories: [{ key: 'fixture-repo', label: 'Fixture repository', base_ref: 'main', base_sha: 'fixture-sha', recovery_keys: ['fixture-recovery'] }],
    roles: { implement: { model: 'fixture-model', effort: 'fixture-effort' }, review: { model: 'fixture-model', effort: 'fixture-effort' } },
  },
}

function json(response, data, status = 200) {
  response.writeHead(status, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' })
  response.end(JSON.stringify(data))
}

createServer(async (request, response) => {
  const url = new URL(request.url, `http://${request.headers.host}`)
  response.setHeader('X-Devflow-UI-Fixture', 'test-only')
  if (url.pathname === '/api/session' && request.method === 'GET') {
    const authenticated = !requireLogin || request.headers.cookie?.includes('fixture-session=1')
    return json(response, authenticated ? { authenticated: true, csrf_token: 'fixture-csrf' } : { authenticated: false })
  }
  if (url.pathname === '/api/session' && request.method === 'POST') {
    let body = ''
    for await (const chunk of request) body += chunk
    if (JSON.parse(body).token !== 'fixture-token') return json(response, { detail: 'Invalid fixture token.' }, 401)
    response.setHeader('Set-Cookie', 'fixture-session=1; HttpOnly; SameSite=Strict; Path=/')
    return json(response, { csrf_token: 'fixture-csrf' })
  }
  if (url.pathname.startsWith('/api/') && requireLogin && !request.headers.cookie?.includes('fixture-session=1')) return json(response, { detail: 'Sign in required.' }, 401)
  if (url.pathname === '/api/service') return json(response, service)
  if (url.pathname === '/api/runs' && request.method === 'GET') return json(response, { runs: [run] })
  if (url.pathname === '/api/runs/ui-fixture-run') return json(response, { run, events: run.events, evidence: [] })
  if (url.pathname === '/api/runs/ui-fixture-run/events') {
    response.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' })
    response.write(': fixture stream open\n\n')
    return
  }
  if (url.pathname.startsWith('/api/')) return json(response, { detail: 'Fixture server is read-only.' }, 409)
  const candidate = url.pathname.startsWith('/assets/') ? join(root, url.pathname) : join(root, 'index.html')
  if (!candidate.startsWith(root)) { response.writeHead(403); return response.end() }
  try {
    const body = await readFile(candidate)
    response.setHeader('Content-Type', candidate.endsWith('.js') ? 'text/javascript' : candidate.endsWith('.css') ? 'text/css' : 'text/html')
    response.end(body)
  } catch { response.writeHead(404); response.end() }
}).listen(port, '127.0.0.1', () => console.log(`UI FIXTURE ONLY: http://127.0.0.1:${port}`))
