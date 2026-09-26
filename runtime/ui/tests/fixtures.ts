/** Mock-only UI test data. Never imported by src/ or the production bundle. */
import type { RunDetail, ServiceInfo } from '../src/model'

export const mockRun: RunDetail = {
  id: 'fixture-run', run_id: 'fixture-run', work_id: 'fixture-work',
  title: 'Fixture workflow', repository: 'fixture/repository', issue: '#1',
  issue_url: 'https://github.com/example/repository/issues/1',
  phase: 'verify', execution_state: 'running', revision: 7,
  authorized_endpoint: 'published_unmerged', observed_at: '2026-01-01T12:00:00Z',
  sequence: 4,
  phase_gates: [
    { id: 'prepare', label: 'Prepare', state: 'passed' },
    { id: 'implement', label: 'Implement', state: 'passed' },
    { id: 'review', label: 'Review', state: 'passed' },
    { id: 'verify', label: 'Verify', state: 'running' },
    { id: 'ci', label: 'CI', state: 'pending' },
    { id: 'deliver', label: 'Deliver', state: 'pending' },
  ],
  roles: [{ role: 'verify', state: 'running', session_id: 'fixture-session', usage: { status: 'unknown' } }],
  decisions: [{ id: 'fixture-decision', revision: 3, candidate_revision: 2, prompt: 'Continue this fixture run?', options: [{ value: 'yes', label: 'Yes' }, { value: 'no', label: 'No' }], state: 'pending' }],
  events: [{ sequence: 4, timestamp: '2026-01-01T12:00:00Z', type: 'verify_started', message: 'Fixture verification started' }],
}

export const mockService: ServiceInfo = {
  status: 'running', version: 'fixture-only',
  repositories: [{ key: 'fixture-repo', label: 'Fixture repository', base_ref: 'main', base_sha: 'fixture-sha', recovery_keys: ['fixture-recovery'] }],
  policy: { roles: { implement: { model: 'fixture-model', effort: 'fixture-effort' } } },
}
