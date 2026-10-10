import { afterEach, expect, it, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { api, ApiError } from '../src/api'
import { FeatureDelivery } from '../src/FeatureDelivery'
import type { RunDetail } from '../src/model'
import { mockRun } from './fixtures'

afterEach(() => vi.restoreAllMocks())

const feature: RunDetail = {
  ...mockRun, projection_revision: 12, outcome: 'blocked',
  feature_delivery: {
    issue_id: 'I_feature', owner: { issue_id: 'I_feature', run_id: 'fixture-run', store_path: '/store', generation: 1 },
    ownership_state: 'stopped', can_continue: true,
    github_plan_url: 'https://github.com/example/repository/issues/1#issuecomment-22',
    repair_budget: { used: 4, maximum: 10, learning_required: false },
    workers: [{ run_id: 'worker-api', chunk_id: 'api1', workstream_id: 'api', issue_url: 'https://github.com/example/repository/issues/10', kind: 'chunk', phase: 'blocked', outcome: 'blocked', cleanup: 'confirmed', pull_request: { number: 20, url: 'https://github.com/example/repository/pull/20' } }],
  },
  feature_plan: {
    plan_identity: { plan_revision: 2, plan_digest: 'a'.repeat(64), comment_id: 22, comment_node_id: 'IC_22', workstream_issues: { api: { id: 'I_10', number: 10, url: 'https://github.com/example/repository/issues/10' } } },
    phase: 'adopted', reason: 'Browser selector belongs to the later web chunk.',
    evidence: [{ path: '/owned/gates/missing-selector.json', sha256: 'b'.repeat(64) }], affected_chunks: ['api1', 'web1'],
    repair_budget: { used: 4, maximum: 10, learning_required: false }, can_revise: true, reason_ineligible: null, expected_revision: 12,
    child_plan_links: { api: { comment_id: 33, comment_node_id: 'IC_33', url: 'https://github.com/example/repository/issues/10#issuecomment-33', digest: 'c'.repeat(64) } },
    authority: { version: 0, allowed_roots: [], allowed_files: ['api.py', 'web.ts'], protected_paths: [] },
    expected_paths: { api1: ['api.py'], web1: ['web.ts'] },
  },
}

it('renders the adopted revision, exact child ownership, affected chunks and frozen scope', async () => {
  const user = userEvent.setup()
  render(<FeatureDelivery run={feature} onRefresh={vi.fn()} />)
  expect(screen.getByText('GitHub plan revision 2 · Adopted')).toBeTruthy()
  expect(screen.getByText('Affected chunks: api1, web1.')).toBeTruthy()
  expect(screen.getByRole('link', { name: 'api workstream plan' }).getAttribute('href')).toBe('https://github.com/example/repository/issues/10#issuecomment-33')
  expect(screen.getByRole('link', { name: 'api' }).getAttribute('href')).toBe('https://github.com/example/repository/issues/10')
  await user.click(screen.getByText('Execution scope and expected files'))
  expect(screen.getByText(/Allowed files: api.py, web.ts/)).toBeTruthy()
  expect(screen.getByText('api1: api.py')).toBeTruthy()
  expect(screen.getByText(/Product repairs: 4 of 10/)).toBeTruthy()
})

it('requests only a bounded reason and observed revision, then links the same feature successor', async () => {
  const request = vi.spyOn(api, 'reviseFeaturePlan').mockResolvedValue({ run_id: 'next-feature', phase: 'queued', revision_phase: 'requested' })
  const refresh = vi.fn().mockResolvedValue(undefined)
  const user = userEvent.setup()
  render(<FeatureDelivery run={feature} onRefresh={refresh} />)
  const button = screen.getByRole('button', { name: 'Request plan revision' })
  expect((button as HTMLButtonElement).disabled).toBe(true)
  await user.type(screen.getByLabelText('Planning defect'), 'The API gate selects a browser file owned by web1.')
  await user.click(button)
  await waitFor(() => expect(refresh).toHaveBeenCalledOnce())
  const body = request.mock.calls[0][1]
  expect(Object.keys(body).sort()).toEqual(['command_id', 'expected_revision', 'reason'])
  expect(body.expected_revision).toBe(12)
  expect(body.reason).toBe('The API gate selects a browser file owned by web1.')
  expect(screen.getByRole('status').textContent).toContain('Plan revision requested.')
  expect(screen.getByRole('link', { name: 'Open the coordinating run' }).getAttribute('href')).toBe('/runs/next-feature')
})

it('preserves the full command after an uncertain response even if local eligibility changes', async () => {
  const request = vi.spyOn(api, 'reviseFeaturePlan')
    .mockRejectedValueOnce(new ApiError('Response unavailable', 503))
    .mockResolvedValueOnce({ run_id: 'next-feature', phase: 'queued', revision_phase: 'requested' })
  const refresh = vi.fn().mockResolvedValue(undefined)
  const user = userEvent.setup()
  const { rerender } = render(<FeatureDelivery run={feature} onRefresh={refresh} />)
  await user.type(screen.getByLabelText('Planning defect'), 'The selector belongs to web1.')
  await user.click(screen.getByRole('button', { name: 'Request plan revision' }))
  await screen.findByRole('alert')
  expect((screen.getByLabelText('Planning defect') as HTMLTextAreaElement).disabled).toBe(true)
  rerender(<FeatureDelivery run={{ ...feature, feature_plan: { ...feature.feature_plan!, expected_revision: 14, can_revise: false, reason_ineligible: 'owner changed' } }} onRefresh={refresh} />)
  await user.click(screen.getByRole('button', { name: 'Retry plan revision request' }))
  await waitFor(() => expect(refresh).toHaveBeenCalledOnce())
  expect(request.mock.calls[1]).toEqual(request.mock.calls[0])
  expect(request.mock.calls[1][1].expected_revision).toBe(12)
})

it('refreshes a rejected stale request before binding a new command', async () => {
  const request = vi.spyOn(api, 'reviseFeaturePlan')
    .mockRejectedValueOnce(new ApiError('Stale projection revision', 409))
    .mockResolvedValueOnce({ run_id: 'next-feature', phase: 'queued', revision_phase: 'requested' })
  const refresh = vi.fn().mockResolvedValue(undefined)
  const user = userEvent.setup()
  const { rerender } = render(<FeatureDelivery run={feature} onRefresh={refresh} />)
  await user.type(screen.getByLabelText('Planning defect'), 'The selector belongs to web1.')
  await user.click(screen.getByRole('button', { name: 'Request plan revision' }))
  await waitFor(() => expect(refresh).toHaveBeenCalledOnce())
  rerender(<FeatureDelivery run={{ ...feature, feature_plan: { ...feature.feature_plan!, expected_revision: 14 } }} onRefresh={refresh} />)
  await user.click(screen.getByRole('button', { name: 'Request plan revision' }))
  await waitFor(() => expect(refresh).toHaveBeenCalledTimes(2))
  expect(request.mock.calls[1][1].command_id).not.toBe(request.mock.calls[0][1].command_id)
  expect(request.mock.calls[1][1].expected_revision).toBe(14)
})


it('describes fresh intake as unplanned until a GitHub revision is published', () => {
  render(<FeatureDelivery run={{ ...feature, feature_plan: { ...feature.feature_plan!,
    plan_identity: { ...feature.feature_plan!.plan_identity, comment_id: null, comment_node_id: null, plan_digest: null },
    phase: 'unplanned', reason: null, affected_chunks: [], evidence: [], child_plan_links: {}, expected_paths: {},
    can_revise: false, reason_ineligible: 'An accepted feature plan is required.',
  } }} onRefresh={vi.fn()} />)
  expect(screen.getByText('Plan state: Unplanned.')).toBeTruthy()
  expect(screen.queryByText(/GitHub plan revision/)).toBeNull()
  expect(screen.queryByRole('button', { name: 'Request plan revision' })).toBeNull()
})
