import { afterEach, expect, it, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { api, ApiError } from '../src/api'
import { RunDetails } from '../src/RunDetails'
import type { RunDetail } from '../src/model'
import { mockRun } from './fixtures'

afterEach(() => vi.restoreAllMocks())

const feature: RunDetail = {
  ...mockRun, projection_revision: 12, decisions: [], outcome: 'blocked',
  feature_delivery: {
    issue_id: 'I_feature', owner: { issue_id: 'I_feature', run_id: 'fixture-run', store_path: '/store', generation: 1 },
    ownership_state: 'stopped', can_continue: true,
    github_plan_url: 'https://github.com/example/repository/issues/1#issuecomment-22',
    repair_budget: { used: 5, maximum: 10, learning_required: true },
    workers: [{ run_id: 'worker-one', chunk_id: 'model', kind: 'chunk', phase: 'delivered', outcome: 'delivered', cleanup: 'confirmed', pull_request: { number: 20, url: 'https://github.com/example/repository/pull/20' } }],
  },
}

it('shows GitHub definition, cumulative repairs, and complete chunk evidence', () => {
  render(<RunDetails run={feature} onRefresh={vi.fn()} />)
  expect(screen.getByRole('link', { name: 'Accepted plan and stack on GitHub' }).getAttribute('href')).toContain('#issuecomment-22')
  expect(screen.getByText(/Product repairs: 5 of 10/)).toBeTruthy()
  expect(screen.getByText(/Learning required/)).toBeTruthy()
  expect(screen.getByRole('link', { name: 'model' }).getAttribute('href')).toBe('/runs/worker-one')
  expect(screen.queryByLabelText('Workflow phase gates')).toBeNull()
})

it('retries a lost continuation response with the original identity and revision', async () => {
  const continueFeature = vi.spyOn(api, 'continueFeature')
    .mockRejectedValueOnce(new ApiError('Response unavailable', 503))
    .mockResolvedValueOnce({ run_id: 'next-run', phase: 'queued' })
  const refresh = vi.fn().mockResolvedValue(undefined)
  const user = userEvent.setup()
  const { rerender } = render(<RunDetails run={feature} onRefresh={refresh} />)
  await user.click(screen.getByRole('button', { name: 'Continue this feature' }))
  await screen.findByRole('alert')
  rerender(<RunDetails run={{ ...feature, projection_revision: 14 }} onRefresh={refresh} />)
  await user.click(screen.getByRole('button', { name: 'Continue this feature' }))
  await waitFor(() => expect(refresh).toHaveBeenCalledOnce())
  expect(continueFeature.mock.calls[1]).toEqual(continueFeature.mock.calls[0])
  expect(continueFeature.mock.calls[0][1].expected_revision).toBe(12)
  expect(screen.getByRole('link', { name: 'Open the coordinating run' }).getAttribute('href')).toBe('/runs/next-run')
})

it('requires a selected merge instruction for the displayed stack revision', async () => {
  const answer = vi.spyOn(api, 'answer').mockResolvedValue(undefined)
  const refresh = vi.fn().mockResolvedValue(undefined)
  const user = userEvent.setup()
  render(<RunDetails run={{ ...feature, outcome: null, phase: 'awaiting_merge',
    pull_request: { stack_id: 42, scope_complete: true, pull_requests: [
      { number: 20, url: 'https://github.com/example/repository/pull/20', chunk_id: 'model' },
      { number: 21, url: 'https://github.com/example/repository/pull/21', chunk_id: 'client' },
    ] },
    decisions: [{ id: 'fixture-run:merge:4', revision: 4, kind: 'merge',
      candidate_revision: 1, prompt: 'Merge this feature?', options: ['merge'], state: 'pending' }],
  }} onRefresh={refresh} />)
  expect(screen.getByText(/stack #42 with #20, #21/)).toBeTruthy()
  expect((screen.getByRole('button', { name: 'Merge this feature' }) as HTMLButtonElement).disabled).toBe(true)
  await user.click(screen.getByRole('radio', { name: 'Merge' }))
  await user.click(screen.getByRole('button', { name: 'Merge this feature' }))
  await waitFor(() => expect(refresh).toHaveBeenCalledOnce())
  expect(answer.mock.calls[0][1]).toMatchObject({ answer: 'merge', decision_id: 'fixture-run:merge:4', decision_revision: 4, candidate_revision: 1 })
})
