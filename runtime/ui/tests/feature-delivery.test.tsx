import { afterEach, expect, it, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { api, ApiError } from '../src/api'
import { RunDetails } from '../src/RunDetails'
import type { RunDetail } from '../src/model'
import { mockRun } from './fixtures'

const completedOperation = (result: unknown = {}) => ({ operation_id: 'fixture-op', run_id: 'fixture-run', kind: 'fixture', stage: 'execution', state: 'succeeded' as const, progress: 'succeeded', next_action: 'inspect_result', result, error: null, authority: 'current_authority_unverified' })

afterEach(() => { vi.restoreAllMocks(); sessionStorage.clear() })

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
  const continueFeature = vi.spyOn(api, 'createOperation')
    .mockRejectedValueOnce(new ApiError('Response unavailable', 503))
    .mockResolvedValueOnce(completedOperation({ run_id: 'next-run', phase: 'queued' }))
  const refresh = vi.fn().mockResolvedValue(undefined)
  const user = userEvent.setup()
  const { rerender } = render(<RunDetails run={feature} onRefresh={refresh} />)
  await user.click(screen.getByRole('button', { name: 'Continue this feature' }))
  await screen.findByRole('alert')
  rerender(<RunDetails run={{ ...feature, projection_revision: 14 }} onRefresh={refresh} />)
  await user.click(screen.getByRole('button', { name: 'Check feature continuation' }))
  await waitFor(() => expect(refresh).toHaveBeenCalledOnce())
  expect(continueFeature.mock.calls[1]).toEqual(continueFeature.mock.calls[0])
  expect((continueFeature.mock.calls[0][0].request as { expected_revision: number }).expected_revision).toBe(12)
  expect(screen.getByRole('link', { name: 'Open the coordinating run' }).getAttribute('href')).toBe('/runs/next-run')
})

it('requires a selected merge instruction for the displayed stack revision', async () => {
  const answer = vi.spyOn(api, 'createOperation').mockResolvedValue(completedOperation())
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
  expect(answer.mock.calls[0][0].request).toMatchObject({ answer: 'merge', decision_id: 'fixture-run:merge:4', decision_revision: 4, candidate_revision: 1 })
})

it('does not announce a successor while feature continuation is pending', async () => {
  const operation = vi.spyOn(api, 'createOperation').mockResolvedValue({ ...completedOperation(), state: 'running', result: null, progress: 'running', next_action: 'wait', authority: null })
  vi.spyOn(api, 'runOperations').mockResolvedValue({ operations: [] })
  const refresh = vi.fn().mockResolvedValue(undefined)
  const user = userEvent.setup()
  const { rerender } = render(<RunDetails run={feature} onRefresh={refresh} />)
  await user.click(screen.getByRole('button', { name: 'Continue this feature' }))
  expect(await screen.findByText(/operation .* is running/i)).toBeTruthy()
  expect(screen.queryByRole('link', { name: 'Open the coordinating run' })).toBeNull()
  expect(refresh).not.toHaveBeenCalled()
  rerender(<RunDetails run={{ ...feature, projection_revision: 99 }} onRefresh={refresh} />)
  await user.click(screen.getByRole('button', { name: 'Check feature continuation' }))
  expect(operation.mock.calls[1]).toEqual(operation.mock.calls[0])
})

it('restores the exact feature continuation after reopening', async () => {
  const operation = vi.spyOn(api, 'createOperation').mockImplementation(async envelope => ({ ...completedOperation(), operation_id: envelope.operation_id, state: 'running', result: null, progress: 'running', next_action: 'wait', authority: null }))
  vi.spyOn(api, 'runOperations').mockResolvedValue({ operations: [] })
  const user = userEvent.setup()
  const view = render(<RunDetails run={feature} onRefresh={vi.fn()} />)
  await user.click(screen.getByRole('button', { name: 'Continue this feature' }))
  expect(await screen.findByText(/operation .* is running/i)).toBeTruthy()
  view.unmount()
  render(<RunDetails run={{ ...feature, projection_revision: 99 }} onRefresh={vi.fn()} />)
  expect(screen.getByText(/previous feature request is retained/i)).toBeTruthy()
  await user.click(screen.getByRole('button', { name: 'Check feature continuation' }))
  expect(operation.mock.calls[1]).toEqual(operation.mock.calls[0])
})
