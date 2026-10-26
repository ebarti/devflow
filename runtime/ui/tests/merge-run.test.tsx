import { afterEach, expect, it, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { api, ApiError } from '../src/api'
import { RunDetails } from '../src/RunDetails'
import { runStatus } from '../src/runStatus'
import { mockRun } from './fixtures'
import type { RunDetail } from '../src/model'

afterEach(() => vi.restoreAllMocks())

const completed = { operation_id: 'merge:done', run_id: 'run-1', kind: 'merge', stage: 'admission', state: 'succeeded' as const, progress: 'succeeded', next_action: 'inspect_result', result: { phase: 'queued' }, error: null, authority: 'current_authority_unverified' }

const delivered: RunDetail = { ...mockRun, title: 'JobCtrl #1042 · Preview exact cover-letter PDFs',
  decisions: [], phase: 'delivered', execution_state: 'terminal', outcome: 'delivered',
  merge: { available: true, phase: 'available', candidate_id: 'candidate', head: 'head', pr_number: 1109 },
}

it('offers an ordinary delivered PR merge without a pending feature decision', async () => {
  const merge = vi.spyOn(api, 'createOperation').mockResolvedValue(completed)
  const refresh = vi.fn().mockResolvedValue(undefined)
  render(<RunDetails run={delivered} onRefresh={refresh} />)
  expect(screen.getByRole('heading', { level: 1 }).textContent).toContain('JobCtrl #1042')
  expect(screen.getByText(/reconcile changes from main/)).toBeTruthy()
  expect(merge).not.toHaveBeenCalled()
  await userEvent.click(screen.getByRole('button', { name: 'Merge PR #1109' }))
  await waitFor(() => expect(refresh).toHaveBeenCalledOnce())
  expect(merge.mock.calls[0][0].request).toMatchObject({ expected_revision: 5, candidate_id: 'candidate', head: 'head', pr_number: 1109, answer: 'merge' })
})

it('retries a lost acknowledgement with the same authority even after an SSE update', async () => {
  const merge = vi.spyOn(api, 'createOperation').mockRejectedValueOnce(new ApiError('Response lost', 0))
    .mockResolvedValue(completed)
  const refresh = vi.fn().mockResolvedValue(undefined)
  const { rerender } = render(<RunDetails run={delivered} onRefresh={refresh} />)
  await userEvent.click(screen.getByRole('button', { name: 'Merge PR #1109' }))
  await waitFor(() => expect(refresh).toHaveBeenCalledOnce())
  rerender(<RunDetails run={{ ...delivered, protocol_revision: 9, merge: { available: false, phase: 'queued' } }} onRefresh={refresh} />)
  await userEvent.click(screen.getByRole('button', { name: 'Retry same merge request' }))
  await waitFor(() => expect(merge).toHaveBeenCalledTimes(2))
  expect(merge.mock.calls[1]).toEqual(merge.mock.calls[0])
})

it.each(['queued', 'waiting_ci', 'decision_required', 'unknown', 'merged'])('shows merge %s without offering a second merge', phase => {
  const run = { ...delivered, merge: { available: false, phase, result: { state: phase, reason: 'Observed result', merged_commit: phase === 'merged' ? 'merged-sha' : undefined } } }
  render(<RunDetails run={run} onRefresh={vi.fn()} />)
  expect(screen.queryByRole('button', { name: /Merge PR/ })).toBeNull()
  expect(runStatus(run)).not.toBe('Delivered')
})

it('hides merge controls from an ineligible ordinary run', () => {
  render(<RunDetails run={{ ...mockRun, merge: { available: false, phase: 'unavailable' } }} onRefresh={vi.fn()} />)
  expect(screen.queryByRole('button', { name: /Merge PR/ })).toBeNull()
})

it('continues a historical textual conflict under its existing merge authority', async () => {
  const merge = vi.spyOn(api, 'createOperation').mockResolvedValue(completed)
  const refresh = vi.fn().mockResolvedValue(undefined)
  render(<RunDetails run={{ ...delivered, merge: { ...delivered.merge!, resume: true, phase: 'decision_required' } }} onRefresh={refresh} />)
  await userEvent.click(screen.getByRole('button', { name: 'Continue merge reconciliation' }))
  await waitFor(() => expect(refresh).toHaveBeenCalledOnce())
  expect(merge.mock.calls[0][0].request).toMatchObject({ candidate_id: 'candidate', head: 'head', pr_number: 1109 })
})

it('shows the exact incompatible requirements instead of a generic Git conflict', () => {
  const run = { ...delivered, merge: { available: false, phase: 'decision_required', result: {
    state: 'decision_required', reason: 'divergent_product_direction', decision: {
      question: 'Should this action require confirmation?', feature_requirement: 'Always require confirmation',
      main_requirement: 'Never ask for confirmation', why_incompatible: 'Both requirements apply to the same action', files: ['action.ts'],
    },
  } } }
  render(<RunDetails run={run} onRefresh={vi.fn()} />)
  expect(screen.getByText('Should this action require confirmation?')).toBeTruthy()
  expect(screen.getByText('Feature requirement: Always require confirmation')).toBeTruthy()
  expect(screen.queryByRole('button', { name: /Merge PR/ })).toBeNull()
})


it('retains the original merge intent while the durable operation is pending', async () => {
  const merge = vi.spyOn(api, 'createOperation').mockResolvedValue({ ...completed, state: 'running', result: null, progress: 'running', next_action: 'wait', authority: null })
  vi.spyOn(api, 'runOperations').mockResolvedValue({ operations: [] })
  const refresh = vi.fn().mockResolvedValue(undefined)
  render(<RunDetails run={delivered} onRefresh={refresh} />)
  await userEvent.click(screen.getByRole('button', { name: 'Merge PR #1109' }))
  await screen.findByText(/operation .* is running/i)
  expect(refresh).not.toHaveBeenCalled()
  await userEvent.click(screen.getByRole('button', { name: 'Retry same merge request' }))
  expect(merge.mock.calls[1]).toEqual(merge.mock.calls[0])
})
