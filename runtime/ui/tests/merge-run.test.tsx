import { afterEach, expect, it, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { api, ApiError } from '../src/api'
import { RunDetails } from '../src/RunDetails'
import { runStatus } from '../src/runStatus'
import { mockRun } from './fixtures'
import type { RunDetail } from '../src/model'

afterEach(() => vi.restoreAllMocks())

const delivered: RunDetail = { ...mockRun, title: 'JobCtrl #1042 · Preview exact cover-letter PDFs',
  decisions: [], phase: 'delivered', execution_state: 'terminal', outcome: 'delivered',
  merge: { available: true, phase: 'available', candidate_id: 'candidate', head: 'head', pr_number: 1109 },
}

it('offers an ordinary delivered PR merge without a pending feature decision', async () => {
  const merge = vi.spyOn(api, 'merge').mockResolvedValue({ run_id: delivered.id, phase: 'queued', command_id: 'accepted' })
  const refresh = vi.fn().mockResolvedValue(undefined)
  render(<RunDetails run={delivered} onRefresh={refresh} />)
  expect(screen.getByRole('heading', { level: 1 }).textContent).toContain('JobCtrl #1042')
  expect(screen.getByText(/reconcile changes from main/)).toBeTruthy()
  expect(merge).not.toHaveBeenCalled()
  await userEvent.click(screen.getByRole('button', { name: 'Merge PR #1109' }))
  await waitFor(() => expect(refresh).toHaveBeenCalledOnce())
  expect(merge.mock.calls[0][1]).toMatchObject({ expected_revision: 5, candidate_id: 'candidate', head: 'head', pr_number: 1109, answer: 'merge' })
})

it('retries a lost acknowledgement with the same authority even after an SSE update', async () => {
  const merge = vi.spyOn(api, 'merge').mockRejectedValueOnce(new ApiError('Response lost', 0))
    .mockResolvedValue({ run_id: delivered.id, phase: 'queued', command_id: 'accepted' })
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
