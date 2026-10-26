import { afterEach, expect, it, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { api, ApiError, type DurableOperation } from '../src/api'
import { RunDetails } from '../src/RunDetails'
import { mockRun } from './fixtures'

afterEach(() => { vi.restoreAllMocks(); sessionStorage.clear() })
function reply(envelope: { operation_id: string; run_id: string | null; kind: string; request: object }, state: DurableOperation['state']): DurableOperation {
  return { ...envelope, stage: 'execution', state, progress: state, next_action: state === 'unknown' ? 'reconcile' : 'wait',
    result: state === 'succeeded' ? { run_id: envelope.run_id, phase: 'cancelling', revision: 6 } : null,
    error: state === 'refused' ? 'stale run revision' : null, authority: null } as DurableOperation
}

it('retains cancellation through lost response, pending operation and a revision update', async () => {
  const create = vi.spyOn(api, 'createOperation')
    .mockRejectedValueOnce(new ApiError('response lost', 0))
    .mockImplementationOnce(async envelope => reply(envelope, 'running'))
    .mockImplementationOnce(async envelope => reply(envelope, 'succeeded'))
  vi.spyOn(api, 'runOperations').mockResolvedValue({ operations: [] })
  const run = { ...mockRun, decisions: [] }
  vi.spyOn(api, 'getRun').mockResolvedValue({ ...run, protocol_revision: 10 })
  const refresh = vi.fn().mockResolvedValue(undefined)
  const user = userEvent.setup()
  const { rerender } = render(<RunDetails run={run} onRefresh={refresh} />)
  await user.click(screen.getByRole('button', { name: 'Cancel run' }))
  await user.type(screen.getByLabelText('Reason for cancellation'), 'Stop this run')
  await user.click(screen.getByRole('button', { name: 'Request cancellation' }))
  expect(await screen.findByText(/cancellation outcome is unknown/i)).toBeTruthy()
  rerender(<RunDetails run={{ ...run, protocol_revision: 9 }} onRefresh={refresh} />)
  await user.click(screen.getByRole('button', { name: 'Retry cancellation request' }))
  await waitFor(() => expect(create).toHaveBeenCalledTimes(2))
  expect((screen.getByLabelText('Reason for cancellation') as HTMLInputElement).disabled).toBe(true)
  await user.click(screen.getByRole('button', { name: 'Retry cancellation request' }))
  await waitFor(() => expect(create).toHaveBeenCalledTimes(3))
  expect(create.mock.calls[1]).toEqual(create.mock.calls[0])
  expect(create.mock.calls[2]).toEqual(create.mock.calls[0])
  expect(screen.queryByLabelText('Reason for cancellation')).toBeNull()
})

it('only durable refusal releases the intent after a post-response run read', async () => {
  const create = vi.spyOn(api, 'createOperation')
    .mockImplementationOnce(async envelope => reply(envelope, 'refused'))
    .mockImplementationOnce(async envelope => reply(envelope, 'succeeded'))
  vi.spyOn(api, 'runOperations').mockResolvedValue({ operations: [] })
  const run = { ...mockRun, decisions: [] }
  const read = vi.spyOn(api, 'getRun').mockRejectedValue(new ApiError('read unavailable', 503))
  const refresh = vi.fn().mockResolvedValue(undefined)
  const user = userEvent.setup()
  const { rerender } = render(<RunDetails run={run} onRefresh={refresh} />)
  await user.click(screen.getByRole('button', { name: 'Cancel run' }))
  await user.type(screen.getByLabelText('Reason for cancellation'), 'Stop this run')
  await user.click(screen.getByRole('button', { name: 'Request cancellation' }))
  await waitFor(() => expect(refresh).toHaveBeenCalledTimes(1))
  expect((screen.getByRole('button', { name: 'Request cancellation' }) as HTMLButtonElement).disabled).toBe(true)
  read.mockResolvedValue({ ...run, protocol_revision: 7 })
  await user.click(screen.getByRole('button', { name: 'Refresh run state' }))
  await waitFor(() => expect(refresh).toHaveBeenCalledTimes(2))
  rerender(<RunDetails run={{ ...run, protocol_revision: 7 }} onRefresh={refresh} />)
  await user.click(screen.getByRole('button', { name: 'Request cancellation' }))
  await waitFor(() => expect(create).toHaveBeenCalledTimes(2))
  const first = create.mock.calls[0][0].request as { command_id: string; expected_revision: number }
  const second = create.mock.calls[1][0].request as { command_id: string; expected_revision: number }
  expect(second.command_id).not.toBe(first.command_id)
  expect(second.expected_revision).toBe(7)
})


it('restores the exact cancellation after unmount and replays the same durable operation', async () => {
  const create = vi.spyOn(api, 'createOperation')
    .mockRejectedValueOnce(new ApiError('response lost', 0))
    .mockImplementationOnce(async envelope => reply(envelope, 'succeeded'))
  vi.spyOn(api, 'runOperations').mockResolvedValue({ operations: [] })
  const run = { ...mockRun, decisions: [] }
  vi.spyOn(api, 'getRun').mockResolvedValue(run)
  const refresh = vi.fn().mockResolvedValue(undefined)
  const user = userEvent.setup()
  const view = render(<RunDetails run={run} onRefresh={refresh} />)
  await user.click(screen.getByRole('button', { name: 'Cancel run' }))
  await user.type(screen.getByLabelText('Reason for cancellation'), 'Stop this run')
  await user.click(screen.getByRole('button', { name: 'Request cancellation' }))
  await screen.findByText(/cancellation outcome is unknown/i)
  view.unmount()
  render(<RunDetails run={{ ...run, protocol_revision: 9 }} onRefresh={refresh} />)
  expect((screen.getByLabelText('Reason for cancellation') as HTMLInputElement).value).toBe('Stop this run')
  await user.click(screen.getByRole('button', { name: 'Retry cancellation request' }))
  await waitFor(() => expect(create).toHaveBeenCalledTimes(2))
  expect(create.mock.calls[1]).toEqual(create.mock.calls[0])
  expect(sessionStorage.getItem(`devflow.cancel:${run.id}`)).toBeNull()
})

it('does not send a cancellation when the request cannot be retained', async () => {
  const create = vi.spyOn(api, 'createOperation')
  vi.spyOn(api, 'runOperations').mockResolvedValue({ operations: [] })
  const run = { ...mockRun, decisions: [] }
  const user = userEvent.setup()
  render(<RunDetails run={run} onRefresh={vi.fn().mockResolvedValue(undefined)} />)
  await user.click(screen.getByRole('button', { name: 'Cancel run' }))
  await user.type(screen.getByLabelText('Reason for cancellation'), 'Stop this run')
  const write = vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => { throw new Error('storage unavailable') })
  await user.click(screen.getByRole('button', { name: 'Request cancellation' }))
  expect(await screen.findByText(/could not be retained/i)).toBeTruthy()
  expect(create).not.toHaveBeenCalled()
  write.mockRestore()
})
