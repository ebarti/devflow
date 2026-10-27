import { afterEach, expect, it, vi } from 'vitest'
import { cleanup, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { api } from '../src/api'
import type { PauseControl } from '../src/api'
import { PauseControls } from '../src/PauseControls'

const running: PauseControl = { root_id: 'r', desired: 'running', observed: 'running', revision: 0, epoch: 0, blockers: [], notification_pending: false, resume_authorization: 'checked when requested', pause_eligible: true }
afterEach(() => { cleanup(); sessionStorage.clear(); vi.restoreAllMocks() })

it('retains exactly the same command after an unknown outcome and reopening', async () => {
  vi.spyOn(api, 'pauseControl').mockResolvedValue(running)
  const send = vi.spyOn(api, 'pauseCommand').mockRejectedValueOnce(new Error('lost'))
    .mockResolvedValueOnce({ state: 'pause_requested' })
  const first = render(<PauseControls runId="r" />)
  await userEvent.setup().click(await screen.findByRole('button', { name: 'Request pause' }))
  await screen.findByText(/outcome is unknown/)
  const original = send.mock.calls[0][1]
  expect(original.action).toBe('pause')
  expect(original.expected_revision).toBe(0)
  first.unmount()
  render(<PauseControls runId="r" />)
  await userEvent.setup().click(await screen.findByRole('button', { name: 'Retry same pause request' }))
  await waitFor(() => expect(send).toHaveBeenCalledTimes(2))
  expect(send.mock.calls[1][1]).toEqual(original)
  await waitFor(() => expect(sessionStorage.getItem('devflow.pause:r')).toBeNull())
})

it('distinguishes requested work from paused, and shows resume blockers', async () => {
  vi.spyOn(api, 'pauseControl').mockResolvedValue({ ...running, desired: 'paused', observed: 'pause_requested', revision: 1, epoch: 1, notification_pending: true })
  const view = render(<PauseControls runId="r" />)
  await screen.findByText(/The pause is not complete yet/)
  expect(screen.queryByRole('button', { name: 'Request resume' })).toBeNull()
  view.unmount()
  vi.mocked(api.pauseControl).mockResolvedValue({ ...running, desired: 'paused', observed: 'paused', revision: 3, epoch: 1, blockers: ['checkpoint requires reconciliation'] })
  render(<PauseControls runId="r" />)
  await screen.findByText('checkpoint requires reconciliation')
  expect((screen.getByRole('button', { name: 'Request resume' }) as HTMLButtonElement).disabled).toBe(true)
})

it('does not send a command if its frozen intent cannot be retained', async () => {
  vi.spyOn(api, 'pauseControl').mockResolvedValue(running)
  const send = vi.spyOn(api, 'pauseCommand')
  vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => { throw new Error('storage denied') })
  render(<PauseControls runId="r" />)
  await userEvent.setup().click(await screen.findByRole('button', { name: 'Request pause' }))
  await screen.findByText(/could not be retained/)
  expect(send).not.toHaveBeenCalled()
})

it('offers intake pause only when the service reports a queued owner eligible', async () => {
  vi.spyOn(api, 'pauseControl').mockResolvedValue({ ...running, pause_eligible: true })
  const send = vi.spyOn(api, 'pauseCommand').mockResolvedValue({ state: 'pause_requested' })
  render(<PauseControls runId="r" />)
  await screen.findByText(/Only queued runs whose workflow has not started/)
  await userEvent.setup().click(await screen.findByRole('button', { name: 'Request pause' }))
  expect(send).toHaveBeenCalledWith('r', expect.objectContaining({ action: 'pause', expected_revision: 0 }))
})

it.each([false, undefined])('does not offer intake pause without positive eligibility (%s)', async pause_eligible => {
  vi.spyOn(api, 'pauseControl').mockResolvedValue({ ...running, pause_eligible })
  render(<PauseControls runId="r" />)
  await screen.findByText(/Running · Epoch 0/)
  expect(screen.queryByRole('button', { name: 'Request pause' })).toBeNull()
})

it('retains intake pause history and resume controls when new pause is ineligible', async () => {
  vi.spyOn(api, 'pauseControl').mockResolvedValue({ ...running, observed: 'paused', desired: 'paused',
    revision: 3, epoch: 1, pause_eligible: false })
  render(<PauseControls runId="r" />)
  await screen.findByText(/Paused · Epoch 1 · Revision 3/)
  expect(screen.queryByRole('button', { name: 'Request pause' })).toBeNull()
  expect(screen.getByRole('button', { name: 'Request resume' })).toBeTruthy()
})
