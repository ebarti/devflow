import { afterEach, expect, it, vi } from 'vitest'
import { cleanup, render, screen } from '@testing-library/react'
import { api } from '../src/api'
import { RunDetails } from '../src/RunDetails'
import { mockRun } from './fixtures'

afterEach(() => { cleanup(); sessionStorage.clear(); vi.restoreAllMocks() })

it('shows intake controls for the exact version and retains them after start', async () => {
  const status = vi.spyOn(api, 'pauseControl').mockResolvedValue({ root_id: mockRun.id,
    desired: 'running', observed: 'running', revision: 2, epoch: 1,
    blockers: [], notification_pending: false, resume_authorization: 'checked when requested',
    pause_eligible: false })
  render(<RunDetails run={{ ...mockRun, pause_intake_version: 1, execution_state: 'running' }} onRefresh={vi.fn()} />)
  await screen.findByText(/Only queued runs whose workflow has not started/)
  await screen.findByText(/Running · Epoch 1 · Revision 2/)
  expect(status).toHaveBeenCalledWith(mockRun.id)
  expect(screen.queryByRole('button', { name: 'Request pause' })).toBeNull()
})

it('does not show controls without an admitted version', () => {
  const status = vi.spyOn(api, 'pauseControl')
  render(<RunDetails run={mockRun} onRefresh={vi.fn()} />)
  expect(screen.queryByRole('heading', { name: 'Pause and resume' })).toBeNull()
  expect(status).not.toHaveBeenCalled()
})

it('does not offer controls for an unsupported intake version', () => {
  const status = vi.spyOn(api, 'pauseControl')
  render(<RunDetails run={{ ...mockRun, pause_intake_version: 2 }} onRefresh={vi.fn()} />)
  expect(screen.queryByRole('heading', { name: 'Pause and resume' })).toBeNull()
  expect(status).not.toHaveBeenCalled()
})
