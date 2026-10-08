import { afterEach, expect, it, vi } from 'vitest'
import { cleanup, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { Settings } from '../src/Settings'
import { api } from '../src/api'
import { mockService } from './fixtures'

const access = { revision: 0, repositories: [{ name: 'ebarti/JobCtrl', allowed: true }] }
const service = { ...mockService, capacity: { active: 0, limit: 2 }, repository_access: access }
afterEach(() => { cleanup(); vi.restoreAllMocks() })

it('shows one configurable identity, clear parallel-agent wording, and saves permission', async () => {
  const user = userEvent.setup()
  const updated = { revision: 1, repositories: [{ name: 'ebarti/JobCtrl', allowed: false }] }
  const save = vi.spyOn(api, 'saveRepositoryAccess').mockResolvedValue(updated)
  const onSaved = vi.fn()
  render(<Settings service={service} loading={false} error="" onRefresh={vi.fn()} onRepositoryAccessSaved={onSaved} />)
  expect(screen.getAllByRole('checkbox')).toHaveLength(1)
  expect(screen.getByText('0 slots in use · maximum 2')).toBeTruthy()
  expect(screen.queryByText(/Unknown queued|fixture-repo|fixture-sha/)).toBeNull()
  const button = screen.getByRole('button', { name: 'Save repository access' })
  expect((button as HTMLButtonElement).disabled).toBe(true)
  await user.click(screen.getByRole('checkbox', { name: 'ebarti/JobCtrl' }))
  await user.click(button)
  await waitFor(() => expect(onSaved).toHaveBeenCalledWith(updated))
  expect(save).toHaveBeenCalledWith({ expected_revision: 0, allowed_repositories: [] })
  expect(screen.getByRole('status').textContent).toContain('saved')
})

it('shows save errors and keeps the requested change available for correction', async () => {
  const user = userEvent.setup()
  vi.spyOn(api, 'saveRepositoryAccess').mockRejectedValue(new Error('Repository access changed. Refresh Settings before saving again.'))
  render(<Settings service={service} loading={false} error="" onRefresh={vi.fn()} onRepositoryAccessSaved={vi.fn()} />)
  await user.click(screen.getByRole('checkbox', { name: 'ebarti/JobCtrl' }))
  await user.click(screen.getByRole('button', { name: 'Save repository access' }))
  expect((await screen.findByRole('alert')).textContent).toContain('Refresh Settings')
  expect((screen.getByRole('checkbox') as HTMLInputElement).checked).toBe(false)
})
