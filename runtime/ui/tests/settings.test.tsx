import { afterEach, expect, it, vi } from 'vitest'
import { act, cleanup, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { Settings } from '../src/Settings'
import { App } from '../src/App'
import { api } from '../src/api'
import { mockService } from './fixtures'

const access = { revision: 0, repositories: [{ name: 'ebarti/JobCtrl', allowed: true }] }
const service = { ...mockService, capacity: { active: 0, limit: 2 }, repository_access: access, repositories: [{ key: 'jobctrl', github_repo: 'ebarti/JobCtrl', label: 'JobCtrl', base_ref: 'HEAD' }] }
afterEach(() => { cleanup(); vi.restoreAllMocks(); window.history.replaceState({}, '', '/') })

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

it('keeps saved permissions when an older service refresh finishes afterward', async () => {
  const user = userEvent.setup()
  window.history.replaceState({}, '', '/settings')
  let resolveRefresh!: (value: typeof service) => void
  const delayedRefresh = new Promise<typeof service>(resolve => { resolveRefresh = resolve })
  vi.spyOn(api, 'getService').mockResolvedValueOnce(service).mockReturnValueOnce(delayedRefresh)
  vi.spyOn(api, 'listRunsPage').mockResolvedValue({ runs: [], next_cursor: null })
  vi.spyOn(api, 'saveRepositoryAccess').mockResolvedValue({ revision: 1, repositories: [{ name: 'ebarti/JobCtrl', allowed: false }] })
  render(<App />)
  await user.click(await screen.findByRole('checkbox', { name: 'ebarti/JobCtrl' }))
  await user.click(screen.getByRole('button', { name: 'Refresh service information' }))
  await user.click(screen.getByRole('button', { name: 'Save repository access' }))
  await screen.findByText('Repository access saved.')
  await act(async () => { resolveRefresh({ ...service }) })
  expect((screen.getByRole('checkbox') as HTMLInputElement).checked).toBe(false)
  await user.click(screen.getByRole('button', { name: 'New run' }))
  expect(screen.getByRole('heading', { name: 'No repositories available' })).toBeTruthy()
})

it('keeps a newer permission refresh when an older save acknowledgement finishes afterward', async () => {
  const user = userEvent.setup()
  window.history.replaceState({}, '', '/settings')
  let resolveSave!: (value: { revision: number; repositories: { name: string; allowed: boolean }[] }) => void
  const delayedSave = new Promise<{ revision: number; repositories: { name: string; allowed: boolean }[] }>(resolve => { resolveSave = resolve })
  vi.spyOn(api, 'getService').mockResolvedValueOnce(service).mockResolvedValueOnce({ ...service, repository_access: { ...access, revision: 2 } })
  vi.spyOn(api, 'listRunsPage').mockResolvedValue({ runs: [], next_cursor: null })
  const save = vi.spyOn(api, 'saveRepositoryAccess').mockReturnValueOnce(delayedSave)
  render(<App />)
  await user.click(await screen.findByRole('checkbox', { name: 'ebarti/JobCtrl' }))
  await user.click(screen.getByRole('button', { name: 'Save repository access' }))
  await waitFor(() => expect(save).toHaveBeenCalledOnce())
  await user.click(screen.getByRole('button', { name: 'Refresh service information' }))
  await waitFor(() => expect((screen.getByRole('checkbox') as HTMLInputElement).checked).toBe(true))
  await act(async () => { resolveSave({ revision: 1, repositories: [{ name: 'ebarti/JobCtrl', allowed: false }] }) })
  await screen.findByText('Repository access saved.')
  expect((screen.getByRole('checkbox') as HTMLInputElement).checked).toBe(true)
  await user.click(screen.getByRole('button', { name: 'New run' }))
  expect(screen.getByRole('option', { name: 'JobCtrl' })).toBeTruthy()
})
