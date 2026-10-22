import { afterEach, expect, it, vi } from 'vitest'
import { cleanup, render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { App } from '../src/App'
import { api } from '../src/api'
import { RunBoard } from '../src/RunBoard'
import type { ProjectBoard } from '../src/model'
import { mockRun, mockService } from './fixtures'

afterEach(() => { cleanup(); vi.restoreAllMocks(); window.history.replaceState(null, '', '/') })
const project: ProjectBoard = {
  items: [{ id: 'item-1', url: 'https://github.com/example/repository/issues/12',
    title: 'Unstarted backlog work', number: 12, repository: 'example/repository',
    type: 'Issue', state: 'OPEN', status: 'Backlog', priority: 'P1', labels: [],
    checked_at: '2026-10-20T12:00:00Z' }],
  projects: [{ url: 'https://github.com/users/example/projects/1', checked_at: '2026-10-20T12:00:00Z', error: null, state: 'observed' }],
}

it('shows Project backlog without inventing a run or retired status columns', () => {
  const select = vi.fn()
  render(<RunBoard runs={[]} projectBoard={project} archived={false} onSelect={select} onToggle={vi.fn()} onRefresh={vi.fn()} />)
  const lane = screen.getByRole('region', { name: 'Backlog column' })
  expect(within(lane).getByRole('link', { name: /Unstarted backlog work/ }).getAttribute('href')).toBe(project.items[0].url)
  for (const name of ['Delivered', 'Merged', 'Tracking', 'Failed', 'Needs attention']) {
    expect(screen.queryByRole('region', { name: `${name} column` })).toBeNull()
  }
  expect(select).not.toHaveBeenCalled()
})

it('keeps one runtime-owned card when the Project includes the same issue with different URL case', () => {
  const item = { ...project.items[0], url: mockRun.issue_url!.toUpperCase() + '/', title: 'Duplicate snapshot title' }
  const run = { ...mockRun, feature: { issue: mockRun.issue_url!, run_id: mockRun.id,
    status: 'Done', version: 1, pull_requests: [] } }
  render(<RunBoard runs={[run]} projectBoard={{ ...project, items: [item] }} archived={false} onSelect={vi.fn()} onToggle={vi.fn()} onRefresh={vi.fn()} />)
  expect(screen.queryByText('Duplicate snapshot title')).toBeNull()
  expect(within(screen.getByRole('region', { name: 'Done column' })).getByRole('button', { name: /Fixture workflow/ })).toBeTruthy()
})

it('preserves backlog when its refresh fails and never starts work from a board refresh', async () => {
  vi.spyOn(api, 'getService').mockResolvedValue(mockService)
  vi.spyOn(api, 'listRunsPage').mockResolvedValue({ runs: [], next_cursor: null })
  vi.spyOn(api, 'getProjectBoard').mockResolvedValueOnce(project).mockRejectedValue(new Error('Project unavailable'))
  const submit = vi.spyOn(api, 'newRun')
  render(<App />)
  await screen.findByRole('link', { name: /Unstarted backlog work/ })
  await userEvent.setup().click(screen.getByRole('button', { name: 'Refresh board' }))
  await screen.findByRole('alert')
  expect(screen.getByRole('alert').textContent).toContain('Showing the last observation')
  expect(screen.getByRole('link', { name: /Unstarted backlog work/ })).toBeTruthy()
  expect(submit).not.toHaveBeenCalled()
})

it('keeps updated PR validation in QA & CI and exposes the pending validation explicitly', () => {
  const run = { ...mockRun, outcome: 'delivered', feature: {
    issue: mockRun.issue_url!, run_id: mockRun.id, status: 'Validating', version: 1, pull_requests: [],
  } }
  render(<RunBoard runs={[run]} archived={false} onSelect={vi.fn()} onToggle={vi.fn()} onRefresh={vi.fn()} />)
  expect(within(screen.getByRole('region', { name: 'QA & CI column' })).getByText('Validation required for the updated PR commit.')).toBeTruthy()
})

it('puts higher priority backlog ahead of explicitly deferred work', () => {
  const held = { ...project.items[0], id: 'held', number: 2, priority: 'P4', title: 'Deferred proposal',
    url: 'https://github.com/example/repository/issues/2', labels: ['status: deferred'] }
  render(<RunBoard runs={[]} projectBoard={{ ...project, items: [held, project.items[0]] }} archived={false} onSelect={vi.fn()} onToggle={vi.fn()} onRefresh={vi.fn()} />)
  const links = within(screen.getByRole('region', { name: 'Backlog column' })).getAllByRole('link')
  expect(links[0].textContent).toContain('Unstarted backlog work')
  expect(links[1].textContent).toContain('Deferred proposal')
})
