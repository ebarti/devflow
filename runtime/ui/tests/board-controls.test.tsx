import { afterEach, describe, expect, it, vi } from 'vitest'
import { cleanup, render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { App } from '../src/App'
import { api, ApiError } from '../src/api'
import { RunBoard, laneFor } from '../src/RunBoard'
import { RunControls } from '../src/RunControls'
import { Statistics } from '../src/Statistics'
import type { Cohort } from '../src/model'
import { mockRun, mockService } from './fixtures'

vi.mock('../src/stream', () => ({ subscribeRun: vi.fn(() => () => {}) }))
afterEach(() => { cleanup(); vi.restoreAllMocks(); window.history.replaceState(null, '', '/') })

describe('workflow board and controls', () => {
  it('starts a new reporting sample and shows its reset time', async () => {
    const sample = { total_runs: 0, cohorts: [], definitions: 'History is retained.' }
    const baseline = {
      ...sample, reset_at: '2026-10-19T14:00:00Z',
    }
    vi.spyOn(api, 'getStatistics').mockResolvedValueOnce(sample).mockResolvedValue(baseline)
    const reset = vi.spyOn(api, 'resetStatistics').mockResolvedValue(baseline)
    render(<Statistics />)
    await screen.findByText('No recorded runs yet.')
    await userEvent.setup().click(screen.getByRole('button', { name: 'Reset statistics' }))
    expect(reset).toHaveBeenCalledWith({ command_id: expect.any(String) })
    await screen.findByRole('status')
    expect(screen.getByRole('status').textContent).toContain('Earlier run history is retained.')
  })

  it('shows revision samples and telemetry gaps, retaining the sample on refresh failure', async () => {
    const cohort: Cohort = {
      release: 'v-test', revision: 'a'.repeat(40), local_digest: null, provider: 'fake',
      runs: 3, terminal: 3, active: 0, delivered: 2, blocked: 1, cancelled: 0, unknown_outcomes: 0,
      first_pass_delivered: 1, success_rate: 2 / 3, repairs: 1,
      median_duration_seconds: 120, duration_observations: 3,
      attempts: 4, observed_tokens: null, token_observations: 0,
      observed_cost_usd: null, cost_observations: 0, roles: {},
    }
    vi.spyOn(api, 'getStatistics').mockResolvedValueOnce({
      total_runs: 3, cohorts: [cohort], definitions: 'Archived tasks remain included.',
    }).mockRejectedValueOnce(new ApiError('Service disconnected', 0))
    render(<Statistics />)
    await screen.findByText('66.7%')
    expect(screen.getAllByText('v-test · simulation')).toHaveLength(2)
    expect(screen.getAllByText('0/4 attempts observed')).toHaveLength(2)
    expect(screen.getAllByText('Unknown')).toHaveLength(2)
    await userEvent.setup().click(screen.getByRole('button', { name: 'Refresh statistics' }))
    await screen.findByRole('alert')
    expect(screen.getByRole('alert').textContent).toContain('showing the last observed sample')
    expect(screen.getByText('66.7%')).toBeTruthy()
  })

  it.each([
    ['preparing', 'Queued'], ['investigating', 'Planning'], ['prepublish_checks', 'Implementation'],
    ['repair_preflight', 'Implementation'], ['review', 'Review'], ['verify', 'QA & CI'],
    ['waiting_ci', 'QA & CI'], ['tracker', 'Tracking'], ['delivered', 'Delivered'],
    ['blocked', 'Needs attention'], ['waiting_plan', 'Needs attention'], ['future-unknown', 'Needs attention'],
  ])('places %s in the %s lane without inventing progress', (phase, lane) => {
    expect(laneFor({ ...mockRun, phase })).toBe(lane)
  })

  it('shows one current issue card and opens each failed previous attempt from history', async () => {
    const feature = { issue: 'https://github.com/o/r/issues/1', run_id: 'current-run', status: 'In review', version: 12, pull_requests: [], mirror: { state: 'consistent' } }
    const current = { ...mockRun, id: feature.run_id, phase: 'feature_revision_review', feature }
    const previous = Array.from({ length: 11 }, (_, i) => ({ ...mockRun, id: `old-${i}`, phase: 'blocked', outcome: 'blocked', execution_state: 'blocked', feature }))
    const select = vi.fn()
    render(<RunBoard runs={[...previous, current]} archived={false} onSelect={select} onToggle={vi.fn()} onRefresh={vi.fn()} />)
    const review = screen.getByRole('region', { name: 'Review column' })
    expect(review.querySelectorAll('.task-card')).toHaveLength(1)
    expect(review.querySelector('details')?.open).toBe(false)
    expect(review.textContent).toContain('Project: Confirmed')
    await userEvent.setup().click(within(review).getByRole('button', { name: /Fixture workflow/ }))
    expect(select).toHaveBeenLastCalledWith('current-run')
    await userEvent.setup().click(screen.getByText('11 previous attempts'))
    expect(within(review).getAllByRole('button', { name: /Failed/ })).toHaveLength(11)
    await userEvent.setup().click(within(review).getByRole('button', { name: /old-3$/ }))
    expect(select).toHaveBeenLastCalledWith('old-3')
    expect(screen.getByRole('region', { name: 'Failed column' }).querySelectorAll('.task-card')).toHaveLength(0)
  })

  it('links to the observed owner when its row has not loaded, without borrowing an old failure', async () => {
    const select = vi.fn()
    const run = { ...mockRun, phase: 'blocked', outcome: 'blocked', execution_state: 'blocked', feature: { issue: 'https://github.com/o/r/issues/1',
      run_id: 'newer-run', status: 'Merged', version: 4, pull_requests: [], mirror: { state: 'pending' } } }
    render(<RunBoard runs={[run]} archived={false} onSelect={select} onToggle={vi.fn()} onRefresh={vi.fn()} />)
    const lane = screen.getByRole('region', { name: 'Merged column' })
    expect(lane.textContent).toContain('Open current run · details not loaded')
    expect(within(lane).getByRole('button', { name: /Fixture workflow/ }).textContent).not.toContain('Failed')
    await userEvent.setup().click(within(lane).getByRole('button', { name: /Fixture workflow/ }))
    expect(select).toHaveBeenCalledWith('newer-run')
  })

  it('keeps a merged issue visibly merged even when its latest attempt stopped earlier', () => {
    const run = { ...mockRun, outcome: 'blocked', execution_state: 'blocked', feature: {
      issue: 'https://github.com/o/r/issues/1', run_id: mockRun.id, status: 'Merged', version: 4, pull_requests: [],
    } }
    render(<RunBoard runs={[run]} archived={false} onSelect={vi.fn()} onToggle={vi.fn()} onRefresh={vi.fn()} />)
    const lane = screen.getByRole('region', { name: 'Merged column' })
    expect(lane.querySelector('.task-card__status')?.textContent).toBe('Merged')
    expect(lane.textContent).toContain('Last attempt: Failed')
  })

  it.each(['Done', 'Needs validation'])('shows current %s ahead of a historical failed attempt', status => {
    const feature = { issue: 'https://github.com/o/r/issues/1', run_id: mockRun.id,
      status, version: 5, pull_requests: [] }
    const run = { ...mockRun, phase: 'blocked', outcome: 'blocked', execution_state: 'blocked', feature }
    expect(laneFor(run)).toBe(status === 'Done' ? 'Done' : 'Needs attention')
  })

  it('keeps terminal failure distinct from waiting for a decision and an unknown blocked phase', () => {
    const feature = { issue: 'https://github.com/o/r/issues/1', run_id: mockRun.id, status: 'In review', version: 1, pull_requests: [] }
    expect(laneFor({ ...mockRun, feature, outcome: 'blocked', execution_state: 'blocked' })).toBe('Failed')
    expect(laneFor({ ...mockRun, feature, phase: 'waiting_plan', execution_state: 'waiting_plan' })).toBe('Needs attention')
    expect(laneFor({ ...mockRun, phase: 'blocked', outcome: null })).toBe('Needs attention')
    expect(laneFor({ ...mockRun, phase: 'cancelled', outcome: 'cancelled', execution_state: 'cancelled' })).toBe('Needs attention')
  })

  it('shows required revision approval as waiting for a decision', () => {
    const run = { ...mockRun, phase: 'waiting_plan_revision', execution_state: 'waiting', outcome: null,
      feature: { issue: 'https://github.com/o/r/issues/1', run_id: mockRun.id, status: 'Planning', version: 2, pull_requests: [] } }
    render(<RunBoard runs={[run]} archived={false} onSelect={vi.fn()} onToggle={vi.fn()} onRefresh={vi.fn()} />)
    const lane = screen.getByRole('region', { name: 'Needs attention column' })
    expect(lane.querySelectorAll('.task-card')).toHaveLength(1)
    expect(lane.textContent).toContain('Waiting for decision')
    expect(screen.getByRole('region', { name: 'Planning column' }).querySelectorAll('.task-card')).toHaveLength(0)
  })

  it('uses the newest feature version when older paginated rows carry stale owners', () => {
    const feature = { issue: 'https://github.com/o/r/issues/1', run_id: 'new', status: 'In progress', version: 12, pull_requests: [] }
    const current = { ...mockRun, id: 'new', feature }
    const old = { ...mockRun, id: 'old', outcome: 'blocked', feature: { ...feature, run_id: 'old', status: 'Blocked', version: 11 } }
    const view = render(<RunBoard runs={[old, current]} archived={false} onSelect={vi.fn()} onToggle={vi.fn()} onRefresh={vi.fn()} />)
    expect(view.container.querySelectorAll('.task-card')).toHaveLength(1)
    expect(screen.getByRole('region', { name: 'Implementation column' }).querySelectorAll('.task-card')).toHaveLength(1)
    view.rerender(<RunBoard runs={[current, old]} archived={false} onSelect={vi.fn()} onToggle={vi.fn()} onRefresh={vi.fn()} />)
    expect(screen.getByRole('region', { name: 'Implementation column' }).querySelectorAll('.task-card')).toHaveLength(1)
  })

  it('keeps archived attempts individually accessible with their own outcomes', () => {
    const feature = { issue: 'https://github.com/o/r/issues/1', run_id: 'active-not-archived', status: 'In review', version: 12, pull_requests: [] }
    render(<RunBoard runs={['one', 'two'].map(id => ({ ...mockRun, id, outcome: 'blocked', feature, archived: true }))} archived onSelect={vi.fn()} onToggle={vi.fn()} onRefresh={vi.fn()} />)
    const failed = screen.getByRole('region', { name: 'Failed column' })
    expect(within(failed).getAllByRole('button', { name: /Failed attempt/ })).toHaveLength(2)
    expect(screen.queryByText(/Open current run/)).toBeNull()
  })

  it.each([
    ['Queued', 'Queued'], ['Planning', 'Planning'], ['In progress', 'Implementation'],
    ['In review', 'Review'], ['Validating', 'QA & CI'], ['Merging', 'Tracking'],
    ['Awaiting merge', 'Awaiting merge'], ['Merged', 'Merged'], ['Blocked', 'Needs attention'],
    ['Cancelled', 'Needs attention'], ['PR closed', 'Needs attention'], ['Needs validation', 'Needs attention'],
  ])('places the current %s feature ahead of an older delivered run', (status, lane) => {
    expect(laneFor({ ...mockRun, phase: 'delivered', feature: {
      issue: 'https://github.com/o/r/issues/1', run_id: 'newer-run', status, version: 1, pull_requests: [],
    } })).toBe(lane)
  })

  it('renders task statuses and opens the selected task', async () => {
    const select = vi.fn()
    render(<RunBoard runs={[mockRun]} archived={false} onSelect={select} onToggle={vi.fn()} onRefresh={vi.fn()} />)
    expect(screen.getByRole('region', { name: 'QA & CI column' }).textContent).toContain(mockRun.title)
    await userEvent.setup().click(screen.getByRole('button', { name: /Fixture workflow/ }))
    expect(select).toHaveBeenCalledWith(mockRun.id)
  })

  it('makes the board the landing page and isolates settings from the run rail', async () => {
    vi.spyOn(api, 'listRunsPage').mockResolvedValue({ runs: [mockRun], next_cursor: null })
    vi.spyOn(api, 'getService').mockResolvedValue(mockService)
    const read = vi.spyOn(api, 'getRun')
    render(<App />)
    await screen.findByRole('region', { name: 'Task status board' })
    expect(read).not.toHaveBeenCalled()
    await userEvent.setup().click(screen.getByRole('button', { name: 'Settings' }))
    expect(screen.queryByRole('complementary', { name: 'Recent runs' })).toBeNull()
    expect(screen.getByRole('heading', { name: 'Agent models' })).toBeTruthy()
  })

  it('reads the archive collection and retains explicit restore commands', async () => {
    const list = vi.spyOn(api, 'listRunsPage').mockImplementation(async archived => ({ runs: archived ? [{ ...mockRun, archived: true }] : [], next_cursor: null }))
    vi.spyOn(api, 'getService').mockResolvedValue(mockService)
    render(<App />)
    await screen.findByRole('button', { name: 'Show archived tasks' })
    await userEvent.setup().click(screen.getByRole('button', { name: 'Show archived tasks' }))
    await screen.findByRole('heading', { name: 'Archived tasks' })
    await waitFor(() => expect(list).toHaveBeenCalledWith(true))
    expect(screen.getByRole('button', { name: /Fixture workflow/ })).toBeTruthy()
  })

  it('retains the exact instruction command after an uncertain transport result', async () => {
    const steer = vi.spyOn(api, 'steer').mockRejectedValueOnce(new ApiError('lost response', 0)).mockResolvedValue(undefined)
    const refresh = vi.fn(async () => {})
    const run = { ...mockRun, phase: 'implement', can_steer: true, projection_revision: 8 }
    const view = render(<RunControls run={run} onRefresh={refresh} />)
    const user = userEvent.setup()
    await user.type(screen.getByLabelText('Instructions for the next phase'), 'Keep the public API compatible.')
    await user.click(screen.getByRole('button', { name: 'Send instructions' }))
    await screen.findByText(/outcome is unknown/)
    view.rerender(<RunControls run={{ ...run, projection_revision: 9 }} onRefresh={refresh} />)
    await user.click(screen.getByRole('button', { name: 'Retry same instructions' }))
    await screen.findByText(/Instructions queued/)
    expect(steer).toHaveBeenCalledTimes(2)
    expect(steer.mock.calls[0]).toEqual(steer.mock.calls[1])
    expect(steer.mock.calls[0][1]).toMatchObject({ expected_revision: 8, message: 'Keep the public API compatible.' })
  })

  it('archives and restores a stopped task using the observed projection revision', async () => {
    const archive = vi.spyOn(api, 'archive').mockResolvedValue(undefined)
    const run = { ...mockRun, outcome: 'delivered', execution_state: 'terminal', projection_revision: 8 }
    const refresh = vi.fn(async () => {})
    const view = render(<RunControls run={run} onRefresh={refresh} />)
    const user = userEvent.setup()
    await user.click(screen.getByRole('button', { name: 'Archive task' }))
    await screen.findByText(/Task archived/)
    expect(archive.mock.calls[0][1]).toMatchObject({ expected_revision: 8, archived: true })
    view.rerender(<RunControls run={{ ...run, archived: true }} onRefresh={refresh} />)
    await user.click(screen.getByRole('button', { name: 'Restore task' }))
    await screen.findByText('Task restored.')
    expect(archive.mock.calls[1][1]).toMatchObject({ archived: false })
  })
})
