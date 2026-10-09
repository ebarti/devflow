import { afterEach, describe, expect, it, vi } from 'vitest'
import { cleanup, render, screen, waitFor } from '@testing-library/react'
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

  it('shows current merged feature state while retaining the historical run phase', () => {
    const run = { ...mockRun, phase: 'blocked', feature: { issue: 'https://github.com/o/r/issues/1',
      run_id: 'newer-run', status: 'Merged', version: 4, pull_requests: [],
      mirror: { state: 'pending', last_error: 'GitHub unavailable' } } }
    expect(laneFor(run)).toBe('Merged')
    render(<RunBoard runs={[run]} archived={false} onSelect={vi.fn()} onToggle={vi.fn()} onRefresh={vi.fn()} />)
    const lane = screen.getByRole('region', { name: 'Merged column' })
    expect(lane.textContent).toContain('Run: Blocked')
    expect(lane.textContent).toContain('Project: Sync pending')
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
