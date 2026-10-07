import { afterEach, expect, it, vi } from 'vitest'
import { act, cleanup, render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { App } from '../src/App'
import { api } from '../src/api'
import { mockRun, mockService } from './fixtures'
import { subscribeRun } from '../src/stream'

vi.mock('../src/stream', () => ({ subscribeRun: vi.fn(() => () => {}) }))
afterEach(() => { cleanup(); vi.restoreAllMocks(); window.history.replaceState(null, '', '/') })

it('loads older rows explicitly and refreshes only the recent page', async () => {
  let poll: (() => void) | undefined
  const interval = window.setInterval.bind(window)
  vi.spyOn(window, 'setInterval').mockImplementation(((fn: () => void, delay: number) => {
    if (delay === 5000) { poll = fn; return 1 }
    return interval(fn, delay)
  }) as typeof window.setInterval)
  vi.spyOn(api, 'getService').mockResolvedValue(mockService)
  const older = { ...mockRun, id: 'old-run', title: 'Historical row' }
  const list = vi.spyOn(api, 'listRunsPage').mockResolvedValueOnce({ runs: [mockRun], next_cursor: 'older' })
    .mockResolvedValueOnce({ runs: [older], next_cursor: null })
    .mockResolvedValue({ runs: [{ ...mockRun, title: 'Current updated row' }], next_cursor: 'older' })
  render(<App />)
  await screen.findByText(mockRun.title!)
  expect(list).toHaveBeenCalledTimes(1)
  await userEvent.setup().click(screen.getByRole('button', { name: 'Load older tasks' }))
  await screen.findByText('Historical row')
  expect(list).toHaveBeenLastCalledWith(false, 'older')
  await act(async () => { poll!() })
  await screen.findByText('Current updated row')
  expect(screen.getByText('Historical row')).toBeTruthy()
  expect(list.mock.calls).toEqual([[false], [false, 'older'], [false]])
})

it('discards a late older page after archive toggle', async () => {
  vi.spyOn(api, 'getService').mockResolvedValue(mockService)
  let finish: ((page: { runs: typeof mockRun[]; next_cursor: null }) => void) | undefined
  const list = vi.spyOn(api, 'listRunsPage').mockImplementation(async (archived, cursor) => {
    if (archived) return { runs: [], next_cursor: null }
    if (cursor) return new Promise(resolve => { finish = resolve })
    return { runs: [mockRun], next_cursor: 'older' }
  })
  render(<App />)
  const user = userEvent.setup()
  await screen.findByText(mockRun.title!)
  await user.click(screen.getByRole('button', { name: 'Load older tasks' }))
  await user.click(screen.getByRole('button', { name: 'Show archived tasks' }))
  await screen.findByRole('heading', { name: 'Archived tasks' })
  await act(async () => { finish!({ runs: [{ ...mockRun, id: 'old', title: 'Wrong collection' }], next_cursor: null }) })
  expect(screen.queryByText('Wrong collection')).toBeNull()
  expect(list).toHaveBeenLastCalledWith(true)
})

it('retries a failed older page with the same cursor without losing loaded rows', async () => {
  vi.spyOn(api, 'getService').mockResolvedValue(mockService)
  const list = vi.spyOn(api, 'listRunsPage').mockResolvedValueOnce({ runs: [mockRun], next_cursor: 'older' })
    .mockRejectedValueOnce(new Error('Fixture page unavailable'))
    .mockResolvedValueOnce({ runs: [{ ...mockRun, id: 'old', title: 'Recovered older row' }], next_cursor: null })
  render(<App />)
  const user = userEvent.setup()
  await screen.findByText(mockRun.title!)
  await user.click(screen.getByRole('button', { name: 'Load older tasks' }))
  await screen.findByText('Fixture page unavailable')
  expect(screen.getByText(mockRun.title!)).toBeTruthy()
  await user.click(screen.getByRole('button', { name: 'Load older tasks' }))
  await screen.findByText('Recovered older row')
  expect(list.mock.calls).toEqual([[false], [false, 'older'], [false, 'older']])
})

const row = (id: string) => ({ ...mockRun, id, title: `Task ${id}` })

it('retains previously observed recent rows when polling moves the page boundary', async () => {
  vi.spyOn(api, 'getService').mockResolvedValue(mockService)
  const initial = Array.from({ length: 50 }, (_, index) => row(String(index + 1)))
  const older = Array.from({ length: 50 }, (_, index) => row(String(index + 51)))
  const list = vi.spyOn(api, 'listRunsPage').mockResolvedValueOnce({ runs: initial, next_cursor: 'after50' })
    .mockResolvedValueOnce({ runs: older, next_cursor: 'after100' })
    .mockResolvedValueOnce({ runs: [row('new1'), row('new2'), row('new3'), ...initial.slice(0, 47)], next_cursor: 'after47' })
    .mockResolvedValueOnce({ runs: [row('101')], next_cursor: null })
  render(<App />)
  const user = userEvent.setup()
  await screen.findByText('Task 50')
  await user.click(screen.getByRole('button', { name: 'Load older tasks' }))
  await screen.findByText('Task 100')
  await user.click(screen.getByRole('button', { name: 'Refresh board' }))
  await screen.findByText('Task new3')
  expect(screen.queryByText('Task 50')).not.toBeNull()
  expect(screen.queryByText('Task 48')).not.toBeNull()
  await user.click(screen.getByRole('button', { name: 'Load older tasks' }))
  await screen.findByText('Task 101')
  expect(list.mock.calls).toEqual([[false], [false, 'after50'], [false], [false, 'after100']])
})

it('retains boundary rows while the first older request is still pending', async () => {
  vi.spyOn(api, 'getService').mockResolvedValue(mockService)
  let finish: ((page: { runs: typeof mockRun[]; next_cursor: null }) => void) | undefined
  vi.spyOn(api, 'listRunsPage').mockResolvedValueOnce({ runs: [row('boundary')], next_cursor: 'older' })
    .mockImplementationOnce(() => new Promise(resolve => { finish = resolve }))
    .mockResolvedValueOnce({ runs: [row('new')], next_cursor: 'boundary' })
  render(<App />)
  const user = userEvent.setup()
  await screen.findByText('Task boundary')
  await user.click(screen.getByRole('button', { name: 'Load older tasks' }))
  await user.click(screen.getByRole('button', { name: 'Refresh board' }))
  await screen.findByText('Task new')
  expect(screen.queryByText('Task boundary')).not.toBeNull()
  await act(async () => { finish!({ runs: [row('old')], next_cursor: null }) })
  await screen.findByText('Task old')
})

it.each([false, true])('reconciles a loaded older row after archived changes from %s', async archived => {
  vi.spyOn(api, 'getService').mockResolvedValue(mockService)
  let selected = { ...row('selected'), archived, execution_state: 'terminal', projection_revision: 12 }
  const recent = { ...row('recent'), archived }
  const list = vi.spyOn(api, 'listRunsPage').mockImplementation(async (collection, cursor) => {
    if (collection !== archived) return { runs: [], next_cursor: null }
    return cursor ? { runs: [selected], next_cursor: null } : { runs: [recent], next_cursor: 'older' }
  })
  vi.spyOn(api, 'getRun').mockImplementation(async () => selected)
  vi.spyOn(api, 'archive').mockImplementation(async () => {
    selected = { ...selected, archived: !archived, projection_revision: 13 }
  })
  vi.mocked(subscribeRun).mockImplementation((_id, _snapshot, handlers) => {
    handlers.onConnection('connected')
    return () => {}
  })
  render(<App />)
  const user = userEvent.setup()
  await screen.findByRole('button', { name: 'Show archived tasks' })
  if (archived) await user.click(screen.getByRole('button', { name: 'Show archived tasks' }))
  await screen.findByText('Task recent')
  await user.click(screen.getByRole('button', { name: 'Load older tasks' }))
  await user.click(await screen.findByText('Task selected'))
  await user.click(await screen.findByRole('button', { name: archived ? 'Restore task' : 'Archive task' }))
  await screen.findByText(archived ? 'Task restored.' : /Task archived\./)
  expect(window.location.pathname).toBe('/runs/selected')
  await waitFor(() => expect(within(screen.getByRole('complementary', { name: 'Recent runs' })).queryByText('Task selected')).toBeNull())
  await user.click(within(screen.getByRole('complementary', { name: 'Main navigation' })).getByRole('button', { name: 'Runs' }))
  expect(within(screen.getByRole('region', { name: 'Task status board' })).queryByText('Task selected')).toBeNull()
  expect(list.mock.calls.filter(([, cursor]) => cursor)).toHaveLength(1)
})

it('keeps a fresher recent observation when a late older page contains its stale duplicate', async () => {
  vi.spyOn(api, 'getService').mockResolvedValue(mockService)
  let finish: ((page: { runs: typeof mockRun[]; next_cursor: null }) => void) | undefined
  vi.spyOn(api, 'listRunsPage').mockResolvedValueOnce({ runs: [row('recent')], next_cursor: 'older' })
    .mockImplementationOnce(() => new Promise(resolve => { finish = resolve }))
    .mockResolvedValueOnce({ runs: [{ ...row('promoted'), title: 'Fresh promoted task' }, row('recent')], next_cursor: 'older' })
  render(<App />)
  const user = userEvent.setup()
  await screen.findByText('Task recent')
  await user.click(screen.getByRole('button', { name: 'Load older tasks' }))
  await user.click(screen.getByRole('button', { name: 'Refresh board' }))
  await screen.findByText('Fresh promoted task')
  await act(async () => { finish!({ runs: [row('promoted'), row('old')], next_cursor: null }) })
  await screen.findByText('Task old')
  expect(screen.getAllByText('Fresh promoted task')).toHaveLength(1)
  expect(screen.queryByText('Task promoted')).toBeNull()
})

it('exposes a bounded paging restart when the entire recent page turns over', async () => {
  vi.spyOn(api, 'getService').mockResolvedValue(mockService)
  const list = vi.spyOn(api, 'listRunsPage').mockResolvedValueOnce({ runs: [row('boundary')], next_cursor: 'older' })
    .mockResolvedValueOnce({ runs: [row('old')], next_cursor: 'oldest' })
    .mockResolvedValueOnce({ runs: [row('new')], next_cursor: 'gap' })
    .mockResolvedValueOnce({ runs: [row('skipped'), row('boundary')], next_cursor: 'older' })
  render(<App />)
  const user = userEvent.setup()
  await screen.findByText('Task boundary')
  await user.click(screen.getByRole('button', { name: 'Load older tasks' }))
  await screen.findByText('Task old')
  await user.click(screen.getByRole('button', { name: 'Refresh board' }))
  await screen.findByText('Task new')
  expect(screen.queryByText(/Recent page changed completely/)).not.toBeNull()
  expect(screen.getByText('Task old')).toBeTruthy()
  expect(list).toHaveBeenCalledTimes(3)
  await user.click(screen.getByRole('button', { name: 'Load older tasks' }))
  await screen.findByText('Task skipped')
  expect(list).toHaveBeenLastCalledWith(false, 'gap')
  expect(screen.getAllByText('Task boundary')).toHaveLength(1)
})

it('reconciles archive observations from the selected run stream without losing selection', async () => {
  vi.spyOn(api, 'getService').mockResolvedValue(mockService)
  const selected = { ...row('selected'), execution_state: 'terminal', projection_revision: 12 }
  vi.spyOn(api, 'listRunsPage').mockImplementation(async (_archived, cursor) => cursor
    ? { runs: [selected], next_cursor: null } : { runs: [row('recent')], next_cursor: 'older' })
  vi.spyOn(api, 'getRun').mockResolvedValue(selected)
  let snapshot: ((run: typeof mockRun) => void) | undefined
  vi.mocked(subscribeRun).mockImplementation((_id, _initial, handlers) => {
    snapshot = handlers.onSnapshot
    handlers.onConnection('connected')
    return () => {}
  })
  render(<App />)
  const user = userEvent.setup()
  await screen.findByText('Task recent')
  await user.click(screen.getByRole('button', { name: 'Load older tasks' }))
  await user.click(await screen.findByText('Task selected'))
  await screen.findByRole('button', { name: 'Archive task' })
  await act(async () => { snapshot!({ ...selected, archived: true, projection_revision: 13 }) })
  expect(window.location.pathname).toBe('/runs/selected')
  expect(within(screen.getByRole('complementary', { name: 'Recent runs' })).queryByText('Task selected')).toBeNull()
  expect(screen.getByRole('button', { name: 'Restore task' })).toBeTruthy()
})

it('does not let an in-flight older page erase the cursor for a newly exposed gap', async () => {
  vi.spyOn(api, 'getService').mockResolvedValue(mockService)
  let finish: ((page: { runs: typeof mockRun[]; next_cursor: null }) => void) | undefined
  const list = vi.spyOn(api, 'listRunsPage').mockResolvedValueOnce({ runs: [row('boundary')], next_cursor: 'older' })
    .mockImplementationOnce(() => new Promise(resolve => { finish = resolve }))
    .mockResolvedValueOnce({ runs: [row('new')], next_cursor: 'gap' })
    .mockResolvedValueOnce({ runs: [row('skipped')], next_cursor: null })
  render(<App />)
  const user = userEvent.setup()
  await screen.findByText('Task boundary')
  await user.click(screen.getByRole('button', { name: 'Load older tasks' }))
  await user.click(screen.getByRole('button', { name: 'Refresh board' }))
  await screen.findByText('Task new')
  await act(async () => { finish!({ runs: [row('old')], next_cursor: null }) })
  await screen.findByText('Task old')
  expect(screen.getByText(/Recent page changed completely/)).toBeTruthy()
  await user.click(screen.getByRole('button', { name: 'Load older tasks' }))
  await screen.findByText('Task skipped')
  expect(list).toHaveBeenLastCalledWith(false, 'gap')
  expect(screen.queryByText(/Recent page changed completely/)).toBeNull()
})
