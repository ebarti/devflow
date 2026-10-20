import { safeWebUrl } from './api'
import { display, time } from './format'
import { failedAttempt, runStatus } from './runStatus'
import type { ProjectBoard, ProjectItem, RunSummary } from './model'

const lanes = ['Backlog', 'Queued', 'Planning', 'Implementation', 'Review', 'QA & CI', 'Awaiting merge', 'Blocked', 'Done'] as const
type Lane = typeof lanes[number]
const emptyProject: ProjectBoard = { items: [], projects: [] }
const priorityRank: Record<string, number> = { P1: 1, P2: 2, P3: 3, P4: 4 }
const identity = (url: string) => url.replace(/\/+$/, '').toLowerCase()

function projectLane(status: string): Lane {
  const mapping: Record<string, Lane> = {
    Backlog: 'Backlog', Queued: 'Queued', Planning: 'Planning',
    'In Progress': 'Implementation', 'In progress': 'Implementation',
    Validating: 'QA & CI', 'Awaiting merge': 'Awaiting merge', Blocked: 'Blocked', Done: 'Done',
    // Read older snapshots without reviving retired columns.
    Merged: 'Done', 'PR closed': 'Backlog', 'In review': 'Review',
    'Needs validation': 'QA & CI', 'Partially merged': 'Implementation', Merging: 'Implementation',
  }
  return mapping[status] ?? 'Blocked'
}

export function laneFor(run: RunSummary): Lane {
  if (run.feature) {
    if (['Done', 'Merged', 'Backlog'].includes(run.feature.status)) return projectLane(run.feature.status)
    if (runStatus(run) === 'Waiting for decision') return 'Blocked'
    if (failedAttempt(run) && !['Awaiting merge', 'Validating', 'Needs validation'].includes(run.feature.status)) return 'Blocked'
    if (run.outcome === 'cancelled' && !['Awaiting merge', 'Validating', 'Needs validation'].includes(run.feature.status)) return 'Backlog'
    if (run.feature.status === 'Validating' && run.phase?.includes('review') && !run.outcome) return 'Review'
    return projectLane(run.feature.status)
  }
  if (run.merge?.phase === 'merged') return 'Done'
  if (failedAttempt(run) || runStatus(run) === 'Waiting for decision') return 'Blocked'
  if (run.outcome === 'cancelled' || run.phase === 'cancelled') return 'Backlog'
  if (run.outcome === 'delivered' || run.phase === 'delivered') return 'Awaiting merge'
  if (['blocked', 'cancelling', 'needs_decision', 'waiting_decision', 'waiting_question', 'waiting_plan', 'waiting_tracker'].includes(run.phase ?? '') || ['blocked', 'waiting_decision', 'unknown', 'waiting_question', 'waiting_plan'].includes(run.execution_state ?? '')) return 'Blocked'
  if (['investigating', 'intake', 'plan', 'awaiting_plan', 'awaiting_answers'].includes(run.phase ?? '')) return 'Planning'
  if (['implement', 'repair', 'prepublish', 'prepublish_checks', 'publishing', 'publishing_pending', 'repair_preflight', 'tracker', 'tracker_start', 'merging'].includes(run.phase ?? '')) return 'Implementation'
  if (run.phase === 'review') return 'Review'
  if (['checks', 'verify', 'ci_wait', 'waiting_ci', 'gates_only', 'metadata_validation', 'qa', 'browser_qa'].includes(run.phase ?? '')) return 'QA & CI'
  if (['preparing', 'accepted', 'queued'].includes(run.phase ?? '')) return 'Queued'
  return 'Blocked'
}

type RunTask = { run: RunSummary; previous: RunSummary[]; currentMissing?: boolean }
type Task = RunTask | { item: ProjectItem }

function currentTasks(runs: RunSummary[]): RunTask[] {
  const features = new Map<string, { feature: NonNullable<RunSummary['feature']>; rows: RunSummary[] }>()
  const byId = new Map(runs.map(run => [run.id, run]))
  for (const run of runs) {
    if (!run.feature) continue
    const key = identity(run.feature.issue)
    const group = features.get(key)
    if (!group) features.set(key, { feature: run.feature, rows: [run] })
    else {
      group.rows.push(run)
      if (run.feature.version > group.feature.version) group.feature = run.feature
    }
  }
  const owners = new Set([...features.values()].map(group => group.feature.run_id))
  const tasks: RunTask[] = runs.filter(run => !run.feature && !owners.has(run.id)).map(run => ({ run, previous: [] }))
  for (const { feature, rows } of features.values()) {
    if (feature.status === 'Cancelled') continue
    const current = byId.get(feature.run_id)
    const run = current ? { ...current, feature } : {
      ...rows[0], id: feature.run_id, run_id: feature.run_id, feature,
      phase: null, execution_state: null, outcome: null, updated_at: null,
    }
    tasks.push({ run, currentMissing: !current, previous: rows.filter(row => row.id !== feature.run_id) })
  }
  return tasks
}

function ProjectCard({ item }: { item: ProjectItem }) {
  const held = item.labels.some(label => ['status: deferred', 'do not merge yet'].includes(label.toLowerCase()))
  return <a className="task-card project-card" href={safeWebUrl(item.url)} target="_blank" rel="noreferrer">
    <span className="task-card__repo">{item.repository} · #{item.number}</span>
    <strong>{item.title}</strong><span className="task-card__status">{held ? 'Deferred' : item.status}</span>
    <small>{item.priority ? `${item.priority} · ` : ''}GitHub Project</small>
    <small>Project checked {time(item.checked_at)}</small>
  </a>
}

export function RunBoard({ runs, projectBoard = emptyProject, archived, onSelect, onToggle, onRefresh }: {
  runs: RunSummary[]; projectBoard?: ProjectBoard; archived: boolean; onSelect: (id: string) => void
  onToggle: () => void; onRefresh: () => void
}) {
  const runtime: RunTask[] = archived ? runs.map(run => ({ run: { ...run, feature: null }, previous: [] })) : currentTasks(runs)
  const known = new Set(runs.flatMap(run => [run.feature?.issue, run.issue_url, run.issue].filter((url): url is string => !!url).map(identity)))
  const projects: Task[] = archived ? [] : projectBoard.items.filter(item => item.status !== 'Cancelled' && !known.has(identity(item.url))).map(item => ({ item }))
  const tasks: Task[] = [...runtime, ...projects]
  const groups = new Map(lanes.map(lane => [lane, [] as Task[]]))
  for (const task of tasks) groups.get('item' in task ? projectLane(task.item.status) : laneFor(task.run))!.push(task)
  const projectItems = new Map(projectBoard.items.map(item => [identity(item.url), item]))
  const backlogOrder = (task: Task) => {
    const item = 'item' in task ? task.item : projectItems.get(identity(task.run.feature?.issue || task.run.issue_url || ''))
    const held = item?.labels.some(label => ['status: deferred', 'do not merge yet'].includes(label.toLowerCase()))
    return (held ? 10 : 0) + (priorityRank[item?.priority || ''] ?? 5)
  }
  groups.get('Backlog')!.sort((left, right) => backlogOrder(left) - backlogOrder(right))
  return <section aria-label="Task status board">
    <div className="board-toolbar"><div><h2>{archived ? 'Archived tasks' : 'Task board'}</h2><p className="subtle">{tasks.length} {archived ? 'archived attempts' : 'work items · GitHub Project and Devflow'} · refreshed every 5 seconds</p>{!archived ? <p className="subtle">Backlog includes configured GitHub Project work. Opening a backlog card does not start execution. Run history stays with its issue.</p> : null}</div><div className="button-row"><button className="outline-button" onClick={onToggle}>{archived ? 'Show current tasks' : 'Show archived tasks'}</button><button className="text-button" onClick={onRefresh}>Refresh board</button></div></div>
    {!archived ? projectBoard.projects.map(project => <p className="subtle" role={project.error || project.state === 'unavailable' ? 'alert' : undefined} key={project.url}>{project.state === 'unavailable' ? 'GitHub Project work items have not been observed yet.' : `GitHub Project checked ${time(project.checked_at)}.`}{project.error ? ` Refresh failed; retaining the last observation. ${project.error}` : ''}</p>) : null}
    {!tasks.length ? <p className="empty-section">{archived ? 'No archived tasks.' : 'No observed work items yet.'}</p> : null}
    <div className="kanban-board">{lanes.map(lane => <section className="kanban-lane" key={lane} aria-label={`${lane} column`}><h3>{lane}<span>{groups.get(lane)!.length}</span></h3><div className="kanban-cards">{groups.get(lane)!.map(task => {
      if ('item' in task) return <article key={task.item.url}><ProjectCard item={task.item} /></article>
      const { run, previous, currentMissing } = task
      const held = run.feature?.status === 'Backlog' && run.feature.issue_observation?.observation?.labels?.some(label => ['status: deferred', 'do not merge yet'].includes(label.toLowerCase()))
      return <article key={run.id}>
        <button className="task-card" onClick={() => onSelect(run.id)}>
          <span className="task-card__repo">{display(run.repository || run.repository_key)}</span><strong>{display(run.title || run.goal, 'Untitled task')}</strong><span className={`task-card__status${lane === 'Blocked' ? ' task-card__status--failed' : ''}`}>{held ? 'Deferred' : lane}</span>
          {currentMissing ? <small>Open current run · details not loaded</small> : <small>{run.outcome ? 'Last attempt' : 'Run'}: {runStatus(run)}{run.feature ? ` · Project: ${run.feature.mirror?.state === 'consistent' ? `Confirmed ${time(run.feature.mirror.checked_at)}` : 'Sync pending'}` : ''}</small>}
          {run.feature?.status === 'Validating' && run.outcome ? <small>Validation required for the updated PR commit.</small> : null}
          {lane === 'Blocked' ? <small>Open this issue for the blocker and recovery options.</small> : null}
          {!currentMissing ? <small>Last update {time(run.updated_at)}</small> : null}
        </button>
        {previous.length ? <details className="task-history"><summary>{previous.length} previous {previous.length === 1 ? 'attempt' : 'attempts'}</summary><ul>{previous.map(attempt => <li key={attempt.id}><button className="text-button" onClick={() => onSelect(attempt.id)}><span className={failedAttempt(attempt) ? 'task-card__status--failed' : ''}>{runStatus(attempt)}</span> · {time(attempt.updated_at)}<small>{attempt.id}</small></button></li>)}</ul></details> : null}
      </article>
    })}</div></section>)}</div>
  </section>
}
