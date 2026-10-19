import { display, time } from './format'
import { failedAttempt, runStatus } from './runStatus'
import type { RunSummary } from './model'

const lanes = ['Queued', 'Planning', 'Implementation', 'Review', 'QA & CI', 'Tracking', 'Awaiting merge', 'Merged', 'Done', 'Delivered', 'Failed', 'Needs attention'] as const

export function laneFor(run: RunSummary): typeof lanes[number] {
  if (failedAttempt(run) && !['Merged', 'Done', 'Awaiting merge', 'Needs validation'].includes(run.feature?.status ?? '')) return 'Failed'
  if (runStatus(run) === 'Waiting for decision') return 'Needs attention'
  if (run.feature) {
    const featureLanes: Record<string, typeof lanes[number]> = {
      Queued: 'Queued', Planning: 'Planning', 'In progress': 'Implementation',
      'In review': 'Review', Validating: 'QA & CI', 'Awaiting merge': 'Awaiting merge',
      Merging: 'Tracking', 'Waiting for GitHub': 'Tracking', Merged: 'Merged', Done: 'Done', Blocked: 'Needs attention',
      Cancelled: 'Needs attention', 'PR closed': 'Needs attention', 'Needs validation': 'Needs attention',
    }
    return featureLanes[run.feature.status] ?? 'Needs attention'
  }
  if (run.phase === 'delivered') return 'Delivered'
  if (['blocked', 'cancelled', 'cancelling', 'needs_decision', 'waiting_decision', 'waiting_question', 'waiting_plan', 'waiting_tracker'].includes(run.phase ?? '') || ['blocked', 'cancelled', 'waiting_decision', 'unknown', 'waiting_question', 'waiting_plan'].includes(run.execution_state ?? '')) return 'Needs attention'
  if (['investigating', 'intake', 'plan', 'awaiting_plan', 'awaiting_answers'].includes(run.phase ?? '')) return 'Planning'
  if (['implement', 'repair', 'prepublish', 'prepublish_checks', 'publishing', 'publishing_pending', 'repair_preflight'].includes(run.phase ?? '')) return 'Implementation'
  if (run.phase === 'review') return 'Review'
  if (['checks', 'verify', 'ci_wait', 'waiting_ci', 'gates_only', 'metadata_validation', 'qa', 'browser_qa'].includes(run.phase ?? '')) return 'QA & CI'
  if (['tracker', 'tracker_start'].includes(run.phase ?? '')) return 'Tracking'
  if (['preparing', 'accepted', 'queued'].includes(run.phase ?? '')) return 'Queued'
  return 'Needs attention'
}

type Task = { run: RunSummary; previous: RunSummary[]; currentMissing?: boolean }

function currentTasks(runs: RunSummary[]): Task[] {
  const features = new Map<string, { feature: NonNullable<RunSummary['feature']>; rows: RunSummary[] }>()
  const byId = new Map(runs.map(run => [run.id, run]))
  for (const run of runs) {
    if (!run.feature) continue
    const group = features.get(run.feature.issue)
    if (!group) features.set(run.feature.issue, { feature: run.feature, rows: [run] })
    else {
      group.rows.push(run)
      if (run.feature.version > group.feature.version) group.feature = run.feature
    }
  }
  const owners = new Set([...features.values()].map(group => group.feature.run_id))
  const tasks: Task[] = runs.filter(run => !run.feature && !owners.has(run.id)).map(run => ({ run, previous: [] }))
  for (const { feature, rows } of features.values()) {
    const current = byId.get(feature.run_id)
    // An older page may arrive before the current owner. Link to the owner,
    // without presenting an old attempt's phase or outcome as current.
    const run = current ? { ...current, feature } : {
      ...rows[0], id: feature.run_id, run_id: feature.run_id, feature,
      phase: null, execution_state: null, outcome: null, updated_at: null,
    }
    tasks.push({ run, currentMissing: !current, previous: rows.filter(row => row.id !== feature.run_id) })
  }
  return tasks
}

export function RunBoard({ runs, archived, onSelect, onToggle, onRefresh }: {
  runs: RunSummary[]; archived: boolean; onSelect: (id: string) => void
  onToggle: () => void; onRefresh: () => void
}) {
  // The archive is a collection of individual attempts, not current features.
  const tasks: Task[] = archived ? runs.map(run => ({ run: { ...run, feature: null }, previous: [] })) : currentTasks(runs)
  const groups = new Map(lanes.map(lane => [lane, [] as Task[]]))
  for (const task of tasks) groups.get(laneFor(task.run))!.push(task)
  return <section aria-label="Task status board">
    <div className="board-toolbar"><div><h2>{archived ? 'Archived tasks' : 'Task board'}</h2><p className="subtle">{tasks.length} {archived ? 'archived attempts' : 'Devflow issues · one current card per issue'} · refreshed every 5 seconds</p>{!archived ? <p className="subtle">This board shows issues admitted to this Devflow installation. The GitHub Project also includes other backlog items. Each card shows the last confirmed Project readback.</p> : null}</div><div className="button-row"><button className="outline-button" onClick={onToggle}>{archived ? 'Show current tasks' : 'Show archived tasks'}</button><button className="text-button" onClick={onRefresh}>Refresh board</button></div></div>
    {!tasks.length ? <p className="empty-section">{archived ? 'No archived tasks.' : 'No current tasks. Start a new run to see it progress here.'}</p> : null}
    <div className="kanban-board">{lanes.map(lane => <section className="kanban-lane" key={lane} aria-label={`${lane} column`}><h3>{lane}<span>{groups.get(lane)!.length}</span></h3><div className="kanban-cards">{groups.get(lane)!.map(({ run, previous, currentMissing }) => <article key={run.id}>
      <button className="task-card" onClick={() => onSelect(run.id)}>
        <span className="task-card__repo">{display(run.repository || run.repository_key)}</span><strong>{display(run.title || run.goal, 'Untitled task')}</strong><span className={`task-card__status${lane === 'Failed' ? ' task-card__status--failed' : ''}`}>{lane === 'Failed' ? 'Failed attempt' : run.feature?.status ?? runStatus(run)}</span>
        {currentMissing ? <small>Open current run · details not loaded</small> : <small>{run.outcome ? 'Last attempt' : 'Run'}: {runStatus(run)}{run.feature ? ` · Project: ${run.feature.mirror?.state === 'consistent' ? `Confirmed ${time(run.feature.mirror.checked_at)}` : 'Sync pending'}` : ''}</small>}
        {lane === 'Failed' ? <small>This attempt stopped. Open it for the reason and recovery options.</small> : null}
        {!currentMissing ? <small>Last update {time(run.updated_at)}</small> : null}
      </button>
      {previous.length ? <details className="task-history"><summary>{previous.length} previous {previous.length === 1 ? 'attempt' : 'attempts'}</summary><ul>{previous.map(attempt => <li key={attempt.id}><button className="text-button" onClick={() => onSelect(attempt.id)}><span className={failedAttempt(attempt) ? 'task-card__status--failed' : ''}>{runStatus(attempt)}</span> · {time(attempt.updated_at)}<small>{attempt.id}</small></button></li>)}</ul></details> : null}
    </article>)}</div></section>)}</div>
  </section>
}
