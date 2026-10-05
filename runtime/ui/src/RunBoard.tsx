import { display, titleCase, time } from './format'
import type { RunSummary } from './model'

const lanes = ['Queued', 'Planning', 'Implementation', 'Review', 'QA & CI', 'Tracking', 'Delivered', 'Needs attention'] as const

export function laneFor(run: RunSummary): typeof lanes[number] {
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

export function RunBoard({ runs, archived, onSelect, onToggle, onRefresh }: {
  runs: RunSummary[]; archived: boolean; onSelect: (id: string) => void
  onToggle: () => void; onRefresh: () => void
}) {
  const groups = new Map(lanes.map(lane => [lane, [] as RunSummary[]]))
  for (const run of runs) groups.get(laneFor(run))!.push(run)
  return <section aria-label="Task status board">
    <div className="board-toolbar"><div><h2>{archived ? 'Archived tasks' : 'Task board'}</h2><p className="subtle">{runs.length} tasks · live workflow statuses · refreshed every 5 seconds</p></div><div className="button-row"><button className="outline-button" onClick={onToggle}>{archived ? 'Show current tasks' : 'Show archived tasks'}</button><button className="text-button" onClick={onRefresh}>Refresh board</button></div></div>
    {!runs.length ? <p className="empty-section">{archived ? 'No archived tasks.' : 'No current tasks. Start a new run to see it progress here.'}</p> : null}
    <div className="kanban-board">{lanes.map(lane => <section className="kanban-lane" key={lane} aria-label={`${lane} column`}><h3>{lane}<span>{groups.get(lane)!.length}</span></h3><div className="kanban-cards">{groups.get(lane)!.map(run => <button className="task-card" key={run.id} onClick={() => onSelect(run.id || run.run_id || '')}>
      <span className="task-card__repo">{display(run.repository || run.repository_key)}</span><strong>{display(run.title || run.goal, 'Untitled task')}</strong><span className="task-card__status">{titleCase(run.phase || 'unknown')}</span><small>Last update {time(run.updated_at)}</small>
    </button>)}</div></section>)}</div>
  </section>
}
