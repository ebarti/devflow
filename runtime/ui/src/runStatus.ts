import { titleCase } from './format'
import type { RunSummary } from './model'

export function failedAttempt(run: RunSummary): boolean {
  return ['blocked', 'failed', 'timed_out', 'terminated'].includes(run.outcome ?? '')
    || ['blocked', 'failed', 'timed_out', 'terminated'].includes(run.execution_state ?? '')
}

export function runStatus(run: RunSummary): string {
  const merge = run.merge?.phase
  if (merge === 'merged') return 'Done'
  if (merge === 'resolving_conflicts') return 'Resolving merge conflicts'
  if (merge === 'decision_required') return 'Merge needs a decision'
  if (merge === 'blocked' || merge === 'unknown') return 'Merge needs attention'
  if (merge && !['available', 'unavailable'].includes(merge)) return 'Merging'
  if (failedAttempt(run)) return 'Failed'
  if (run.outcome === 'cancelled' || run.execution_state === 'cancelled') return 'Cancelled'
  if (run.outcome === 'delivered') return 'PR published'
  if (['tracker', 'tracker_start', 'waiting_tracker'].includes(run.phase ?? '')) return 'Finalizing delivery'
  if (['needs_decision', 'waiting_decision', 'waiting_question', 'waiting_plan', 'waiting_plan_revision'].includes(run.phase ?? '')
    || ['waiting_decision', 'waiting_question', 'waiting_plan'].includes(run.execution_state ?? '')) return 'Waiting for decision'
  return titleCase(run.phase)
}
