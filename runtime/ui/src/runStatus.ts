import { titleCase } from './format'
import type { RunSummary } from './model'

export function failedAttempt(run: RunSummary): boolean {
  return ['blocked', 'failed', 'timed_out', 'terminated'].includes(run.outcome ?? '')
    || ['blocked', 'failed', 'timed_out', 'terminated'].includes(run.execution_state ?? '')
}

export function runStatus(run: RunSummary): string {
  if (failedAttempt(run)) return 'Failed'
  if (run.outcome === 'cancelled' || run.execution_state === 'cancelled') return 'Cancelled'
  if (run.outcome === 'delivered') return 'Delivered'
  if (['needs_decision', 'waiting_decision', 'waiting_question', 'waiting_plan'].includes(run.phase ?? '')
    || ['waiting_decision', 'waiting_question', 'waiting_plan'].includes(run.execution_state ?? '')) return 'Waiting for decision'
  return titleCase(run.phase)
}
