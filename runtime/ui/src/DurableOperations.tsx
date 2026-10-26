import { useCallback, useEffect, useState } from 'react'
import { api, type DurableOperation } from './api'

export function DurableOperations({ runId }: { runId: string }) {
  const [items, setItems] = useState<DurableOperation[]>([])
  const [error, setError] = useState('')
  const refresh = useCallback(async () => {
    try {
      setItems((await api.runOperations(runId)).operations)
      setError('')
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : 'Operations unavailable')
    }
  }, [runId])
  useEffect(() => {
    void refresh()
    const timer = window.setInterval(() => void refresh(), 5000)
    return () => window.clearInterval(timer)
  }, [refresh])
  if (!items.length && !error) return null
  return <section aria-label="Durable operations">
    <h2>Durable operations</h2>
    {error ? <p role="alert">{error}</p> : null}
    {items.map(item => <div key={item.operation_id}>
      <strong>{item.kind}</strong> · {item.state} · {item.progress}
      <p>Operation {item.operation_id}. Next action: {item.next_action}.</p>
      {item.error ? <p role="alert">{item.error}</p> : null}
      {item.authority ? <p>Historical result; current authority is unverified.</p> : null}
      {item.state === 'unknown' ? <button onClick={() => void api.reconcileOperation(item.operation_id).then(refresh).catch(reason => setError(String(reason)))}>Reconcile original operation</button> : null}
      {item.result != null ? <details><summary>Result</summary><pre>{JSON.stringify(item.result, null, 2)}</pre></details> : null}
    </div>)}
  </section>
}
