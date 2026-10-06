import { useCallback, useEffect, useRef, useState } from 'react'
import { api } from './api'
import { display, shortTime, time } from './format'
import { PlayIcon, PlusIcon, SettingsIcon } from './icons'
import { NewRun } from './NewRun'
import { RunDetails } from './RunDetails'
import { Settings } from './Settings'
import { RunBoard } from './RunBoard'
import { Statistics } from './Statistics'
import { subscribeRun } from './stream'
import type { RunDetail, RunSummary, ServiceInfo } from './model'

type Page = 'runs' | 'new' | 'settings' | 'statistics'
type Connection = 'connecting' | 'connected' | 'disconnected'

function route(): { page: Page; id: string | null } {
  const path = window.location.pathname
  if (path === '/statistics') return { page: 'statistics', id: null }
  if (path === '/settings') return { page: 'settings', id: null }
  if (path === '/new') return { page: 'new', id: null }
  if (path.startsWith('/runs/')) {
    try { return { page: 'runs', id: decodeURIComponent(path.slice('/runs/'.length)) } }
    catch { return { page: 'runs', id: null } }
  }
  return { page: 'runs', id: null }
}

export function App() {
  const [location, setLocation] = useState(route)
  const [showArchived, setShowArchived] = useState(false)
  const listRequest = useRef(0)
  const [runs, setRuns] = useState<RunSummary[]>([])
  const [olderRuns, setOlderRuns] = useState<RunSummary[]>([])
  const [nextCursor, setNextCursor] = useState<string | null>(null)
  const [olderLoading, setOlderLoading] = useState(false)
  const [olderError, setOlderError] = useState('')
  const olderLoaded = useRef(false)
  const olderRequest = useRef(0)
  const recentIds = new Set(runs.map(run => run.id))
  const allRuns = [...runs, ...olderRuns.filter(old => !recentIds.has(old.id))]
  const [runsLoading, setRunsLoading] = useState(true)
  const [runsError, setRunsError] = useState('')
  const [detail, setDetail] = useState<RunDetail | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [detailError, setDetailError] = useState('')
  const [detailRetry, setDetailRetry] = useState(0)
  const [connection, setConnection] = useState<Connection>('connecting')
  const [lastGoodAt, setLastGoodAt] = useState<string | null>(null)
  const [staleSince, setStaleSince] = useState<string | null>(null)
  const [service, setService] = useState<ServiceInfo | null>(null)
  const [serviceLoading, setServiceLoading] = useState(true)
  const [serviceError, setServiceError] = useState('')

  const navigate = useCallback((page: Page, id: string | null = null, replace = false) => {
    const path = page === 'statistics' ? '/statistics' : page === 'settings' ? '/settings' : page === 'new' ? '/new' : id ? `/runs/${encodeURIComponent(id)}` : '/'
    window.history[replace ? 'replaceState' : 'pushState'](null, '', path)
    setLocation({ page, id })
  }, [])

  useEffect(() => {
    const onPop = () => setLocation(route())
    window.addEventListener('popstate', onPop)
    return () => window.removeEventListener('popstate', onPop)
  }, [])

  const refreshRuns = useCallback(async () => {
    const requestId = ++listRequest.current
    try {
      const page = await api.listRunsPage(showArchived)
      if (requestId !== listRequest.current) return
      setRuns(page.runs)
      if (!olderLoaded.current) setNextCursor(page.next_cursor)
      setRunsError('')
      setRunsLoading(false)
    } catch (cause) {
      if (requestId !== listRequest.current) return
      setRunsError(cause instanceof Error ? cause.message : 'Could not load runs.')
      setRunsLoading(false)
      setConnection('disconnected')
      setStaleSince(current => current ?? new Date().toISOString())
    }
  }, [showArchived])

  const loadOlder = async () => {
    if (!nextCursor || olderLoading) return
    const requestId = ++olderRequest.current
    setOlderLoading(true)
    setOlderError('')
    try {
      const page = await api.listRunsPage(showArchived, nextCursor)
      if (requestId !== olderRequest.current) return
      olderLoaded.current = true
      setOlderRuns(current => {
        const merged = new Map(current.map(run => [run.id, run]))
        for (const run of page.runs) merged.set(run.id, run)
        return [...merged.values()]
      })
      setNextCursor(page.next_cursor)
    } catch (cause) {
      if (requestId === olderRequest.current) setOlderError(cause instanceof Error ? cause.message : 'Could not load older tasks.')
    } finally {
      if (requestId === olderRequest.current) setOlderLoading(false)
    }
  }

  const toggleArchive = () => {
    ++listRequest.current
    ++olderRequest.current
    olderLoaded.current = false
    setOlderRuns([])
    setNextCursor(null)
    setOlderLoading(false)
    setOlderError('')
    setRuns([])
    setRunsLoading(true)
    setShowArchived(value => !value)
  }

  useEffect(() => {
    if (location.page !== 'runs' || location.id) return
    const timer = window.setInterval(() => void refreshRuns(), 5000)
    return () => window.clearInterval(timer)
  }, [location.page, location.id, refreshRuns])

  const refreshService = useCallback(async () => {
    setServiceLoading(true)
    try {
      const info = await api.getService()
      setService(info)
      setServiceError('')
    } catch (cause) {
      setServiceError(cause instanceof Error ? cause.message : 'Could not load service information.')
    }
    finally { setServiceLoading(false) }
  }, [])

  useEffect(() => {
    void refreshRuns()
    void refreshService()
  }, [refreshRuns, refreshService])

  const loadDetail = useCallback(async (id: string) => {
    const snapshot = await api.getRun(id)
    setDetail(snapshot)
    setDetailError('')
    setDetailLoading(false)
    setLastGoodAt(new Date().toISOString())
    return snapshot
  }, [])

  useEffect(() => {
    const id = location.page === 'runs' ? location.id : null
    if (!id) { setDetail(null); setDetailLoading(false); return }
    let alive = true
    let unsubscribe: (() => void) | undefined
    setDetail(current => current?.id === id ? current : null)
    setDetailLoading(true)
    setConnection('connecting')
    void api.getRun(id).then(snapshot => {
      if (!alive) return
      setDetail(snapshot); setDetailError(''); setDetailLoading(false)
      setLastGoodAt(new Date().toISOString())
      unsubscribe = subscribeRun(id, snapshot, {
        onSnapshot: next => {
          if (!alive) return
          setDetail(next); setDetailError(''); setLastGoodAt(new Date().toISOString())
          void refreshRuns()
        },
        onConnection: state => {
          if (!alive) return
          setConnection(state)
          if (state === 'disconnected') setStaleSince(current => current ?? new Date().toISOString())
          if (state === 'connected') setStaleSince(null)
        },
      })
    }).catch(cause => {
      if (!alive) return
      setDetailError(cause instanceof Error ? cause.message : 'Could not load the run.')
      setDetailLoading(false); setConnection('disconnected')
      setStaleSince(current => current ?? new Date().toISOString())
    })
    return () => { alive = false; unsubscribe?.() }
  }, [location.page, location.id, detailRetry, refreshRuns])

  const refreshCurrent = useCallback(async () => {
    if (!location.id) return
    if (connection === 'disconnected') {
      // Re-enter the owning read/subscribe effect after an initial read failure.
      setDetailRetry(current => current + 1)
      await refreshRuns()
      return
    }
    try { await loadDetail(location.id); await refreshRuns() }
    catch (cause) {
      setDetailError(cause instanceof Error ? cause.message : 'Could not refresh the run.')
      setConnection('disconnected')
      setStaleSince(current => current ?? new Date().toISOString())
    }
  }, [location.id, connection, loadDetail, refreshRuns])

  const serviceConnected = location.page === 'runs' && location.id ? connection === 'connected' : !runsError && !serviceError && !runsLoading && !serviceLoading
  const serviceLabel = serviceConnected ? 'Connected' : runsLoading || serviceLoading || connection === 'connecting' ? 'Connecting' : 'Disconnected'

  return <div className={`app-shell${location.page !== 'runs' || !location.id ? ' app-shell--wide' : ''}`}>
    <aside className="nav-rail" aria-label="Main navigation">
      <div className="brand">Devflow</div>
      <nav className="primary-nav">
        <button className={location.page === 'runs' ? 'nav-link nav-link--active' : 'nav-link'} onClick={() => navigate('runs')}><PlayIcon />Runs</button>
        <button className={location.page === 'statistics' ? 'nav-link nav-link--active' : 'nav-link'} onClick={() => navigate('statistics')}><span aria-hidden="true">▥</span>Statistics</button>
        <button className={location.page === 'settings' ? 'nav-link nav-link--active' : 'nav-link'} onClick={() => navigate('settings')}><SettingsIcon />Settings</button>
      </nav>
      <div className="service-indicator" role="status"><span className={`service-indicator__dot ${serviceConnected ? 'service-indicator__dot--good' : serviceLabel === 'Connecting' ? 'service-indicator__dot--waiting' : 'service-indicator__dot--bad'}`} /><div>Local service<small>{serviceLabel}</small></div></div>
    </aside>
    <header className="top-bar">
      <div><h1>{location.page === 'runs' ? 'Runs' : location.page === 'new' ? 'New run' : location.page === 'statistics' ? 'Statistics' : 'Settings'}</h1><p>{location.page === 'runs' ? 'Local development workflows' : location.page === 'new' ? 'Start an authorized workflow' : location.page === 'statistics' ? 'Observed delivery efficiency' : 'Local service information'}</p></div>
      {location.page !== 'new' ? <button className="new-run-button" onClick={() => navigate('new')}><PlusIcon />New run</button> : null}
    </header>
    {location.page === 'runs' && location.id ? <aside className="run-rail" aria-label="Recent runs">
      <div className="rail-heading"><h2>Recent runs</h2></div>
      {runsError ? <div className="rail-error" role="alert">{allRuns.length ? 'Run list is stale.' : runsError} <button className="text-button" onClick={() => void refreshRuns()}>Retry</button></div> : null}
      {runsLoading && !allRuns.length ? <p className="rail-placeholder">Loading runs…</p> : null}
      {!runsLoading && !allRuns.length ? <p className="rail-placeholder">No runs yet. Start a run to see its progress here.</p> : null}
      <div className="run-list">{allRuns.map(run => {
        const id = run.id || run.run_id || ''
        return <button key={id} className={location.page === 'runs' && location.id === id ? 'run-item run-item--active' : 'run-item'} onClick={() => navigate('runs', id)}>
          <span className="run-item__top"><strong>{display(run.repository || run.repository_key, 'Repository unknown')}{run.issue ? ` ${run.issue}` : ''}</strong><time dateTime={run.updated_at ?? undefined}>{shortTime(run.updated_at)}</time></span>
          <span className="run-item__goal">{display(run.title || run.goal, 'Untitled run')}</span>
        </button>
      })}</div>
    </aside> : null}
    <main className="main-panel" id="main-content">
      {location.page === 'statistics' ? <Statistics /> : null}
      {location.page === 'settings' ? <Settings service={service} loading={serviceLoading} error={serviceError} onRefresh={() => void refreshService()} /> : null}
      {location.page === 'new' ? serviceLoading && !service ? <p className="empty-section">Loading admission policy…</p> : <NewRun service={service} onCreated={id => { void refreshRuns(); navigate('runs', id) }} onBack={() => navigate('runs', allRuns[0]?.id || allRuns[0]?.run_id || null)} /> : null}
      {location.page === 'runs' ? <>
        {connection === 'disconnected' || detailError ? <div className="connection-banner connection-banner--bad" role="alert"><div><strong>Disconnected · showing last observed state</strong><span>{detailError || runsError || `Connection lost ${time(staleSince)}.`} {lastGoodAt ? `Last successful read ${time(lastGoodAt)}.` : ''}</span></div><button onClick={() => void refreshCurrent()}>Retry</button></div> : null}
        {connection === 'connecting' && detail ? <div className="connection-banner" role="status">Reconnecting · showing last observed state from {time(lastGoodAt)}.</div> : null}
        {detailLoading && !detail ? <div className="empty-state"><h2>Loading run…</h2><p>Reading the local service projection.</p></div> : null}
        {!detailLoading && !detail && location.id ? <div className="empty-state"><h2>Run unavailable</h2><p>{detailError || 'The service has not returned this run.'}</p><button className="outline-button" onClick={() => void refreshCurrent()}>Retry</button></div> : null}
        {!location.id ? <>{runsError ? <p className="inline-alert" role="alert">Run board is stale. {runsError}</p> : null}{runsLoading ? <p>Loading tasks…</p> : <RunBoard runs={allRuns.filter(run => Boolean(run.archived) === showArchived)} archived={showArchived} onSelect={id => navigate('runs', id)} onToggle={toggleArchive} onRefresh={() => void refreshRuns()} />}{olderError ? <p role="alert">{olderError}</p> : null}{nextCursor ? <button className="outline-button" disabled={olderLoading} onClick={() => void loadOlder()}>{olderLoading ? 'Loading older tasks…' : 'Load older tasks'}</button> : null}</> : null}
        {detail ? <RunDetails key={detail.id} run={detail} onRefresh={refreshCurrent} /> : null}
      </> : null}
    </main>
  </div>
}
