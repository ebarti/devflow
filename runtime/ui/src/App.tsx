import { useCallback, useEffect, useState } from 'react'
import { api, ApiError } from './api'
import { display, shortTime, time } from './format'
import { ChevronIcon, PlayIcon, PlusIcon, SettingsIcon } from './icons'
import { NewRun } from './NewRun'
import { RunDetails } from './RunDetails'
import { Settings } from './Settings'
import { SignIn } from './SignIn'
import { subscribeRun } from './stream'
import type { RunDetail, RunSummary, ServiceInfo } from './model'

type Page = 'runs' | 'new' | 'settings'
type Connection = 'connecting' | 'connected' | 'disconnected'
type AuthState = 'checking' | 'signed_in' | 'sign_in' | 'unavailable'

function route(): { page: Page; id: string | null } {
  const path = window.location.pathname
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
  const [authState, setAuthState] = useState<AuthState>('checking')
  const [runs, setRuns] = useState<RunSummary[]>([])
  const [runsLoading, setRunsLoading] = useState(true)
  const [runsError, setRunsError] = useState('')
  const [detail, setDetail] = useState<RunDetail | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [detailError, setDetailError] = useState('')
  const [connection, setConnection] = useState<Connection>('connecting')
  const [lastGoodAt, setLastGoodAt] = useState<string | null>(null)
  const [staleSince, setStaleSince] = useState<string | null>(null)
  const [service, setService] = useState<ServiceInfo | null>(null)
  const [serviceLoading, setServiceLoading] = useState(true)
  const [serviceError, setServiceError] = useState('')

  const navigate = useCallback((page: Page, id: string | null = null, replace = false) => {
    const path = page === 'settings' ? '/settings' : page === 'new' ? '/new' : id ? `/runs/${encodeURIComponent(id)}` : '/'
    window.history[replace ? 'replaceState' : 'pushState'](null, '', path)
    setLocation({ page, id })
  }, [])

  useEffect(() => {
    const onPop = () => setLocation(route())
    window.addEventListener('popstate', onPop)
    return () => window.removeEventListener('popstate', onPop)
  }, [])

  const refreshRuns = useCallback(async () => {
    try {
      const items = await api.listRuns()
      setRuns(items)
      setRunsError('')
      setRunsLoading(false)
      if (!route().id && route().page === 'runs' && items.length) navigate('runs', items[0].id || items[0].run_id || null, true)
    } catch (cause) {
      if (cause instanceof ApiError && cause.status === 401) setAuthState('sign_in')
      setRunsError(cause instanceof Error ? cause.message : 'Could not load runs.')
      setRunsLoading(false)
      setConnection('disconnected')
      setStaleSince(current => current ?? new Date().toISOString())
    }
  }, [navigate])

  const refreshService = useCallback(async () => {
    setServiceLoading(true)
    try {
      const info = await api.getService()
      setService(info)
      setServiceError('')
    } catch (cause) {
      if (cause instanceof ApiError && cause.status === 401) setAuthState('sign_in')
      setServiceError(cause instanceof Error ? cause.message : 'Could not load service information.')
    }
    finally { setServiceLoading(false) }
  }, [])

  useEffect(() => {
    let alive = true
    void api.session().then(authenticated => {
      if (!alive) return
      if (authenticated) {
        setAuthState('signed_in')
        void refreshRuns()
        void refreshService()
      } else {
        setAuthState('sign_in')
        setRunsLoading(false); setServiceLoading(false)
      }
    }).catch(cause => {
      if (!alive) return
      const message = cause instanceof Error ? cause.message : 'Local session unavailable.'
      setRunsError(message); setServiceError(message); setRunsLoading(false); setServiceLoading(false)
      setAuthState('unavailable')
      setConnection('disconnected'); setStaleSince(new Date().toISOString())
    })
    return () => { alive = false }
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
    if (authState !== 'signed_in') return
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
      if (cause instanceof ApiError && cause.status === 401) setAuthState('sign_in')
      setDetailError(cause instanceof Error ? cause.message : 'Could not load the run.')
      setDetailLoading(false); setConnection('disconnected')
      setStaleSince(current => current ?? new Date().toISOString())
    })
    return () => { alive = false; unsubscribe?.() }
  }, [authState, location.page, location.id, refreshRuns])

  const refreshCurrent = useCallback(async () => {
    if (!location.id) return
    try { await loadDetail(location.id); await refreshRuns() }
    catch (cause) {
      if (cause instanceof ApiError && cause.status === 401) setAuthState('sign_in')
      setDetailError(cause instanceof Error ? cause.message : 'Could not refresh the run.')
      setConnection('disconnected')
      setStaleSince(current => current ?? new Date().toISOString())
    }
  }, [location.id, loadDetail, refreshRuns])

  const serviceConnected = authState === 'signed_in' && (location.page === 'runs' && location.id ? connection === 'connected' : !runsError && !serviceError && !runsLoading && !serviceLoading)
  const serviceLabel = authState === 'sign_in' ? 'Sign in required' : serviceConnected ? 'Connected' : authState === 'checking' || runsLoading || serviceLoading || connection === 'connecting' ? 'Connecting' : 'Disconnected'

  return <div className="app-shell">
    <aside className="nav-rail" aria-label="Main navigation">
      <div className="brand">Devflow</div>
      <nav className="primary-nav">
        <button className={location.page === 'runs' ? 'nav-link nav-link--active' : 'nav-link'} onClick={() => navigate('runs', runs[0]?.id || runs[0]?.run_id || null)}><PlayIcon />Runs</button>
        <button className={location.page === 'settings' ? 'nav-link nav-link--active' : 'nav-link'} onClick={() => navigate('settings')}><SettingsIcon />Settings</button>
      </nav>
      <div className="service-indicator" role="status"><span className={`service-indicator__dot ${serviceConnected ? 'service-indicator__dot--good' : serviceLabel === 'Connecting' ? 'service-indicator__dot--waiting' : 'service-indicator__dot--bad'}`} /><div>Local service<small>{serviceLabel}</small></div></div>
    </aside>
    <header className="top-bar">
      <div><h1>{location.page === 'runs' ? 'Runs' : location.page === 'new' ? 'New run' : 'Settings'}</h1><p>{location.page === 'runs' ? 'Local development workflows' : location.page === 'new' ? 'Start an authorized workflow' : 'Local service information'}</p></div>
      {location.page !== 'new' ? <button className="new-run-button" disabled={authState !== 'signed_in'} onClick={() => navigate('new')}><PlusIcon />New run</button> : null}
    </header>
    <aside className="run-rail" aria-label="Recent runs">
      <div className="rail-heading"><h2>Recent runs</h2></div>
      {runsError ? <div className="rail-error" role="alert">{runs.length ? 'Run list is stale.' : runsError} <button className="text-button" onClick={() => void refreshRuns()}>Retry</button></div> : null}
      {runsLoading && !runs.length ? <p className="rail-placeholder">Loading runs…</p> : null}
      {!runsLoading && !runs.length ? <p className="rail-placeholder">No runs yet. Start a run to see its progress here.</p> : null}
      <div className="run-list">{runs.map(run => {
        const id = run.id || run.run_id || ''
        return <button key={id} className={location.page === 'runs' && location.id === id ? 'run-item run-item--active' : 'run-item'} onClick={() => navigate('runs', id)}>
          <span className="run-item__top"><strong>{display(run.repository || run.repository_key, 'Repository unknown')}{run.issue ? ` ${run.issue}` : ''}</strong><time dateTime={run.updated_at ?? undefined}>{shortTime(run.updated_at)}</time></span>
          <span className="run-item__goal">{display(run.title || run.goal, 'Untitled run')}</span>
        </button>
      })}</div>
    </aside>
    <main className="main-panel" id="main-content">
      {authState === 'checking' ? <div className="empty-state"><h2>Connecting…</h2><p>Checking the local browser session.</p></div> : null}
      {authState === 'sign_in' ? <SignIn onSignedIn={() => { setAuthState('signed_in'); setRunsLoading(true); void refreshRuns(); void refreshService() }} /> : null}
      {authState === 'unavailable' ? <div className="empty-state"><h2>Local service unavailable</h2><p>{runsError || 'The local service could not be reached.'}</p><button className="outline-button" onClick={() => {
        setAuthState('checking')
        void api.session().then(authenticated => {
          setAuthState(authenticated ? 'signed_in' : 'sign_in')
          if (authenticated) { void refreshRuns(); void refreshService() }
        }).catch(() => setAuthState('unavailable'))
      }}>Retry connection</button></div> : null}
      {authState === 'signed_in' ? <>
      {location.page === 'settings' ? <Settings service={service} loading={serviceLoading} error={serviceError} onRefresh={() => void refreshService()} /> : null}
      {location.page === 'new' ? serviceLoading && !service ? <p className="empty-section">Loading admission policy…</p> : <NewRun service={service} onCreated={id => { void refreshRuns(); navigate('runs', id) }} onBack={() => navigate('runs', runs[0]?.id || runs[0]?.run_id || null)} /> : null}
      {location.page === 'runs' ? <>
        {connection === 'disconnected' || detailError ? <div className="connection-banner connection-banner--bad" role="alert"><div><strong>Disconnected · showing last observed state</strong><span>{detailError || runsError || `Connection lost ${time(staleSince)}.`} {lastGoodAt ? `Last successful read ${time(lastGoodAt)}.` : ''}</span></div><button onClick={() => void refreshCurrent()}>Retry</button></div> : null}
        {connection === 'connecting' && detail ? <div className="connection-banner" role="status">Reconnecting · showing last observed state from {time(lastGoodAt)}.</div> : null}
        {detailLoading && !detail ? <div className="empty-state"><h2>Loading run…</h2><p>Reading the local service projection.</p></div> : null}
        {!detailLoading && !detail && location.id ? <div className="empty-state"><h2>Run unavailable</h2><p>{detailError || 'The service has not returned this run.'}</p><button className="outline-button" onClick={() => void refreshCurrent()}>Retry</button></div> : null}
        {!detail && !location.id && !runsLoading ? <div className="empty-state"><h2>No run selected</h2><p>Start a run or select one from the list to inspect its progress.</p><button className="primary-button" onClick={() => navigate('new')}>New run <ChevronIcon /></button></div> : null}
        {detail ? <RunDetails key={detail.id} run={detail} onRefresh={refreshCurrent} /> : null}
      </> : null}
      </> : null}
    </main>
  </div>
}
