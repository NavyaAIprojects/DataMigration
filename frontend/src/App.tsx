import React, { useState, useEffect, useRef, useCallback } from "react";
import axios from "axios";
import "./App.css";

const API = process.env.REACT_APP_API_URL || "http://localhost:8000/api";

type DbType = "mssql" | "databricks";
type ConnectionStatus = "idle" | "testing" | "connected" | "failed";

interface AgentInfo {
  agent_id: string;
  display_name: string;
  agent_type: string;
  status: string;
  progress: number;
  message: string;
  started_at?: string;
  completed_at?: string;
  error?: string;
}

interface MigrationJob {
  job_id: string;
  status: string;
  progress: number;
  current_step: string;
  steps_completed: string[];
  steps_total: number;
  errors: string[];
  start_time?: string;
  end_time?: string;
  stats?: Record<string, any>;
  agents?: Record<string, AgentInfo>;
}

function App() {
  const [sourceDb, setSourceDb] = useState<DbType>("mssql");
  const [targetDb, setTargetDb] = useState<DbType>("databricks");
  const [envFile, setEnvFile] = useState<File | null>(null);
  const [humanDecisionsFile, setHumanDecisionsFile] = useState<File | null>(null);
  const [sourceStatus, setSourceStatus] = useState<ConnectionStatus>("idle");
  const [targetStatus, setTargetStatus] = useState<ConnectionStatus>("idle");
  const [sourceInfo, setSourceInfo] = useState<any>(null);
  const [targetInfo, setTargetInfo] = useState<any>(null);
  const [migrationJob, setMigrationJob] = useState<MigrationJob | null>(null);
  const [error, setError] = useState<string>("");
  const [resetStatus, setResetStatus] = useState<"idle" | "resetting" | "done">("idle");
  const [history, setHistory] = useState<any[]>([]);
  const [elapsed, setElapsed] = useState<string>("00:00:00");
  const [showCompletedTables, setShowCompletedTables] = useState(false);
  const pollingRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const startTimeRef = useRef<number | null>(null);

  const formatDuration = (ms: number): string => {
    const totalSec = Math.floor(ms / 1000);
    const h = Math.floor(totalSec / 3600);
    const m = Math.floor((totalSec % 3600) / 60);
    const s = totalSec % 60;
    return [h, m, s].map((v) => String(v).padStart(2, "0")).join(":");
  };

  useEffect(() => {
    if (migrationJob?.status === "running" || migrationJob?.status === "pending") {
      if (!startTimeRef.current) startTimeRef.current = Date.now();
      timerRef.current = setInterval(() => {
        if (startTimeRef.current) setElapsed(formatDuration(Date.now() - startTimeRef.current));
      }, 1000);
      return () => { if (timerRef.current) clearInterval(timerRef.current); };
    } else if (migrationJob?.status === "completed" || migrationJob?.status === "failed") {
      if (timerRef.current) clearInterval(timerRef.current);
      if (migrationJob.start_time && migrationJob.end_time) {
        const start = new Date(migrationJob.start_time).getTime();
        const end = new Date(migrationJob.end_time).getTime();
        setElapsed(formatDuration(end - start));
      }
    }
  }, [migrationJob?.status, migrationJob?.start_time, migrationJob?.end_time]);

  const fetchHistory = useCallback(async () => {
    try { const resp = await axios.get(`${API}/migration-history`); setHistory(resp.data); } catch {}
  }, []);

  useEffect(() => { fetchHistory(); }, [fetchHistory]);

  const resetTarget = async () => {
    if (!envFile) { setError("Please upload the .env file first"); return; }
    if (!window.confirm("This will DELETE ALL data from the target Databricks catalog. Are you sure?")) return;
    setResetStatus("resetting"); setError("");
    try {
      const formData = new FormData();
      formData.append("env_file", envFile);
      const resp = await axios.post(`${API}/reset-target`, formData);
      setResetStatus("done"); setTargetInfo(null); setTargetStatus("idle");
      setMigrationJob(null); startTimeRef.current = null; setElapsed("00:00:00");
      setTimeout(() => setResetStatus("idle"), 3000);
      alert(resp.data.message);
    } catch (e: any) { setResetStatus("idle"); setError(e.response?.data?.detail || e.message); }
  };

  const testConnection = async (dbType: DbType) => {
    if (!envFile) { setError("Please upload the .env file first"); return; }
    const setter = dbType === "mssql" ? setSourceStatus : setTargetStatus;
    const infoSetter = dbType === "mssql" ? setSourceInfo : setTargetInfo;
    setter("testing"); setError("");
    try {
      const formData = new FormData();
      formData.append("env_file", envFile);
      formData.append("db_type", dbType);
      const resp = await axios.post(`${API}/test-connection`, formData);
      if (resp.data.connected) { setter("connected"); infoSetter(resp.data.info); }
      else { setter("failed"); setError(resp.data.error || "Connection failed"); }
    } catch (e: any) { setter("failed"); setError(e.message); }
  };

  const startMigration = async () => {
    if (!envFile) { setError("Please upload the .env file"); return; }
    setError("");
    try {
      const formData = new FormData();
      formData.append("env_file", envFile);
      if (humanDecisionsFile) formData.append("human_decisions_file", humanDecisionsFile);
      const resp = await axios.post(`${API}/start-migration`, formData);
      setMigrationJob({
        job_id: resp.data.job_id, status: "running", progress: 0,
        current_step: "Initializing agents...", steps_completed: [],
        steps_total: 0, errors: [], agents: {},
      });
    } catch (e: any) { setError(e.message); }
  };

  const pollStatus = useCallback(async () => {
    if (!migrationJob?.job_id) return;
    try {
      const resp = await axios.get(`${API}/migration-status/${migrationJob.job_id}`);
      setMigrationJob(resp.data);
      if (resp.data.status === "completed" || resp.data.status === "failed") {
        if (pollingRef.current) clearInterval(pollingRef.current);
        fetchHistory();
      }
    } catch {}
  }, [migrationJob?.job_id, fetchHistory]);

  useEffect(() => {
    if (migrationJob?.status === "running" || migrationJob?.status === "pending") {
      pollingRef.current = setInterval(pollStatus, 1500);
      return () => { if (pollingRef.current) clearInterval(pollingRef.current); };
    }
  }, [migrationJob?.status, pollStatus]);

  const downloadReport = () => {
    if (migrationJob?.job_id) window.open(`${API}/download-report/${migrationJob.job_id}`, "_blank");
  };

  const statusIcon = (s: ConnectionStatus) => {
    switch (s) {
      case "connected": return <span className="status-dot green" />;
      case "failed": return <span className="status-dot red" />;
      case "testing": return <span className="status-dot yellow pulse" />;
      default: return <span className="status-dot gray" />;
    }
  };

  const agentStatusIcon = (status: string) => {
    switch (status) {
      case "completed": return <span className="agent-icon completed">&#10003;</span>;
      case "running": return <span className="agent-icon running">&#9654;</span>;
      case "failed": return <span className="agent-icon failed">&#10007;</span>;
      default: return <span className="agent-icon pending">&#9679;</span>;
    }
  };

  // Parse agents into groups
  const agents = migrationJob?.agents || {};
  const agentList = Object.values(agents);
  const schemaAgent = agentList.find(a => a.agent_type === "schema");
  const tableAgents = agentList.filter(a => a.agent_type === "table");
  const viewAgent = agentList.find(a => a.agent_type === "view");
  const procAgent = agentList.find(a => a.agent_type === "proc");
  const validationAgentInfo = agentList.find(a => a.agent_type === "validation");
  const reportAgentInfo = agentList.find(a => a.agent_type === "report");

  const runningTables = tableAgents.filter(a => a.status === "running");
  const completedTables = tableAgents.filter(a => a.status === "completed");
  const failedTables = tableAgents.filter(a => a.status === "failed");
  const pendingTables = tableAgents.filter(a => a.status === "pending");

  const isRunning = migrationJob?.status === "running" || migrationJob?.status === "pending";
  const isCompleted = migrationJob?.status === "completed";
  const isFailed = migrationJob?.status === "failed";

  return (
    <div className="app">
      <header className="header">
        <div className="header-content">
          <h1>Database Migration Tool</h1>
          <p className="subtitle">MSSQL to Databricks &mdash; Multi-Agent Architecture</p>
        </div>
      </header>

      <main className="main">
        {/* Step 1: Database Selection */}
        <section className="card">
          <div className="card-header">
            <span className="step-num">1</span>
            <h2>Select Databases</h2>
          </div>
          <div className="db-select-row">
            <div className="db-select">
              <label>Source Database</label>
              <select value={sourceDb} onChange={(e) => setSourceDb(e.target.value as DbType)} disabled={isRunning}>
                <option value="mssql">Microsoft SQL Server (MSSQL)</option>
              </select>
              <div className="connection-row">
                {statusIcon(sourceStatus)}
                <button onClick={() => testConnection("mssql")} disabled={!envFile || isRunning} className="btn-sm">
                  {sourceStatus === "testing" ? "Testing..." : "Test Connection"}
                </button>
                {sourceStatus === "connected" && <span className="conn-text green-text">Connected</span>}
                {sourceStatus === "failed" && <span className="conn-text red-text">Failed</span>}
              </div>
              {sourceInfo && (
                <div className="info-box">
                  <div className="info-item"><span className="info-num">{sourceInfo.table_count}</span> tables</div>
                  <div className="info-item"><span className="info-num">{sourceInfo.view_count}</span> views</div>
                  <div className="info-item"><span className="info-num">{sourceInfo.proc_count}</span> procs</div>
                  <div className="info-item"><span className="info-num">{sourceInfo.trigger_count}</span> triggers</div>
                </div>
              )}
            </div>
            <div className="arrow-container"><div className="arrow">&#10132;</div></div>
            <div className="db-select">
              <label>Target Database</label>
              <select value={targetDb} onChange={(e) => setTargetDb(e.target.value as DbType)} disabled={isRunning}>
                <option value="databricks">Databricks (Delta Lake)</option>
              </select>
              <div className="connection-row">
                {statusIcon(targetStatus)}
                <button onClick={() => testConnection("databricks")} disabled={!envFile || isRunning} className="btn-sm">
                  {targetStatus === "testing" ? "Testing..." : "Test Connection"}
                </button>
                {targetStatus === "connected" && <span className="conn-text green-text">Connected</span>}
                {targetStatus === "failed" && <span className="conn-text red-text">Failed</span>}
              </div>
              {targetInfo && (
                <div className="info-box">
                  <div className="info-item"><span className="info-num">{targetInfo.catalogs?.length}</span> catalogs</div>
                  <div className="info-item">Ready for migration</div>
                </div>
              )}
              <div className="reset-row">
                <button onClick={resetTarget} disabled={!envFile || isRunning || resetStatus === "resetting"} className="btn-reset">
                  {resetStatus === "resetting" ? "Resetting..." : resetStatus === "done" ? "Target Reset" : "Reset Target DB"}
                </button>
                {resetStatus === "done" && <span className="conn-text green-text">Cleared</span>}
              </div>
            </div>
          </div>
        </section>

        {/* Step 2: Upload Files */}
        <section className="card">
          <div className="card-header">
            <span className="step-num">2</span>
            <h2>Upload Configuration Files</h2>
          </div>
          <div className="upload-row">
            <div className="upload-box">
              <div className="upload-icon">&#128274;</div>
              <label>Environment File (.env) <span className="required">*</span></label>
              <p className="upload-desc">MSSQL, Databricks & GCP connection credentials</p>
              <label className="file-input-label">
                <input type="file" accept="*" onChange={(e) => {
                  setEnvFile(e.target.files?.[0] || null);
                  setSourceStatus("idle"); setTargetStatus("idle");
                  setSourceInfo(null); setTargetInfo(null);
                }} disabled={isRunning} />
                {envFile ? envFile.name : "Choose file..."}
              </label>
            </div>
            <div className="upload-box">
              <div className="upload-icon">&#9881;</div>
              <label>Human Decisions File (.env)</label>
              <p className="upload-desc">Pre-answers for 14 issues: HIERARCHYID, stored procs, collation, triggers, etc.</p>
              <label className="file-input-label">
                <input type="file" accept="*" onChange={(e) => setHumanDecisionsFile(e.target.files?.[0] || null)} disabled={isRunning} />
                {humanDecisionsFile ? humanDecisionsFile.name : "Choose file..."}
              </label>
            </div>
          </div>
        </section>

        {/* Step 3: Run Migration */}
        <section className="card">
          <div className="card-header">
            <span className="step-num">3</span>
            <h2>Run Migration</h2>
          </div>

          {!migrationJob && (
            <div className="start-section">
              <p className="start-desc">
                Multi-agent parallel migration: 8 concurrent table workers, automatic view translation,
                procedure documentation. Auto-fixes 44 known compatibility issues.
              </p>
              <button className="btn-primary" onClick={startMigration} disabled={!envFile || isRunning}>
                &#9654; Start Migration
              </button>
            </div>
          )}

          {migrationJob && (
            <div className="migration-progress">
              <div className="progress-header">
                <span className={`status-badge ${migrationJob.status}`}>
                  {migrationJob.status.toUpperCase()}
                </span>
                <div className="progress-header-right">
                  <span className="elapsed-time">{elapsed}</span>
                  <span className="progress-pct">{Math.round(migrationJob.progress)}%</span>
                </div>
              </div>

              <div className="progress-bar-container">
                <div className={`progress-bar ${migrationJob.status}`} style={{ width: `${migrationJob.progress}%` }} />
              </div>

              <p className="current-step">{migrationJob.current_step}</p>

              {/* ── Agent Status Panel ── */}
              {agentList.length > 0 && (
                <div className="agents-panel">
                  <h4 className="agents-title">Agent Status</h4>

                  {/* Schema Agent */}
                  {schemaAgent && (
                    <div className={`agent-row ${schemaAgent.status}`}>
                      {agentStatusIcon(schemaAgent.status)}
                      <span className="agent-name">Schema Setup</span>
                      <div className="agent-bar-wrap">
                        <div className="agent-bar" style={{ width: `${schemaAgent.progress}%` }} />
                      </div>
                      <span className="agent-msg">{schemaAgent.message}</span>
                    </div>
                  )}

                  {/* Table Agents Group */}
                  {tableAgents.length > 0 && (
                    <div className="agent-group">
                      <div className="agent-group-header">
                        <span className="agent-group-icon">
                          {completedTables.length === tableAgents.length
                            ? <span className="agent-icon completed">&#10003;</span>
                            : runningTables.length > 0
                            ? <span className="agent-icon running">&#9654;</span>
                            : <span className="agent-icon pending">&#9679;</span>}
                        </span>
                        <span className="agent-group-title">
                          Tables ({completedTables.length}/{tableAgents.length})
                        </span>
                        <span className="agent-group-detail">
                          {runningTables.length > 0 && <span className="running-count">{runningTables.length} running</span>}
                          {failedTables.length > 0 && <span className="failed-count">{failedTables.length} failed</span>}
                          {pendingTables.length > 0 && <span className="pending-count">{pendingTables.length} queued</span>}
                        </span>
                      </div>

                      {/* Running table agents - always visible */}
                      {runningTables.map(a => (
                        <div key={a.agent_id} className="agent-row running sub-agent">
                          {agentStatusIcon(a.status)}
                          <span className="agent-name">{a.display_name}</span>
                          <div className="agent-bar-wrap">
                            <div className="agent-bar" style={{ width: `${a.progress}%` }} />
                          </div>
                          <span className="agent-msg">{a.message}</span>
                        </div>
                      ))}

                      {/* Failed table agents - always visible */}
                      {failedTables.map(a => (
                        <div key={a.agent_id} className="agent-row failed sub-agent">
                          {agentStatusIcon(a.status)}
                          <span className="agent-name">{a.display_name}</span>
                          <span className="agent-msg agent-error">{a.message}</span>
                        </div>
                      ))}

                      {/* Completed tables - collapsible */}
                      {completedTables.length > 0 && (
                        <div
                          className="agent-collapse-toggle"
                          onClick={() => setShowCompletedTables(!showCompletedTables)}
                        >
                          {showCompletedTables ? "Hide" : "Show"} {completedTables.length} completed tables
                          <span className="collapse-arrow">{showCompletedTables ? "▲" : "▼"}</span>
                        </div>
                      )}
                      {showCompletedTables && completedTables.map(a => (
                        <div key={a.agent_id} className="agent-row completed sub-agent">
                          {agentStatusIcon(a.status)}
                          <span className="agent-name">{a.display_name}</span>
                          <span className="agent-msg">{a.message}</span>
                        </div>
                      ))}
                    </div>
                  )}

                  {/* View Agent */}
                  {viewAgent && (
                    <div className={`agent-row ${viewAgent.status}`}>
                      {agentStatusIcon(viewAgent.status)}
                      <span className="agent-name">Views Migration</span>
                      <div className="agent-bar-wrap">
                        <div className="agent-bar" style={{ width: `${viewAgent.progress}%` }} />
                      </div>
                      <span className="agent-msg">{viewAgent.message}</span>
                    </div>
                  )}

                  {/* Proc Agent */}
                  {procAgent && (
                    <div className={`agent-row ${procAgent.status}`}>
                      {agentStatusIcon(procAgent.status)}
                      <span className="agent-name">Stored Procedures</span>
                      <div className="agent-bar-wrap">
                        <div className="agent-bar" style={{ width: `${procAgent.progress}%` }} />
                      </div>
                      <span className="agent-msg">{procAgent.message}</span>
                    </div>
                  )}

                  {/* Validation Agent */}
                  {validationAgentInfo && (
                    <div className={`agent-row ${validationAgentInfo.status}`}>
                      {agentStatusIcon(validationAgentInfo.status)}
                      <span className="agent-name">Validation Check</span>
                      <div className="agent-bar-wrap">
                        <div className="agent-bar" style={{ width: `${validationAgentInfo.progress}%` }} />
                      </div>
                      <span className="agent-msg">{validationAgentInfo.message}</span>
                    </div>
                  )}

                  {/* Report Agent */}
                  {reportAgentInfo && (
                    <div className={`agent-row ${reportAgentInfo.status}`}>
                      {agentStatusIcon(reportAgentInfo.status)}
                      <span className="agent-name">PDF Report</span>
                      <div className="agent-bar-wrap">
                        <div className="agent-bar" style={{ width: `${reportAgentInfo.progress}%` }} />
                      </div>
                      <span className="agent-msg">{reportAgentInfo.message}</span>
                    </div>
                  )}
                </div>
              )}

              {/* Errors */}
              {migrationJob.errors && migrationJob.errors.length > 0 && (
                <div className="errors-box">
                  <h4>Errors ({migrationJob.errors.length})</h4>
                  {migrationJob.errors.map((err, i) => (
                    <div key={i} className="error-item">{err}</div>
                  ))}
                </div>
              )}

              {/* Stats */}
              {isCompleted && migrationJob.stats && (
                <div className="stats-section">
                  <h4>Migration Results</h4>
                  <div className="stats-grid">
                    {(() => {
                      const s = migrationJob.stats;
                      const src = sourceInfo || {};
                      const tablesOk = !src.table_count || s.tables_created >= src.table_count;
                      const viewsOk = !src.view_count || s.views_created >= src.view_count;
                      const procsOk = !src.proc_count || s.procedures_migrated >= src.proc_count;
                      const triggersOk = !src.trigger_count || s.triggers_migrated >= src.trigger_count;
                      const validOk = s.validation_pct === 100;
                      return (<>
                        <div className="stat-card blue"><div className="stat-value">{s.schemas_created}</div><div className="stat-label">Schemas</div></div>
                        <div className={`stat-card ${tablesOk ? "blue" : "red"}`}><div className="stat-value">{s.tables_created}{!tablesOk && src.table_count ? `/${src.table_count}` : ""}</div><div className="stat-label">Tables</div></div>
                        <div className="stat-card blue"><div className="stat-value">{s.rows_transferred?.toLocaleString()}</div><div className="stat-label">Rows</div></div>
                        <div className={`stat-card ${viewsOk ? "purple" : "red"}`}><div className="stat-value">{s.views_created}{!viewsOk && src.view_count ? `/${src.view_count}` : ""}</div><div className="stat-label">Views</div></div>
                        <div className={`stat-card ${procsOk ? "purple" : "red"}`}><div className="stat-value">{s.procedures_migrated}{!procsOk && src.proc_count ? `/${src.proc_count}` : ""}</div><div className="stat-label">Procedures</div></div>
                        <div className={`stat-card ${triggersOk ? "purple" : "red"}`}><div className="stat-value">{s.triggers_migrated}{!triggersOk && src.trigger_count ? `/${src.trigger_count}` : ""}</div><div className="stat-label">Triggers</div></div>
                        <div className={`stat-card ${validOk ? "green" : "red"}`}><div className="stat-value">{s.validation_pct}%</div><div className="stat-label">Validated</div></div>
                        <div className="stat-card green"><div className="stat-value">{s.issues_auto_fixed}</div><div className="stat-label">Auto-Fixed</div></div>
                        <div className="stat-card green"><div className="stat-value">{s.issues_human_resolved}</div><div className="stat-label">Human Resolved</div></div>
                      </>);
                    })()}
                    <div className="stat-card orange"><div className="stat-value">{elapsed}</div><div className="stat-label">Time Taken</div></div>
                    <div className="stat-card orange"><div className="stat-value">${migrationJob.stats.estimated_cost_usd}</div><div className="stat-label">Est. Cost</div></div>
                    <div className="stat-card orange"><div className="stat-value">{migrationJob.stats.tokens_used?.toLocaleString()}</div><div className="stat-label">Tokens</div></div>
                  </div>
                </div>
              )}

              {isCompleted && (
                <button className="btn-primary btn-download" onClick={downloadReport}>
                  &#128196; Download PDF Report
                </button>
              )}

              {isFailed && (
                <button className="btn-primary btn-retry" onClick={() => setMigrationJob(null)}>
                  &#8634; Reset &amp; Try Again
                </button>
              )}
            </div>
          )}
        </section>

        {/* Migration History */}
        {history.length > 0 && (
          <section className="card">
            <div className="card-header">
              <span className="step-num">4</span>
              <h2>Migration History</h2>
            </div>
            <div className="history-table-wrapper">
              <table className="history-table">
                <thead>
                  <tr>
                    <th>Date &amp; Time</th><th>Status</th><th>Duration</th>
                    <th>Source</th><th>Target</th><th>Tables</th><th>Rows</th>
                    <th>Views</th><th>Procs</th><th>Issues Fixed</th>
                    <th>Tokens</th><th>Cost</th><th>Errors</th>
                  </tr>
                </thead>
                <tbody>
                  {history.map((h, i) => (
                    <tr key={i} className={h.status === "failed" ? "row-failed" : ""}>
                      <td className="td-nowrap">{h.start_time ? new Date(h.start_time).toLocaleString() : "\u2014"}</td>
                      <td><span className={`history-badge ${h.status}`}>{h.status?.toUpperCase()}</span></td>
                      <td>{h.duration || "\u2014"}</td>
                      <td className="td-small">{h.source_db?.database || "\u2014"}<br /><span className="td-muted">{h.source_db?.host}</span></td>
                      <td className="td-small">{h.target_db?.catalog || "\u2014"}<br /><span className="td-muted">{h.target_db?.host?.replace("https://", "").slice(0, 20)}</span></td>
                      <td>{h.tables_loaded ?? "\u2014"}</td>
                      <td>{h.rows_transferred?.toLocaleString() ?? "\u2014"}</td>
                      <td>{h.views_created ?? "\u2014"}</td>
                      <td>{h.procedures_migrated ?? "\u2014"}</td>
                      <td>{(h.issues_auto_fixed || 0) + (h.issues_human_resolved || 0)}</td>
                      <td>{h.tokens_used?.toLocaleString() ?? "\u2014"}</td>
                      <td>${h.estimated_cost_usd ?? "0"}</td>
                      <td className={h.errors_count > 0 ? "td-error" : ""}>{h.errors_count ?? 0}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        )}

        {error && (
          <div className="error-banner" onClick={() => setError("")}>
            {error} <span className="dismiss">(click to dismiss)</span>
          </div>
        )}
      </main>

      <footer className="footer">
        <p>HealthDB POC | MSSQL to Databricks Migration | Multi-Agent | {new Date().getFullYear()}</p>
      </footer>
    </div>
  );
}

export default App;
