import { useEffect, useState } from "react";
import { createAssetNote, getAssetPassport, rebuildAssetPassport } from "../api.js";
import Card from "../components/Card.jsx";
import ConfidenceBar from "../components/ConfidenceBar.jsx";
import SectionLabel from "../components/SectionLabel.jsx";
import StatusBadge from "../components/StatusBadge.jsx";
import Tag from "../components/Tag.jsx";
import { formatNumber, formatPercent, formatRelativeTime, formatValue, humanizeRootCause, toneFromStatus, titleFromAsset } from "../utils.js";

function PassportIdentity({ asset, passport }) {
  const identity = passport.identity || {};
  const connectivity = passport.connectivity || {};
  return (
    <Card className="overview-hero">
      <div className="overview-hero__content">
        <SectionLabel>Digital Passport</SectionLabel>
        <h2>{titleFromAsset(asset)}</h2>
        <p>
          Living operational record built from the machine registry, semantic discovery, anomaly analytics and observability
          quality indicators.
        </p>
        <div className="overview-hero__tags">
          <Tag tone="blue" label={(identity.asset_type || asset.asset_type || "generic").toUpperCase()} />
          <Tag tone={connectivity.connection_status === "connected" ? "green" : "orange"} label={(connectivity.connection_status || "unknown").toUpperCase()} />
        </div>
      </div>

      <div className="passport-summary">
        <div className="passport-summary__row">
          <span>Manufacturer</span>
          <strong>{identity.manufacturer}</strong>
        </div>
        <div className="passport-summary__row">
          <span>Model</span>
          <strong>{identity.model}</strong>
        </div>
        <div className="passport-summary__row">
          <span>Serial</span>
          <strong>{identity.serial_number}</strong>
        </div>
        <div className="passport-summary__row">
          <span>Location</span>
          <strong>{identity.location}</strong>
        </div>
      </div>
    </Card>
  );
}

function NotesPanel({ assetId, notes, onNoteAdded }) {
  const [note, setNote] = useState("");
  const [busy, setBusy] = useState(false);

  async function handleSubmit() {
    if (!note.trim()) {
      return;
    }
    setBusy(true);
    try {
      await createAssetNote(assetId, { note: note.trim(), author: "operator" });
      setNote("");
      onNoteAdded();
    } finally {
      setBusy(false);
    }
  }

  return (
    <Card>
      <div className="section-header">
        <div>
          <SectionLabel>Notes & Interventions</SectionLabel>
          <h3>Operator context</h3>
        </div>
      </div>

      <div className="notes-form">
        <textarea value={note} onChange={(event) => setNote(event.target.value)} placeholder="Add maintenance notes, operator observations or contextual interventions." />
        <div className="form-actions form-actions--tight">
          <button type="button" className="primary-button" onClick={handleSubmit} disabled={busy}>
            {busy ? "Saving…" : "Add Note"}
          </button>
        </div>
      </div>

      <div className="timeline-list">
        {notes.length ? (
          notes.map((item) => (
            <div className="timeline-list__item" key={item.id}>
              <div className="timeline-list__meta">
                <strong>{item.author}</strong>
                <span>{formatRelativeTime(item.created_at)}</span>
              </div>
              <p>{item.note}</p>
            </div>
          ))
        ) : (
          <div className="chart-empty">No notes have been attached to this machine yet.</div>
        )}
      </div>
    </Card>
  );
}

export default function MachinePassportView({ assetId, registryAsset }) {
  const [payload, setPayload] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  async function handleRebuildPassport() {
    try {
      await rebuildAssetPassport(assetId);
      await loadPassport();
    } catch (requestError) {
      setError("The passport could not be rebuilt right now.");
    }
  }

  async function loadPassport() {
    if (!assetId) {
      return;
    }
    setLoading(true);
    try {
      const response = await getAssetPassport(assetId);
      setPayload(response);
      setError("");
    } catch (requestError) {
      setError("The passport for this asset could not be loaded.");
      setPayload(null);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadPassport();
  }, [assetId]);

  if (!assetId) {
    return <div className="screen-state">Select an asset from the registry to inspect its digital passport.</div>;
  }

  if (loading && !payload) {
    return <div className="screen-state">Loading digital passport…</div>;
  }

  if (error && !payload) {
    return <div className="screen-state screen-state--error">{error}</div>;
  }

  const asset = payload?.asset || registryAsset;
  const passport = payload?.passport || {};
  const diagnostics = passport.diagnostics || {};
  const observability = passport.observability || {};
  const semantic = passport.semantic || {};
  const baseline = passport.baseline || {};
  const events = payload?.events || [];
  const notes = payload?.notes || [];

  return (
    <div className="dashboard-stack">
      <PassportIdentity asset={asset} passport={passport} />

      <div className="kpi-row">
        <Card className="kpi-card">
          <SectionLabel>Health Score</SectionLabel>
          <div className="kpi-card__value">{formatNumber(diagnostics.health_score, 1)}</div>
          <p className="kpi-card__detail">Composite score that combines anomalies and observability quality.</p>
        </Card>
        <Card className="kpi-card">
          <SectionLabel>Coverage</SectionLabel>
          <div className="kpi-card__value">{formatPercent(semantic.coverage_ratio || 0, 1)}</div>
          <p className="kpi-card__detail">Share of discovered nodes mapped into the semantic signal model.</p>
        </Card>
        <Card className="kpi-card">
          <SectionLabel>Signals</SectionLabel>
          <div className="kpi-card__value">{semantic.signal_count || 0}</div>
          <p className="kpi-card__detail">Signals attached to this asset after discovery or live seeding.</p>
        </Card>
        <Card className="kpi-card">
          <SectionLabel>Baseline</SectionLabel>
          <div className="kpi-card__value">{baseline.confidence || 0}%</div>
          <p className="kpi-card__detail">Confidence of the operational fingerprint inferred for the asset.</p>
        </Card>
      </div>

      <Card>
        <div className="split-header">
          <div>
            <SectionLabel>Technical Identity</SectionLabel>
            <h3>Connection and profile</h3>
          </div>
          <button type="button" className="secondary-button" onClick={handleRebuildPassport}>
            Rebuild Passport
          </button>
        </div>
        <div className="metrics-grid">
          <div className="metric-cell">
            <span>Endpoint</span>
            <strong>{passport.connectivity?.opcua_endpoint || "Not configured"}</strong>
          </div>
          <div className="metric-cell">
            <span>Profile</span>
            <strong>{passport.connectivity?.profile_id || "generic"}</strong>
          </div>
          <div className="metric-cell">
            <span>Security</span>
            <strong>{passport.connectivity?.security_mode || "none"}</strong>
          </div>
          <div className="metric-cell">
            <span>Last discovery</span>
            <strong>{formatRelativeTime(passport.connectivity?.last_discovered_at)}</strong>
          </div>
          <div className="metric-cell">
            <span>Last seen</span>
            <strong>{formatRelativeTime(passport.connectivity?.last_seen_at)}</strong>
          </div>
          <div className="metric-cell">
            <span>Status</span>
            <strong>{passport.connectivity?.status || "draft"}</strong>
          </div>
        </div>
      </Card>

      <div className="two-column-grid">
        <Card>
          <div className="section-header">
            <div>
              <SectionLabel>Semantic Coverage</SectionLabel>
              <h3>Signals and subsystems</h3>
            </div>
          </div>
          <div className="distribution-grid">
            {Object.entries(semantic.categories || {}).map(([category, count]) => (
              <div className="distribution-card" key={category}>
                <div className="distribution-card__row">
                  <span className="distribution-card__name">{category}</span>
                  <strong>{count}</strong>
                </div>
              </div>
            ))}
          </div>
          <div className="preview-list">
            {(semantic.signals_preview || []).map((signal) => (
              <div className="preview-list__row" key={`${signal.display_name}-${signal.subsystem}`}>
                <strong>{signal.display_name}</strong>
                <span>
                  {signal.category} · {signal.subsystem} · {signal.unit}
                </span>
              </div>
            ))}
          </div>
        </Card>

        <Card>
          <div className="section-header">
            <div>
              <SectionLabel>Operational Fingerprint</SectionLabel>
              <h3>Learned baseline</h3>
            </div>
            <StatusBadge tone={baseline.status === "ready" ? "nominal" : "warning"} label={baseline.status || "learning"} />
          </div>
          <div className="confidence-block">
            <div className="confidence-block__label">
              <span>Baseline confidence</span>
              <strong>{baseline.confidence || 0}%</strong>
            </div>
            <ConfidenceBar value={baseline.confidence || 0} />
          </div>
          <div className="preview-list">
            {(baseline.signals || []).map((signal) => (
              <div className="preview-list__row" key={signal.signal_key}>
                <strong>{signal.display_name}</strong>
                <span>
                  Min {formatValue(signal.min_value)} · Max {formatValue(signal.max_value)} · Avg {formatValue(signal.avg_value)}
                </span>
              </div>
            ))}
          </div>
        </Card>
      </div>

      <div className="two-column-grid">
        <Card>
          <div className="split-header">
          <div>
            <SectionLabel>Diagnostics</SectionLabel>
            <h3>{humanizeRootCause(diagnostics.root_cause || "nominal")}</h3>
            <p className="card-copy">{diagnostics.summary}</p>
          </div>
          <StatusBadge
            tone={toneFromStatus(diagnostics.active_anomalies > 0 ? "warning" : "nominal")}
            label={diagnostics.active_anomalies > 0 ? "active anomalies" : "stable"}
          />
        </div>
          <div className="confidence-block">
            <div className="confidence-block__label">
              <span>Monitoring confidence</span>
              <strong>{diagnostics.monitoring_confidence || 0}%</strong>
            </div>
            <ConfidenceBar value={diagnostics.monitoring_confidence || 0} />
          </div>
          <div className="preview-list">
            {(diagnostics.priority_signals || []).map((signal) => (
              <div className="preview-list__row" key={signal.display_name}>
                <strong>{signal.display_name}</strong>
                <span>
                  {formatValue(signal.value, signal.unit)} · {signal.severity} · score {formatNumber(signal.anomaly_score, 2)}
                </span>
              </div>
            ))}
          </div>
        </Card>

        <Card>
          <div className="section-header">
            <div>
              <SectionLabel>Observability Quality</SectionLabel>
              <h3>Pipeline and exporter health</h3>
            </div>
            <StatusBadge tone={observability.exporter_reachable ? "nominal" : "critical"} label={observability.exporter_reachable ? "reachable" : "degraded"} />
          </div>
          <div className="metrics-grid">
            <div className="metric-cell">
              <span>Scrape success</span>
              <strong>{formatPercent(observability.scrape_success || 0, 1)}</strong>
            </div>
            <div className="metric-cell">
              <span>Scrape duration</span>
              <strong>{formatNumber(observability.scrape_duration_seconds, 3)} s</strong>
            </div>
            <div className="metric-cell">
              <span>Exporter CPU</span>
              <strong>{formatNumber(observability.exporter_cpu_rate, 3)}</strong>
            </div>
            <div className="metric-cell">
              <span>Exporter memory</span>
              <strong>{formatNumber(observability.exporter_memory_mb, 1)} MB</strong>
            </div>
            <div className="metric-cell">
              <span>Analytics CPU</span>
              <strong>{formatNumber(observability.analytics_cpu_rate, 3)}</strong>
            </div>
            <div className="metric-cell">
              <span>Analytics memory</span>
              <strong>{formatNumber(observability.analytics_memory_mb, 1)} MB</strong>
            </div>
          </div>
        </Card>
      </div>

      <div className="two-column-grid">
        <Card>
          <div className="section-header">
            <div>
              <SectionLabel>Recent Events</SectionLabel>
              <h3>Automatic history</h3>
            </div>
          </div>
          <div className="timeline-list">
            {events.length ? (
              events.map((event) => (
                <div className="timeline-list__item" key={event.id}>
                  <div className="timeline-list__meta">
                    <StatusBadge tone={event.severity === "critical" ? "critical" : event.severity === "warning" ? "warning" : "nominal"} label={event.event_type} />
                    <span>{formatRelativeTime(event.created_at)}</span>
                  </div>
                  <strong>{event.title}</strong>
                  <p>{event.detail}</p>
                </div>
              ))
            ) : (
              <div className="chart-empty">No automatic events have been recorded for this asset yet.</div>
            )}
          </div>
        </Card>

        <NotesPanel assetId={assetId} notes={notes} onNoteAdded={loadPassport} />
      </div>
    </div>
  );
}
