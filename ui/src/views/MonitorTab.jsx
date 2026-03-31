import { useDeferredValue, useEffect, useMemo, useState } from "react";
import { createAssetNote, getSignalSeries } from "../api.js";
import Card from "../components/Card.jsx";
import DiagnosisBanner from "../components/DiagnosisBanner.jsx";
import MetricCard from "../components/MetricCard.jsx";
import SectionLabel from "../components/SectionLabel.jsx";
import Sparkline from "../components/Sparkline.jsx";
import StatusBadge from "../components/StatusBadge.jsx";
import {
  connectionTypeLabel,
  formatFreshness,
  formatNumber,
  formatPercent,
  formatRelativeTime,
  formatValue,
  humanizeCollectionMode,
  titleFromAsset,
  toneFromConnectorHealth,
  toneFromStatus,
} from "../utils.js";

const CATEGORY_FILTERS = ["all", "sensor", "production", "maintenance", "alarm", "status"];
const WINDOW_OPTIONS = [15, 30, 60];

function SearchIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" aria-hidden="true">
      <circle cx="11" cy="11" r="7" fill="none" stroke="currentColor" strokeWidth="2" />
      <path d="M20 20l-3.5-3.5" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
    </svg>
  );
}

function severityRank(severity) {
  return { critical: 0, warning: 1, nominal: 2 }[severity] ?? 3;
}

function toneColor(severity) {
  if (severity === "critical") {
    return "#dc2626";
  }
  if (severity === "warning") {
    return "#d97706";
  }
  return "#16a34a";
}

function SignalDetailModal({ open, signal, series, loading, minutes, connection, onClose }) {
  useEffect(() => {
    function onKeyDown(event) {
      if (event.key === "Escape") {
        onClose();
      }
    }

    if (!open) {
      return undefined;
    }

    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [open, onClose]);

  if (!open || !signal) {
    return null;
  }

  const seriesValues = (series?.points || []).map((point) => Number(point.value));
  const min = seriesValues.length ? Math.min(...seriesValues) : null;
  const max = seriesValues.length ? Math.max(...seriesValues) : null;
  const latest = seriesValues.length ? seriesValues[seriesValues.length - 1] : null;
  const sourceRef = signal.path || signal.source_ref || signal.signal_key;

  return (
    <div className="modal-backdrop" onClick={onClose} role="presentation">
      <div className="modal-shell" onClick={(event) => event.stopPropagation()} role="dialog" aria-modal="true" aria-label={`${signal.display_name} detail`}>
        <div className="modal-shell__header">
          <div>
            <SectionLabel>Historical Trace</SectionLabel>
            <h3>{signal.display_name}</h3>
            <p className="card-copy">
              {connectionTypeLabel(connection?.connection_type)} · {humanizeCollectionMode(connection?.collection_mode)} · {sourceRef}
            </p>
          </div>
          <div className="modal-shell__header-actions">
            <StatusBadge tone={toneFromStatus(signal.severity)} label={signal.severity} />
            <button type="button" className="modal-close" onClick={onClose}>
              Close
            </button>
          </div>
        </div>

        <div className="signal-detail-grid signal-detail-grid--modal">
          <Card className="signal-detail-card signal-detail-card--trace">
            <div className="signal-detail-meta">
              <span>{connectionTypeLabel(connection?.connection_type)}</span>
              <span>{humanizeCollectionMode(connection?.collection_mode)}</span>
              <span>{sourceRef}</span>
            </div>
            <div className="detail-metrics-row">
              <div className="detail-metric">
                <span>Latest value</span>
                <strong>{formatValue(signal.value, signal.unit)}</strong>
              </div>
              <div className="detail-metric">
                <span>Subsystem</span>
                <strong>{signal.subsystem}</strong>
              </div>
              <div className="detail-metric">
                <span>Detector votes</span>
                <strong>{signal.detector_vote_total}</strong>
              </div>
              <div className="detail-metric">
                <span>Normalized score</span>
                <strong>{formatNumber(signal.anomaly_score, 2)}</strong>
              </div>
            </div>

            {loading ? (
              <div className="empty-block">Loading time series…</div>
            ) : (
              <>
                <Sparkline data={seriesValues} width={340} height={100} color={toneColor(signal.severity)} filled className="sparkline--large" />
                <div className="trace-summary">
                  <span>Min {min === null ? "--" : formatNumber(min, 2)}</span>
                  <span>Max {max === null ? "--" : formatNumber(max, 2)}</span>
                  <span>Latest {latest === null ? "--" : formatNumber(latest, 2)}</span>
                </div>
              </>
            )}
          </Card>

          <Card className="signal-detail-card">
            <div className="card-header-row card-header-row--compact">
              <div>
                <SectionLabel>Detector State</SectionLabel>
                <h3>Current detector outputs</h3>
              </div>
            </div>

            <div className="detector-state-row">
              <div className="detector-state-box">
                <span className="detector-state-box__label">RULES</span>
                <strong>{formatNumber(signal.detectors.rules.score, 2)}</strong>
                <StatusBadge tone={signal.detectors.rules.flag ? "warning" : "nominal"} label={signal.detectors.rules.flag ? "triggered" : "nominal"} />
              </div>
              <div className="detector-state-box detector-state-box--amber">
                <span className="detector-state-box__label">ZSCORE</span>
                <strong>{formatNumber(signal.detectors.zscore.score, 2)}</strong>
                <StatusBadge tone={signal.detectors.zscore.flag ? "warning" : "nominal"} label={signal.detectors.zscore.flag ? "triggered" : "nominal"} />
              </div>
              <div className="detector-state-box detector-state-box--violet">
                <span className="detector-state-box__label">MAD</span>
                <strong>{formatNumber(signal.detectors.mad.score, 2)}</strong>
                <StatusBadge tone={signal.detectors.mad.flag ? "warning" : "nominal"} label={signal.detectors.mad.flag ? "triggered" : "nominal"} />
              </div>
            </div>
          </Card>
        </div>
      </div>
    </div>
  );
}

export default function MonitorTab({ asset, registryAsset, pipeline, passportPayload, onPassportRefresh }) {
  const [category, setCategory] = useState("all");
  const [minutes, setMinutes] = useState(30);
  const [search, setSearch] = useState("");
  const [selectedSignal, setSelectedSignal] = useState("");
  const [modalOpen, setModalOpen] = useState(false);
  const [series, setSeries] = useState(null);
  const [seriesLoading, setSeriesLoading] = useState(false);
  const [note, setNote] = useState("");
  const [noteBusy, setNoteBusy] = useState(false);
  const deferredSearch = useDeferredValue(search);

  useEffect(() => {
    if (!asset?.signals?.length) {
      setSelectedSignal("");
      return;
    }
    const exists = asset.signals.some((signal) => signal.signal_key === selectedSignal);
    if (!selectedSignal || !exists) {
      setSelectedSignal(asset.signals[0].signal_key);
    }
  }, [asset?.asset_id, asset?.signals, selectedSignal]);

  const filteredSignals = useMemo(() => {
    if (!asset?.signals) {
      return [];
    }
    return asset.signals.filter((signal) => {
      const matchesCategory = category === "all" || signal.category === category;
      const haystack = [signal.display_name, signal.signal, signal.subsystem].join(" ").toLowerCase();
      const matchesSearch = !deferredSearch || haystack.includes(deferredSearch.toLowerCase());
      return matchesCategory && matchesSearch;
    });
  }, [asset?.signals, category, deferredSearch]);

  const activeSignal = asset?.signals?.find((signal) => signal.signal_key === selectedSignal) || filteredSignals[0] || null;
  const healthScore = passportPayload?.passport?.diagnostics?.health_score ?? asset?.kpis?.health_score ?? 0;
  const notes = passportPayload?.notes || [];
  const connectivity = passportPayload?.passport?.connectivity || {};
  const observability = asset?.observability || passportPayload?.passport?.observability || {};
  const connectionType = asset?.connection?.connection_type || asset?.primary_connection_type || registryAsset?.primary_connection_type || connectivity.primary_connection_type || "unknown";
  const collectionMode =
    asset?.connection?.collection_mode || observability.collection_mode || connectivity.collection_mode || (connectionType === "mqtt" ? "subscription" : "scrape");
  const connectorHealth = asset?.connection?.connector_health || observability.connector_health || connectivity.connector_health || "unknown";
  const connectorStatus = asset?.connection?.connector_status || observability.connector_status || connectivity.connection_status || registryAsset?.connection_status || "unknown";
  const continuityScore = asset?.connection?.continuity_score ?? observability.continuity_score;
  const freshnessSeconds = asset?.connection?.freshness_seconds ?? observability.freshness_seconds;
  const rawScrapeSuccess = observability.scrape_success ?? pipeline.exporter_scrape_success ?? 0;
  const scrapeSuccessPercent = rawScrapeSuccess > 1 ? rawScrapeSuccess : rawScrapeSuccess * 100;
  const connection = {
    connection_type: connectionType,
    collection_mode: collectionMode,
    connector_health: connectorHealth,
    connector_status: connectorStatus,
    continuity_score: continuityScore,
    continuity_label: asset?.connection?.continuity_label || observability.continuity_label || (connectionType === "mqtt" ? "message continuity" : "sample continuity"),
    endpoint_or_host: asset?.connection?.endpoint_or_host || connectivity.endpoint || registryAsset?.opcua_endpoint || "",
    topic_root: asset?.connection?.topic_root || connectivity.topic_root || "",
    broker_url: asset?.connection?.broker_url || connectivity.broker_url || "",
    client_id: asset?.connection?.client_id || connectivity.client_id || "",
    freshness_seconds: freshnessSeconds,
    last_seen_at: asset?.connection?.last_seen_at || observability.last_seen_at || registryAsset?.last_seen_at || connectivity.last_seen_at || "",
  };

  useEffect(() => {
    let cancelled = false;

    async function loadSeries() {
      if (!asset?.asset_id || !activeSignal) {
        setSeries(null);
        return;
      }
      setSeriesLoading(true);
      try {
        const payload = await getSignalSeries({
          assetId: asset.asset_id,
          signal: activeSignal.signal,
          path: activeSignal.path || "",
          minutes,
        });
        if (!cancelled) {
          setSeries(payload);
        }
      } catch (requestError) {
        if (!cancelled) {
          setSeries(null);
        }
      } finally {
        if (!cancelled) {
          setSeriesLoading(false);
        }
      }
    }

    loadSeries();
    return () => {
      cancelled = true;
    };
  }, [activeSignal, asset?.asset_id, minutes]);

  if (!asset) {
    return <div className="screen-state">Select a machine to open the live monitoring workspace.</div>;
  }

  async function handleAddNote() {
    if (!note.trim()) {
      return;
    }
    setNoteBusy(true);
    try {
      await createAssetNote(asset.asset_id, { note: note.trim(), author: "operator" });
      setNote("");
      await onPassportRefresh(asset.asset_id);
    } finally {
      setNoteBusy(false);
    }
  }

  const prioritySignals = [...asset.signals]
    .sort((left, right) => {
      const severityDelta = severityRank(left.severity) - severityRank(right.severity);
      if (severityDelta !== 0) {
        return severityDelta;
      }
      return (right.anomaly_score || 0) - (left.anomaly_score || 0);
    })
    .slice(0, 6);

  const pipelineCells = [
    { label: "Scrape success", value: formatPercent(scrapeSuccessPercent, 1) },
    { label: "Scrape duration", value: `${formatNumber(observability.scrape_duration_seconds ?? pipeline.exporter_scrape_duration_seconds, 3)} s` },
    { label: "Exporter CPU", value: formatNumber(observability.exporter_cpu_rate ?? pipeline.exporter_cpu_rate, 3) },
    { label: "Exporter mem", value: `${formatNumber(observability.exporter_memory_mb ?? pipeline.exporter_memory_mb, 1)} MB` },
    { label: "Analytics CPU", value: formatNumber(observability.analytics_cpu_rate ?? pipeline.analytics_cpu_rate, 3) },
    { label: "Analytics mem", value: `${formatNumber(observability.analytics_memory_mb ?? pipeline.analytics_memory_mb, 1)} MB` },
  ];

  return (
    <>
      <div className="tab-stack">
        <DiagnosisBanner
          assetName={titleFromAsset(registryAsset || asset)}
          status={asset.status}
          anomalyCount={asset.diagnosis.active_anomalies}
          confidence={asset.diagnosis.monitoring_confidence * 100}
          subtitle={`${titleFromAsset(registryAsset || asset)} · ${connectionTypeLabel(connectionType)} · ${humanizeCollectionMode(collectionMode)} · connector ${connectorHealth} · active anomalies ${asset.diagnosis.active_anomalies}`}
        />

        <div className="metrics-row">
          <MetricCard eyebrow="Active anomalies" value={asset.kpis.active_anomalies} detail="Current anomaly count" tone="red" />
          <MetricCard eyebrow="Signals tracked" value={asset.kpis.signals_tracked} detail="Semantic telemetry available" tone="teal" />
          <MetricCard eyebrow="Detector votes" value={asset.kpis.detector_votes} detail="Positive detector votes" tone="amber" />
          <MetricCard eyebrow="Health score" value={formatNumber(healthScore, 1)} detail="Composite machine health" tone="green" />
        </div>

        <div className="monitor-layout">
          <div className="monitor-top-grid">
            <Card className="monitor-risk-card">
              <div className="card-header-row">
                <div>
                  <SectionLabel>Signals At Risk</SectionLabel>
                  <h3>Priority signals</h3>
                </div>
              </div>
              <div className="signal-risk-list">
                {prioritySignals.map((signal) => (
                  <div className="signal-risk-row" key={signal.signal_key}>
                    <div>
                      <strong>{signal.display_name}</strong>
                      <span>
                        {signal.category} · {signal.subsystem} · {connectionTypeLabel(connectionType)}
                      </span>
                    </div>
                    <span className="signal-value">{formatValue(signal.value, signal.unit)}</span>
                    <StatusBadge tone={toneFromStatus(signal.severity)} label={signal.severity} />
                  </div>
                ))}
              </div>
            </Card>

            <div className="monitor-side-stack">
              <Card>
                <div className="card-header-row">
                  <div>
                    <SectionLabel>Pipeline State</SectionLabel>
                    <h3>Runtime health</h3>
                  </div>
                  <span className={`inline-pill${connectorHealth === "outage" ? " inline-pill--critical" : ""}`}>
                    {connectorHealth === "healthy" ? "Healthy" : connectorHealth === "degraded" ? "Degraded" : "Outage"}
                  </span>
                </div>
                <div className="pipeline-grid">
                  {pipelineCells.map((cell) => (
                    <div className="pipeline-grid__cell" key={cell.label}>
                      <dt>{cell.label}</dt>
                      <dd>{cell.value}</dd>
                    </div>
                  ))}
                </div>
              </Card>
            </div>
          </div>

          <Card className="monitor-explorer-card">
            <div className="card-header-row">
              <div>
                <SectionLabel>Signals Explorer</SectionLabel>
                <h3>All signals</h3>
              </div>
            </div>

            <div className="signals-toolbar">
              <label className="search-field search-field--compact">
                <span className="search-field__icon">
                  <SearchIcon />
                </span>
                <input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search signal or subsystem" />
              </label>
            </div>

            <div className="signals-filter-row">
              <div className="chip-row">
                {CATEGORY_FILTERS.map((item) => (
                  <button
                    key={item}
                    type="button"
                    className={`filter-chip${category === item ? " filter-chip--active" : ""}`}
                    onClick={() => setCategory(item)}
                  >
                    {item}
                  </button>
                ))}
              </div>

              <div className="chip-row chip-row--window">
                {WINDOW_OPTIONS.map((item) => (
                  <button
                    key={item}
                    type="button"
                    className={`window-chip${minutes === item ? " window-chip--active" : ""}`}
                    onClick={() => setMinutes(item)}
                  >
                    {item}m
                  </button>
                ))}
              </div>
            </div>

            <div className="signals-table">
              {filteredSignals.map((signal) => (
                <button
                  type="button"
                  key={signal.signal_key}
                  className={`signals-table__row${selectedSignal === signal.signal_key ? " signals-table__row--selected" : ""}`}
                  onClick={() => setSelectedSignal(signal.signal_key)}
                  onDoubleClick={() => {
                    setSelectedSignal(signal.signal_key);
                    setModalOpen(true);
                  }}
                >
                  <span className="signals-table__spark">
                    <Sparkline data={signal.trend || [signal.value]} width={32} height={20} color={toneColor(signal.severity)} />
                  </span>
                  <span className="signals-table__meta">
                    <strong>{signal.display_name}</strong>
                    <span>
                      {signal.category} · {signal.subsystem} · {connectionTypeLabel(connectionType)}
                    </span>
                  </span>
                  <span className="signal-value">{formatValue(signal.value, signal.unit)}</span>
                  <StatusBadge tone={toneFromStatus(signal.severity)} label={signal.severity} />
                </button>
              ))}
            </div>
          </Card>

          <Card className="monitor-context-card">
            <div className="card-header-row">
              <div>
                <SectionLabel>Operator Context</SectionLabel>
                <h3>Connector context and notes</h3>
              </div>
            </div>

            <div className="context-grid">
              <div className="context-grid__cell">
                <dt>Protocol</dt>
                <dd>{connectionTypeLabel(connectionType)}</dd>
              </div>
              <div className="context-grid__cell">
                <dt>Collection</dt>
                <dd>{humanizeCollectionMode(collectionMode)}</dd>
              </div>
              <div className="context-grid__cell">
                <dt>Connector health</dt>
                <dd>
                  <StatusBadge tone={toneFromConnectorHealth(connectorHealth)} label={connectorHealth} />
                </dd>
              </div>
              <div className="context-grid__cell">
                <dt>{connection.continuity_label}</dt>
                <dd>{continuityScore === null || continuityScore === undefined ? "--" : formatPercent(continuityScore, 1)}</dd>
              </div>
              <div className="context-grid__cell">
                <dt>Freshness</dt>
                <dd>{formatFreshness(connection.freshness_seconds)}</dd>
              </div>
              <div className="context-grid__cell context-grid__cell--wide">
                <dt>{connectionType === "mqtt" ? "Broker" : "Endpoint"}</dt>
                <dd>{connectionType === "mqtt" ? connection.broker_url || connection.endpoint_or_host || "--" : connection.endpoint_or_host || "--"}</dd>
              </div>
              {connectionType === "mqtt" ? (
                <div className="context-grid__cell context-grid__cell--wide">
                  <dt>Topic root</dt>
                  <dd>{connection.topic_root || "--"}</dd>
                </div>
              ) : null}
            </div>

            <div className="notes-composer">
              <input value={note} onChange={(event) => setNote(event.target.value)} placeholder="Add an operator note…" />
              <button type="button" className="primary-button primary-button--sm" onClick={handleAddNote} disabled={noteBusy}>
                {noteBusy ? "Saving…" : "Add"}
              </button>
            </div>

            <div className="notes-list">
              {notes.length ? (
                notes.map((item) => (
                  <div className="notes-list__item" key={item.id}>
                    <span className="note-timestamp">{formatRelativeTime(item.created_at)}</span>
                    <p>{item.note}</p>
                  </div>
                ))
              ) : (
                <div className="empty-block">No notes have been added yet.</div>
              )}
            </div>
          </Card>
        </div>
      </div>

      <SignalDetailModal open={modalOpen} signal={activeSignal} series={series} loading={seriesLoading} minutes={minutes} connection={connection} onClose={() => setModalOpen(false)} />
    </>
  );
}
