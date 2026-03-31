import { useEffect, useState } from "react";
import { getTimeline } from "../api.js";
import Card from "../components/Card.jsx";
import CategoryBadge from "../components/CategoryBadge.jsx";
import DiagnosisBanner from "../components/DiagnosisBanner.jsx";
import SectionLabel from "../components/SectionLabel.jsx";
import Sparkline from "../components/Sparkline.jsx";
import StatusBadge from "../components/StatusBadge.jsx";
import {
  connectionTypeLabel,
  formatFreshness,
  formatNumber,
  formatPercent,
  formatTimestamp,
  formatValue,
  humanizeCollectionMode,
  humanizeRootCause,
  titleFromAsset,
  toneFromConnectorHealth,
  toneFromStatus,
} from "../utils.js";

function severityRank(severity) {
  return { critical: 0, warning: 1, nominal: 2 }[severity] ?? 3;
}

function EvidenceEmptyState() {
  return (
    <div className="evidence-empty">
      <svg width="32" height="32" viewBox="0 0 24 24" aria-hidden="true">
        <circle cx="12" cy="12" r="9" fill="none" stroke="currentColor" strokeWidth="1.5" opacity="0.4" />
        <path d="M12 8v8M8 12h8" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" opacity="0.4" />
      </svg>
      <p>No active evidences were generated in the latest cycle.</p>
    </div>
  );
}

function TimelineItem({ entry, type, last, defaultConnectionType, defaultContinuityLabel }) {
  const truthMode = entry.mode === "nominal" ? "nominal" : "warning";
  const diagnosisTone = entry.active_anomalies > 0 ? "warning" : "nominal";
  const tone = type === "truth" ? truthMode : diagnosisTone;
  const badge = type === "truth" ? entry.mode : entry.active_anomalies > 0 ? "anomalies" : "stable";
  const headline = type === "truth" ? entry.event_label : humanizeRootCause(entry.root_cause || "nominal");
  const detail =
    type === "truth"
      ? `Elapsed ${entry.elapsed_seconds}s from scenario start.`
      : `Confidence ${formatPercent((entry.monitoring_confidence || 0) * 100)} · active anomalies ${entry.active_anomalies} · ${connectionTypeLabel(entry.connection_type || defaultConnectionType)} · ${(entry.connector_health || entry.connector_status || "unknown")} · ${(entry.continuity_label || defaultContinuityLabel || "continuity")} ${entry.continuity_score === null || entry.continuity_score === undefined ? "--" : formatPercent(entry.continuity_score, 1)}`;

  return (
    <div className="dot-timeline__item">
      <div className="dot-timeline__rail">
        <span className={`dot-timeline__dot dot-timeline__dot--${tone}`} />
        {!last ? <span className="dot-timeline__line" /> : null}
      </div>
      <div className="dot-timeline__content">
        <div className="dot-timeline__top">
          <StatusBadge tone={tone} label={badge} />
          <span className="dot-timeline__timestamp">{formatTimestamp(entry.timestamp)}</span>
        </div>
        <strong>{headline}</strong>
        <p>{detail}</p>
      </div>
    </div>
  );
}

export default function DiagnoseTab({ asset, registryAsset, passportPayload }) {
  const [timeline, setTimeline] = useState(null);
  const [loadingTimeline, setLoadingTimeline] = useState(false);

  useEffect(() => {
    let cancelled = false;

    async function loadTimeline() {
      if (!asset?.asset_id) {
        setTimeline(null);
        return;
      }
      setLoadingTimeline(true);
      try {
        const payload = await getTimeline(asset.asset_id, 20);
        if (!cancelled) {
          setTimeline(payload);
        }
      } catch (requestError) {
        if (!cancelled) {
          setTimeline(null);
        }
      } finally {
        if (!cancelled) {
          setLoadingTimeline(false);
        }
      }
    }

    loadTimeline();
    return () => {
      cancelled = true;
    };
  }, [asset?.asset_id]);

  if (!asset) {
    return <div className="screen-state">Select a machine to investigate diagnosis and experiment alignment.</div>;
  }

  const connectivity = passportPayload?.passport?.connectivity || {};
  const observability = asset.observability || passportPayload?.passport?.observability || {};
  const connectionType = asset.connection?.connection_type || asset.primary_connection_type || registryAsset?.primary_connection_type || connectivity.primary_connection_type || "unknown";
  const collectionMode = asset.connection?.collection_mode || observability.collection_mode || connectivity.collection_mode || (connectionType === "mqtt" ? "subscription" : "scrape");
  const connectorHealth = asset.connection?.connector_health || observability.connector_health || connectivity.connector_health || "unknown";
  const connectorStatus = asset.connection?.connector_status || observability.connector_status || connectivity.connection_status || registryAsset?.connection_status || "unknown";
  const continuityScore = asset.connection?.continuity_score ?? observability.continuity_score;
  const continuityLabel = asset.connection?.continuity_label || observability.continuity_label || (connectionType === "mqtt" ? "message continuity" : "sample continuity");
  const freshnessSeconds = asset.connection?.freshness_seconds ?? observability.freshness_seconds;

  const matrixSignals = [...asset.signals]
    .filter((signal) => !["alarm", "status", "energy"].includes(signal.category))
    .sort((left, right) => {
      const severityDelta = severityRank(left.severity) - severityRank(right.severity);
      if (severityDelta !== 0) {
        return severityDelta;
      }
      return (right.anomaly_score || 0) - (left.anomaly_score || 0);
    });

  const summary = timeline?.summary || {};
  const timelineContext = timeline?.context || {};
  const truthEvents = timeline?.ground_truth || [];
  const diagnosisEvents = timeline?.analytics || [];
  const subtitle = `${titleFromAsset(registryAsset || asset)} · ${connectionTypeLabel(connectionType)} · ${humanizeCollectionMode(collectionMode)} · connector ${connectorHealth} · top signal ${asset.diagnosis.top_signal || "--"}`;

  return (
    <div className="tab-stack">
      <DiagnosisBanner
        assetName={titleFromAsset(registryAsset || asset)}
        status={asset.status}
        anomalyCount={asset.diagnosis.active_anomalies}
        confidence={asset.diagnosis.monitoring_confidence * 100}
        subtitle={subtitle}
      />

      <Card>
        <div className="card-header-row">
          <div>
            <SectionLabel>Diagnosis Context</SectionLabel>
            <h3>Protocol and connector quality</h3>
          </div>
        </div>

        <div className="diagnosis-context-grid">
          <div className="diagnosis-context-grid__cell">
            <span>Protocol</span>
            <strong>{connectionTypeLabel(connectionType)}</strong>
          </div>
          <div className="diagnosis-context-grid__cell">
            <span>Collection mode</span>
            <strong>{humanizeCollectionMode(collectionMode)}</strong>
          </div>
          <div className="diagnosis-context-grid__cell">
            <span>Connector health</span>
            <StatusBadge tone={toneFromConnectorHealth(connectorHealth)} label={connectorHealth} />
          </div>
          <div className="diagnosis-context-grid__cell">
            <span>Connector status</span>
            <strong>{connectorStatus}</strong>
          </div>
          <div className="diagnosis-context-grid__cell">
            <span>{continuityLabel}</span>
            <strong>{continuityScore === null || continuityScore === undefined ? "--" : formatPercent(continuityScore, 1)}</strong>
          </div>
          <div className="diagnosis-context-grid__cell">
            <span>Freshness</span>
            <strong>{formatFreshness(freshnessSeconds)}</strong>
          </div>
        </div>
      </Card>

      <Card>
        <div className="card-header-row">
          <div>
            <SectionLabel>Evidence Ledger</SectionLabel>
            <h3>Active evidences</h3>
          </div>
        </div>

        {asset.diagnosis.evidences.length ? (
          <div className="evidence-grid">
            {asset.diagnosis.evidences.map((evidence) => (
              <article
                className={`evidence-card evidence-card--${evidence.severity === "critical" ? "critical" : "warning"}`}
                key={`${evidence.signal}-${evidence.label}`}
              >
                <div className="evidence-card__header">
                  <StatusBadge tone={evidence.severity === "critical" ? "critical" : "warning"} label={evidence.severity} />
                </div>
                <strong>{evidence.label}</strong>
                <p>{evidence.reason}</p>
                <span className="evidence-card__value">{formatValue(evidence.value, evidence.unit)}</span>
              </article>
            ))}
          </div>
        ) : (
          <EvidenceEmptyState />
        )}
      </Card>

      <Card>
        <div className="card-header-row">
          <div>
            <SectionLabel>Detector Matrix</SectionLabel>
            <h3>Signal-level detector outputs</h3>
          </div>
        </div>

        <div className="table-shell">
          <table className="detector-table" role="table">
            <thead>
              <tr>
                <th>Signal</th>
                <th>Category</th>
                <th>Latest value</th>
                <th>Rules</th>
                <th>Z-Score</th>
                <th>MAD</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {matrixSignals.map((signal) => (
                <tr key={signal.signal_key}>
                  <td>
                    <div className="matrix-signal">
                      <Sparkline data={signal.trend || [signal.value]} width={40} height={16} color="#9ca3af" />
                      <div className="matrix-signal__meta">
                        <span>{signal.display_name}</span>
                        <small>{connectionTypeLabel(signal.connection_type || connectionType)} · {signal.subsystem}</small>
                      </div>
                    </div>
                  </td>
                  <td>
                    <CategoryBadge category={signal.category} />
                  </td>
                  <td className="table-value">{formatValue(signal.value, signal.unit)}</td>
                  <td>{formatNumber(signal.detectors.rules.score, 0)}</td>
                  <td className={signal.detectors.zscore.score > 1.5 ? "table-score table-score--highlight" : "table-score"}>
                    {formatNumber(signal.detectors.zscore.score, 2)}
                  </td>
                  <td className={signal.detectors.mad.score > 1.3 ? "table-score table-score--highlight" : "table-score"}>
                    {formatNumber(signal.detectors.mad.score, 2)}
                  </td>
                  <td>
                    <StatusBadge tone={toneFromStatus(signal.severity)} label={signal.severity} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card>

      <Card>
        <div className="card-header-row">
          <div>
            <SectionLabel>Experiment Timeline</SectionLabel>
            <h3>Scenario and detection alignment</h3>
          </div>
          <div className="experiment-pill-row">
            <span className="experiment-pill">Detection observed</span>
            <span className="experiment-pill experiment-pill--blue">{connectionTypeLabel(timelineContext.connection_type || connectionType)}</span>
            <span className="experiment-pill experiment-pill--neutral">{humanizeCollectionMode(timelineContext.collection_mode || collectionMode)}</span>
          </div>
        </div>

        <div className="experiment-metrics">
          <div className="experiment-metric experiment-metric--blue">
            <span className="experiment-metric__label">First fault</span>
            <strong className="experiment-metric__value">{formatTimestamp(summary.first_fault_at)}</strong>
          </div>
          <div className="experiment-metric experiment-metric--coral">
            <span className="experiment-metric__label">First detection</span>
            <strong className="experiment-metric__value">{formatTimestamp(summary.first_detection_at)}</strong>
          </div>
          <div className="experiment-metric experiment-metric--green">
            <span className="experiment-metric__label">Detection delay</span>
            <strong className="experiment-metric__value experiment-metric__value--large">
              {summary.detection_delay_seconds === null ? "--" : `${formatNumber(summary.detection_delay_seconds, 1)} s`}
            </strong>
          </div>
        </div>

        <div className="timeline-context-row">
          <span className="timeline-context-pill">{connectionTypeLabel(timelineContext.connection_type || connectionType)}</span>
          <span className="timeline-context-pill">{humanizeCollectionMode(timelineContext.collection_mode || collectionMode)}</span>
          <span className={`timeline-context-pill timeline-context-pill--${toneFromConnectorHealth(timelineContext.connector_health || connectorHealth)}`}>
            {(timelineContext.connector_health || connectorHealth || "unknown").toUpperCase()}
          </span>
          <span className="timeline-context-pill">
            {(timelineContext.continuity_label || continuityLabel) || "continuity"}{" "}
            {timelineContext.continuity_score === null || timelineContext.continuity_score === undefined
              ? "--"
              : formatPercent(timelineContext.continuity_score, 1)}
          </span>
        </div>

        <div className="timeline-columns">
          <div className="timeline-column">
            <div className="timeline-column__header">
              <SectionLabel>Ground Truth</SectionLabel>
              <h4>Scenario events</h4>
            </div>
            {loadingTimeline ? (
              <div className="empty-block">Loading scenario events…</div>
            ) : (
              <div className="dot-timeline">
                {truthEvents.map((entry, index) => (
                  <TimelineItem
                    entry={entry}
                    key={`${entry.timestamp}-${entry.event_label}-${index}`}
                    type="truth"
                    last={index === truthEvents.length - 1}
                    defaultConnectionType={connectionType}
                    defaultContinuityLabel={continuityLabel}
                  />
                ))}
              </div>
            )}
          </div>

          <div className="timeline-column">
            <div className="timeline-column__header">
              <SectionLabel>Analytics History</SectionLabel>
              <h4>Diagnosis states</h4>
            </div>
            {loadingTimeline ? (
              <div className="empty-block">Loading diagnosis states…</div>
            ) : (
              <div className="dot-timeline">
                {diagnosisEvents.map((entry, index) => (
                  <TimelineItem
                    entry={entry}
                    key={`${entry.timestamp}-${entry.root_cause}-${index}`}
                    type="diagnosis"
                    last={index === diagnosisEvents.length - 1}
                    defaultConnectionType={connectionType}
                    defaultContinuityLabel={continuityLabel}
                  />
                ))}
              </div>
            )}
          </div>
        </div>
      </Card>
    </div>
  );
}
