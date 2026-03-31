import Card from "../components/Card.jsx";
import ConfidenceBar from "../components/ConfidenceBar.jsx";
import SectionLabel from "../components/SectionLabel.jsx";
import Sparkline from "../components/Sparkline.jsx";
import StatusBadge from "../components/StatusBadge.jsx";
import { formatNumber, formatPercent, formatValue, toneFromStatus, humanizeRootCause } from "../utils.js";

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

function EvidenceCard({ evidence }) {
  return (
    <article className="evidence-card">
      <div className="evidence-card__header">
        <StatusBadge tone={evidence.severity === "critical" ? "critical" : "warning"} label={evidence.severity} />
      </div>
      <strong>{evidence.label}</strong>
      <p>{evidence.reason}</p>
      <span className="evidence-card__value">{formatValue(evidence.value, evidence.unit)}</span>
    </article>
  );
}

export default function DiagnosisView({ asset, registryAsset }) {
  const confidencePercent = Math.round(asset.diagnosis.monitoring_confidence * 100);
  const matrixSignals = asset.signals.filter((signal) => !["alarm", "status", "energy"].includes(signal.category));

  return (
    <div className="dashboard-stack">
      <Card>
        <div className="split-header">
          <div>
            <SectionLabel>Explainable Diagnosis</SectionLabel>
            <h2>{humanizeRootCause(asset.diagnosis.root_cause)}</h2>
            <p className="card-copy">{registryAsset?.display_name ? `${registryAsset.display_name}. ` : ""}{asset.diagnosis.summary}</p>
          </div>
          <StatusBadge tone={toneFromStatus(asset.status)} label={asset.status} size="md" />
        </div>

        <div className="confidence-block">
          <div className="confidence-block__label">
            <span>Monitoring confidence</span>
            <strong>{formatPercent(confidencePercent)}</strong>
          </div>
          <ConfidenceBar value={confidencePercent} />
        </div>

        <div className="metrics-grid metrics-grid--triple">
          <div className="metric-cell">
            <span>Top signal</span>
            <strong>{asset.diagnosis.top_signal || "--"}</strong>
          </div>
          <div className="metric-cell">
            <span>Active anomalies</span>
            <strong>{asset.diagnosis.active_anomalies}</strong>
          </div>
          <div className="metric-cell">
            <span>Vote ratio</span>
            <strong>{formatNumber(asset.diagnosis.vote_ratio, 2)}</strong>
          </div>
        </div>
      </Card>

      <Card>
        <div className="section-header">
          <div>
            <SectionLabel>Evidence Ledger</SectionLabel>
            <h3>Active evidences</h3>
          </div>
        </div>
        {asset.diagnosis.evidences.length ? (
          <div className="evidence-grid">
            {asset.diagnosis.evidences.map((evidence) => (
              <EvidenceCard evidence={evidence} key={`${evidence.signal}-${evidence.label}`} />
            ))}
          </div>
        ) : (
          <EvidenceEmptyState />
        )}
      </Card>

      <Card>
        <div className="section-header">
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
                <th>Latest Value</th>
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
                      <Sparkline data={signal.trend || [signal.value]} width={40} height={16} color="#94a3b8" />
                      <span>{signal.display_name}</span>
                    </div>
                  </td>
                  <td>
                    <span className="category-pill">{signal.category}</span>
                  </td>
                  <td className="table-value">{formatValue(signal.value, signal.unit)}</td>
                  <td>{formatNumber(signal.detectors.rules.score, 2)}</td>
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
    </div>
  );
}
