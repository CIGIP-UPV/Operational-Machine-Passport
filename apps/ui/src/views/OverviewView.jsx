import Card from "../components/Card.jsx";
import ConfidenceBar from "../components/ConfidenceBar.jsx";
import MetricCard from "../components/MetricCard.jsx";
import SectionLabel from "../components/SectionLabel.jsx";
import Sparkline from "../components/Sparkline.jsx";
import StatusBadge from "../components/StatusBadge.jsx";
import Tag from "../components/Tag.jsx";
import { categoryColor, formatNumber, formatPercent, formatValue, humanizeRootCause, titleFromAsset, toneFromStatus } from "../utils.js";

function SignalRow({ signal }) {
  return (
    <li className="risk-row">
      <div className="risk-row__left">
        <strong>{signal.display_name}</strong>
        <span>
          {signal.category} · {signal.subsystem}
        </span>
      </div>
      <div className="risk-row__right">
        <span className="risk-row__value">{formatValue(signal.value, signal.unit)}</span>
        <StatusBadge tone={toneFromStatus(signal.severity)} label={signal.severity} />
      </div>
    </li>
  );
}

export default function OverviewView({ asset, pipeline, registryAsset }) {
  const prioritySignals = [...asset.signals]
    .sort((left, right) => (right.detectors?.zscore?.score || 0) - (left.detectors?.zscore?.score || 0))
    .slice(0, 6);

  const categories = asset.signals.reduce((accumulator, signal) => {
    accumulator[signal.category] = (accumulator[signal.category] || 0) + 1;
    return accumulator;
  }, {});
  const totalSignals = asset.signals.length || 1;

  return (
    <div className="dashboard-stack">
      <Card className="overview-hero">
        <div className="overview-hero__content">
          <SectionLabel>Asset Overview</SectionLabel>
          <h2>{titleFromAsset(registryAsset || asset)}</h2>
          <p>
            Generic industrial observability workspace for OPC UA assets. This view summarizes operational health, semantic
            signals and diagnosis confidence for the connected machine.
          </p>
          <div className="overview-hero__tags">
            <Tag tone={asset.status === "nominal" ? "green" : "orange"} label={asset.status.toUpperCase()} />
            <Tag tone="blue" label={asset.asset_type.toUpperCase()} />
          </div>
        </div>

        <div className="overview-hero__trend">
          <Sparkline data={asset.trend || []} width={220} height={80} filled />
        </div>
      </Card>

      <div className="kpi-row">
        <MetricCard eyebrow="Active Anomalies" value={asset.kpis.active_anomalies} detail="Signals currently under anomaly conditions" tone="green" />
        <MetricCard eyebrow="Monitoring Confidence" value={formatPercent(asset.kpis.monitoring_confidence * 100)} detail="Current diagnosis confidence for the asset" tone="blue" />
        <MetricCard eyebrow="Signals Tracked" value={asset.kpis.signals_tracked} detail="Semantic metrics discovered through the exporter" tone="slate" />
        <MetricCard eyebrow="Detector Votes" value={asset.kpis.detector_votes} detail="Total positive detector votes in the current cycle" tone="muted" />
      </div>

      <Card>
        <div className="split-header">
          <div>
            <SectionLabel>Diagnosis Snapshot</SectionLabel>
            <h3>{humanizeRootCause(asset.diagnosis.root_cause)}</h3>
            <p className="card-copy">{asset.diagnosis.summary}</p>
          </div>
          <StatusBadge tone={toneFromStatus(asset.status)} label={asset.status} size="md" />
        </div>
        <ConfidenceBar value={asset.diagnosis.monitoring_confidence * 100} />
      </Card>

      <div className="two-column-grid">
        <Card>
          <div className="section-header">
            <div>
              <SectionLabel>Signals At Risk</SectionLabel>
              <h3>Priority signals</h3>
            </div>
          </div>
          <ul className="risk-list">
            {prioritySignals.map((signal) => (
              <SignalRow key={signal.signal_key} signal={signal} />
            ))}
          </ul>
        </Card>

        <Card>
          <div className="split-header">
            <div>
              <SectionLabel>Pipeline State</SectionLabel>
              <h3>Runtime health</h3>
            </div>
            <span className="inline-pill">{pipeline.exporter_up ? "EXPORTER REACHABLE" : "EXPORTER DOWN"}</span>
          </div>

          <div className="metrics-grid">
            <div className="metric-cell">
              <span>Scrape success</span>
              <strong>{formatPercent((pipeline.exporter_scrape_success || 0) * 100)}</strong>
            </div>
            <div className="metric-cell">
              <span>Scrape duration</span>
              <strong>{formatNumber(pipeline.exporter_scrape_duration_seconds, 3)} s</strong>
            </div>
            <div className="metric-cell">
              <span>Exporter CPU</span>
              <strong>{formatNumber(pipeline.exporter_cpu_rate, 3)}</strong>
            </div>
            <div className="metric-cell">
              <span>Exporter memory</span>
              <strong>{formatNumber(pipeline.exporter_memory_mb, 1)} MB</strong>
            </div>
            <div className="metric-cell">
              <span>Analytics CPU</span>
              <strong>{formatNumber(pipeline.analytics_cpu_rate, 3)}</strong>
            </div>
            <div className="metric-cell">
              <span>Analytics memory</span>
              <strong>{formatNumber(pipeline.analytics_memory_mb, 1)} MB</strong>
            </div>
          </div>
        </Card>
      </div>

      <Card>
        <div className="section-header">
          <div>
            <SectionLabel>Semantic Distribution</SectionLabel>
            <h3>Signals by category</h3>
          </div>
        </div>

        <div className="distribution-grid">
          {Object.entries(categories).map(([category, count]) => {
            const color = categoryColor(category);
            const ratio = (count / totalSignals) * 100;
            return (
              <div className="distribution-card" key={category}>
                <div className="distribution-card__row">
                  <div className="distribution-card__meta">
                    <span className="distribution-card__dot" style={{ background: color }} />
                    <span className="distribution-card__name">{category}</span>
                  </div>
                  <strong>{count}</strong>
                </div>
                <div className="distribution-card__bar">
                  <div className="distribution-card__fill" style={{ width: `${ratio}%`, background: color }} />
                </div>
              </div>
            );
          })}
        </div>
      </Card>
    </div>
  );
}
