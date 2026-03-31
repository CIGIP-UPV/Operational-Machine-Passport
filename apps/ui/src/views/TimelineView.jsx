import { useEffect, useState } from "react";
import { getTimeline } from "../api.js";
import Card from "../components/Card.jsx";
import SectionLabel from "../components/SectionLabel.jsx";
import StatusBadge from "../components/StatusBadge.jsx";
import { formatNumber, formatPercent, formatTimestamp } from "../utils.js";

function truthTone(entry) {
  return entry.mode === "nominal" ? "nominal" : "warning";
}

function diagnosisTone(entry) {
  return entry.active_anomalies > 0 ? "warning" : "nominal";
}

function TimelineEvent({ entry, type, last }) {
  const tone = type === "truth" ? truthTone(entry) : diagnosisTone(entry);
  const headline = type === "truth" ? entry.event_label : entry.top_signal || "No dominant signal";
  const detail =
    type === "truth"
      ? `Elapsed ${entry.elapsed_seconds}s from scenario start.`
      : `Confidence ${formatPercent((entry.monitoring_confidence || 0) * 100)} · active anomalies ${entry.active_anomalies}`;
  const badgeLabel = type === "truth" ? entry.mode : entry.active_anomalies > 0 ? "anomalies" : "stable";

  return (
    <div className="dot-timeline__item">
      <div className="dot-timeline__rail">
        <span className={`dot-timeline__dot dot-timeline__dot--${tone}`} />
        {!last ? <span className="dot-timeline__line" /> : null}
      </div>
      <div className="dot-timeline__content">
        <div className="dot-timeline__top">
          <StatusBadge tone={tone} label={badgeLabel} />
          <span className="dot-timeline__timestamp">{formatTimestamp(entry.timestamp)}</span>
        </div>
        <strong>{headline}</strong>
        <p>{detail}</p>
      </div>
    </div>
  );
}

export default function TimelineView({ asset, registryAsset }) {
  const [timeline, setTimeline] = useState(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    let cancelled = false;

    async function loadTimeline() {
      if (!asset?.asset_id) {
        return;
      }
      setLoading(true);
      try {
        const payload = await getTimeline(asset.asset_id, 20);
        if (!cancelled) {
          setTimeline(payload);
        }
      } catch (error) {
        if (!cancelled) {
          setTimeline(null);
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    loadTimeline();
    return () => {
      cancelled = true;
    };
  }, [asset?.asset_id]);

  const summary = timeline?.summary || {};
  const truthEvents = timeline?.ground_truth || [];
  const diagnosisEvents = timeline?.analytics || [];

  return (
    <div className="dashboard-stack">
      <Card className="timeline-summary-card">
        <div className="split-header">
          <div>
            <SectionLabel>Experiment Timeline</SectionLabel>
            <h3>{registryAsset?.display_name ? `${registryAsset.display_name} scenario and detection alignment` : "Scenario and detection alignment"}</h3>
          </div>
          <span className="experiment-pill">DETECTION OBSERVED</span>
        </div>

        <div className="metrics-grid metrics-grid--triple">
          <div className="metric-cell">
            <span>First fault</span>
            <strong>{formatTimestamp(summary.first_fault_at)}</strong>
          </div>
          <div className="metric-cell">
            <span>First detection</span>
            <strong>{formatTimestamp(summary.first_detection_at)}</strong>
          </div>
          <div className="metric-cell metric-cell--alert">
            <span>Detection delay</span>
            <strong>{summary.detection_delay_seconds === null ? "--" : `${formatNumber(summary.detection_delay_seconds, 1)} s`}</strong>
          </div>
        </div>
      </Card>

      <div className="two-column-grid">
        <Card>
          <div className="section-header">
            <div>
              <SectionLabel>Ground Truth</SectionLabel>
              <h3>Scenario events</h3>
            </div>
          </div>

          {loading ? (
            <div className="chart-empty">Loading scenario events…</div>
          ) : (
            <div className="dot-timeline">
              {truthEvents.map((entry, index) => (
                <TimelineEvent
                  entry={entry}
                  key={`${entry.timestamp}-${entry.elapsed_seconds}`}
                  type="truth"
                  last={index === truthEvents.length - 1}
                />
              ))}
            </div>
          )}
        </Card>

        <Card>
          <div className="section-header">
            <div>
              <SectionLabel>Analytics History</SectionLabel>
              <h3>Diagnosis states</h3>
            </div>
          </div>

          {loading ? (
            <div className="chart-empty">Loading diagnosis states…</div>
          ) : (
            <div className="dot-timeline">
              {diagnosisEvents.map((entry, index) => (
                <TimelineEvent
                  entry={entry}
                  key={`${entry.timestamp}-${entry.root_cause}-${index}`}
                  type="diagnosis"
                  last={index === diagnosisEvents.length - 1}
                />
              ))}
            </div>
          )}
        </Card>
      </div>
    </div>
  );
}
