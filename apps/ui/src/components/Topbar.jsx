import { useEffect, useState } from "react";
import { connectionTypeLabel, formatClock, titleFromAsset } from "../utils.js";

function ChevronIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" aria-hidden="true">
      <path d="M7 10l5 5 5-5" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

export default function Topbar({ assetOptions, selectedAssetId, onChangeAsset, currentAsset, pipeline, onOpenRegistry, registryView = false }) {
  const [now, setNow] = useState(Date.now());
  const pipelineConnected = pipeline?.exporter_up && pipeline?.exporter_scrape_success >= 1;
  const hasAssets = assetOptions.length > 0;
  const connectionType = currentAsset?.connection?.connection_type || currentAsset?.primary_connection_type || "unknown";

  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, []);

  return (
    <header className="topbar">
      <div className="logo" aria-label="Operational Machine Passport">
        <span>Operational</span>
        <span className="logo__secondary">Machine Passport</span>
      </div>

      <div className="topbar-divider" />

      <label className="machine-selector">
        <select value={selectedAssetId || ""} onChange={(event) => onChangeAsset(event.target.value)} aria-label="Selected machine" disabled={!hasAssets}>
          {!hasAssets ? <option value="">No machines registered</option> : null}
          {assetOptions.map((asset) => (
            <option key={asset.asset_id} value={asset.asset_id}>
              {titleFromAsset(asset)}
            </option>
          ))}
        </select>
        <span className="machine-selector__text">
          <span className="machine-name">{currentAsset ? titleFromAsset(currentAsset) : "Select machine"}</span>
          <span className="machine-badges">
            <span className="machine-type">{(currentAsset?.asset_type || "generic").toUpperCase()}</span>
            <span className="machine-protocol">{connectionTypeLabel(connectionType)}</span>
          </span>
        </span>
        <span className="machine-selector__icon">
          <ChevronIcon />
        </span>
      </label>

      <button type="button" className={`secondary-button topbar-action${registryView ? " topbar-action--active" : ""}`} onClick={onOpenRegistry}>
        Machine List
      </button>

      <div className="pipeline-status">
        <span className={`pipeline-dot${pipelineConnected ? "" : " pipeline-dot--disconnected"}`} />
        <span className={`pipeline-label${pipelineConnected ? "" : " pipeline-label--disconnected"}`}>
          {pipelineConnected ? "Pipeline connected" : "Disconnected"}
        </span>
        <span className="pipeline-time">{formatClock(now)}</span>
      </div>
    </header>
  );
}
