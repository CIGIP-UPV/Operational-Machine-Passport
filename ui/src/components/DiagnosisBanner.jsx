import ConfidenceBar from "./ConfidenceBar.jsx";

function bannerTone(status, anomalyCount) {
  if (status === "critical") {
    return "critical";
  }
  if (anomalyCount > 0 || status === "warning") {
    return "warning";
  }
  return "nominal";
}

function bannerTitle(status, anomalyCount) {
  if (status === "critical") {
    return "Critical anomaly detected";
  }
  if (anomalyCount > 0 || status === "warning") {
    return "Nominal — with warnings";
  }
  return "Nominal";
}

export default function DiagnosisBanner({ assetName, status, anomalyCount, confidence, subtitle, compact = false }) {
  const tone = bannerTone(status, anomalyCount);
  return (
    <section className={`diagnosis-banner diagnosis-banner--${tone}${compact ? " diagnosis-banner--compact" : ""}`}>
      <div className="diagnosis-banner__left">
        <div className={`diagnosis-banner__icon diagnosis-banner__icon--${tone}`} />
        <div className="diagnosis-banner__text">
          <h2>{bannerTitle(status, anomalyCount)}</h2>
          <p>{subtitle || `${assetName} · confidence ${Math.round(confidence)}% · active anomalies ${anomalyCount}`}</p>
        </div>
      </div>

      <div className="diagnosis-banner__confidence">
        <div className="diagnosis-banner__confidence-label">
          <span>Monitoring confidence</span>
          <strong>{Math.round(confidence)}%</strong>
        </div>
        <ConfidenceBar value={confidence} />
      </div>
    </section>
  );
}
