function toneForValue(value) {
  if (value > 80) {
    return "good";
  }
  if (value >= 50) {
    return "medium";
  }
  return "low";
}

export default function ConfidenceBar({ value }) {
  const safeValue = Math.max(0, Math.min(100, Number(value) || 0));
  const tone = toneForValue(safeValue);

  return (
    <div className="confidence">
      <div className="confidence__bar">
        <div className={`confidence__fill confidence__fill--${tone}`} style={{ width: `${safeValue}%` }} />
      </div>
      <span className="confidence__value">{safeValue.toFixed(0)}%</span>
    </div>
  );
}
