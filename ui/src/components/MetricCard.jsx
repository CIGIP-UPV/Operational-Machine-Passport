import SectionLabel from "./SectionLabel.jsx";

export default function MetricCard({ eyebrow, value, detail, tone = "blue" }) {
  return (
    <article className={`metric-card metric-card--${tone}`}>
      <SectionLabel>{eyebrow}</SectionLabel>
      <strong className="metric-card__value">{value}</strong>
      <span className="metric-card__detail">{detail}</span>
    </article>
  );
}
