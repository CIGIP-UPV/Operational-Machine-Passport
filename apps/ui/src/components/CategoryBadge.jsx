const CATEGORY_CLASS = {
  sensor: "sensor",
  production: "production",
  maintenance: "maintenance",
  alarm: "alarm",
  status: "status",
  energy: "energy",
  signal: "signal",
};

export default function CategoryBadge({ category }) {
  return <span className={`category-badge category-badge--${CATEGORY_CLASS[category] || "signal"}`}>{category}</span>;
}
