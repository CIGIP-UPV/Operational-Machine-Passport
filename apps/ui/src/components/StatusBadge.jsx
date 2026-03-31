export default function StatusBadge({ tone = "nominal", label, size = "sm" }) {
  return (
    <span className={`status-badge status-badge--${tone} status-badge--${size}`} aria-label={`Status ${label}`}>
      <span className="status-badge__dot" />
      <span>{String(label).toUpperCase()}</span>
    </span>
  );
}
