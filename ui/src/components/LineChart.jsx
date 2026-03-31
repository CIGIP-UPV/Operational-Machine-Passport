function buildPoints(points, width, height, padding) {
  if (!points.length) {
    return "";
  }

  const values = points.map((point) => Number(point.value));
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;

  return points
    .map((point, index) => {
      const x = padding + (index / Math.max(points.length - 1, 1)) * (width - padding * 2);
      const y = height - padding - ((Number(point.value) - min) / range) * (height - padding * 2);
      return `${x},${y}`;
    })
    .join(" ");
}

export default function LineChart({ points, height = 240, accent = "var(--blue-500)" }) {
  const width = 760;
  const padding = 28;

  if (!points || !points.length) {
    return <div className="chart-empty">No historical samples available yet.</div>;
  }

  const values = points.map((point) => Number(point.value));
  const min = Math.min(...values);
  const max = Math.max(...values);
  const polyline = buildPoints(points, width, height, padding);
  const last = points[points.length - 1];
  const areaPoints = `${padding},${height - padding} ${polyline} ${width - padding},${height - padding}`;

  return (
    <div className="chart-shell">
      <svg className="line-chart" viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none" aria-hidden="true">
        <defs>
          <linearGradient id="series-fill" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stopColor={accent} stopOpacity="0.28" />
            <stop offset="100%" stopColor={accent} stopOpacity="0.02" />
          </linearGradient>
        </defs>
        <line x1={padding} y1={padding} x2={width - padding} y2={padding} className="line-chart__grid" />
        <line
          x1={padding}
          y1={height / 2}
          x2={width - padding}
          y2={height / 2}
          className="line-chart__grid line-chart__grid--mid"
        />
        <line
          x1={padding}
          y1={height - padding}
          x2={width - padding}
          y2={height - padding}
          className="line-chart__grid"
        />
        <polygon points={areaPoints} fill="url(#series-fill)" />
        <polyline points={polyline} fill="none" stroke={accent} strokeWidth="3" strokeLinejoin="round" strokeLinecap="round" />
      </svg>
      <div className="chart-legend">
        <span>Min {min.toFixed(2)}</span>
        <span>Max {max.toFixed(2)}</span>
        <span>Latest {Number(last.value).toFixed(2)}</span>
      </div>
    </div>
  );
}
