function pathFromSeries(data, width, height, padding) {
  if (!data.length) {
    return "";
  }

  const values = data.map((item) => Number(item));
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;

  return values
    .map((value, index) => {
      const x = padding + (index / Math.max(values.length - 1, 1)) * (width - padding * 2);
      const y = height - padding - ((value - min) / range) * (height - padding * 2);
      return `${index === 0 ? "M" : "L"} ${x.toFixed(2)} ${y.toFixed(2)}`;
    })
    .join(" ");
}

function areaFromSeries(data, width, height, padding) {
  if (!data.length) {
    return "";
  }
  const line = pathFromSeries(data, width, height, padding);
  const firstX = padding;
  const lastX = width - padding;
  const baseline = height - padding;
  return `${line} L ${lastX} ${baseline} L ${firstX} ${baseline} Z`;
}

export default function Sparkline({
  data = [],
  width = 60,
  height = 24,
  color = "#3b82f6",
  filled = false,
  className = "",
}) {
  const values = data.length ? data : [0, 0, 0, 0];
  const linePath = pathFromSeries(values, width, height, 2);
  const areaPath = areaFromSeries(values, width, height, 2);
  const lastValue = Number(values[values.length - 1] || 0);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const lastX = width - 2;
  const lastY = height - 2 - ((lastValue - min) / range) * (height - 4);

  return (
    <svg
      className={`sparkline ${className}`.trim()}
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      preserveAspectRatio="none"
      aria-hidden="true"
    >
      {filled ? <path d={areaPath} fill={color} opacity="0.08" /> : null}
      <path d={linePath} fill="none" stroke={color} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
      <circle cx={lastX} cy={lastY} r="2.5" fill={color} />
    </svg>
  );
}
