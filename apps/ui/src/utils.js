export function formatNumber(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "--";
  }
  return Number(value).toLocaleString(undefined, {
    maximumFractionDigits: digits,
    minimumFractionDigits: Number.isInteger(Number(value)) ? 0 : Math.min(1, digits),
  });
}

export function formatValue(value, unit = "") {
  if (value === null || value === undefined) {
    return "--";
  }
  if (unit === "boolean") {
    return Number(value) >= 1 ? "Active" : "Inactive";
  }
  const suffix = unit && unit !== "unknown" ? ` ${unit}` : "";
  return `${formatNumber(value, 2)}${suffix}`;
}

export function formatPercent(value, digits = 0) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "--";
  }
  return `${Number(value).toLocaleString(undefined, {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  })}%`;
}

export function formatTimestamp(value) {
  if (!value) {
    return "--";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString();
}

export function formatClock(value = Date.now()) {
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "--";
  }
  return date.toLocaleTimeString();
}

export function formatRelativeTime(value) {
  if (!value) {
    return "--";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  const deltaSeconds = Math.max(0, Math.floor((Date.now() - date.getTime()) / 1000));
  if (deltaSeconds < 60) {
    return `${deltaSeconds}s ago`;
  }
  if (deltaSeconds < 3600) {
    return `${Math.floor(deltaSeconds / 60)}m ago`;
  }
  return `${Math.floor(deltaSeconds / 3600)}h ago`;
}

export function formatFreshness(seconds) {
  if (seconds === null || seconds === undefined || Number.isNaN(Number(seconds))) {
    return "--";
  }
  const value = Math.max(0, Math.round(Number(seconds)));
  if (value < 60) {
    return `${value}s`;
  }
  if (value < 3600) {
    return `${Math.floor(value / 60)}m`;
  }
  return `${Math.floor(value / 3600)}h`;
}

export function humanizeRootCause(value) {
  return (value || "nominal")
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

export function toneFromStatus(value) {
  if (value === "critical") {
    return "critical";
  }
  if (value === "warning") {
    return "warning";
  }
  if (value === "medium") {
    return "medium";
  }
  return "nominal";
}

export function categoryColor(category) {
  const palette = {
    production: "#3b82f6",
    maintenance: "#f59e0b",
    sensor: "#22c55e",
    alarm: "#dc2626",
    status: "#8b5cf6",
    energy: "#0ea5e9",
    signal: "#64748b",
  };
  return palette[category] || "#64748b";
}

export function toneFromConnectionStatus(value) {
  if (value === "error" || value === "disconnected") {
    return "critical";
  }
  if (value === "connected" || value === "monitored") {
    return "nominal";
  }
  if (value === "discovering") {
    return "medium";
  }
  return "warning";
}

export function toneFromConnectorHealth(value) {
  if (value === "outage") {
    return "critical";
  }
  if (value === "degraded") {
    return "warning";
  }
  return "nominal";
}

export function connectionTypeLabel(value) {
  if (value === "opcua") {
    return "OPC UA";
  }
  if (value === "mqtt") {
    return "MQTT";
  }
  return (value || "unknown").toUpperCase();
}

export function humanizeCollectionMode(value) {
  if (value === "subscription") {
    return "Subscription";
  }
  if (value === "scrape") {
    return "Scrape";
  }
  return value ? value.charAt(0).toUpperCase() + value.slice(1) : "--";
}

export function titleFromAsset(asset) {
  if (!asset) {
    return "Unknown asset";
  }
  return asset.display_name || asset.asset_id || "Unnamed asset";
}
