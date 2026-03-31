const runtimeApiBase = import.meta.env.VITE_API_BASE_URL || window.location.origin;

async function request(path, query = {}) {
  const url = new URL(path, runtimeApiBase);
  Object.entries(query).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      url.searchParams.set(key, value);
    }
  });

  const response = await fetch(url.toString(), {
    headers: {
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    throw new Error(`Request failed with status ${response.status}`);
  }

  return response.json();
}

async function send(path, method, payload) {
  const response = await fetch(new URL(path, runtimeApiBase), {
    method,
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    throw new Error(`Request failed with status ${response.status}`);
  }
  return response.json();
}

export function getDashboardState(assetId) {
  return request("/api/state", assetId ? { asset_id: assetId } : {});
}

export function getAssets() {
  return request("/api/assets");
}

export async function createAsset(payload) {
  return send("/api/assets", "POST", payload);
}

export async function updateAsset(assetId, payload) {
  return send(`/api/assets/${assetId}`, "PATCH", payload);
}

export async function testAssetConnection(payloadOrAssetId) {
  const path =
    typeof payloadOrAssetId === "string"
      ? `/api/assets/${payloadOrAssetId}/test-connection`
      : "/api/assets/test-connection";
  const body = typeof payloadOrAssetId === "string" ? {} : payloadOrAssetId;
  return send(path, "POST", body);
}

export async function discoverAsset(payloadOrAssetId) {
  const path =
    typeof payloadOrAssetId === "string"
      ? `/api/assets/${payloadOrAssetId}/discover`
      : "/api/assets/discover";
  const body = typeof payloadOrAssetId === "string" ? {} : payloadOrAssetId;
  return send(path, "POST", body);
}

export function getAssetPassport(assetId) {
  return request(`/api/assets/${assetId}/passport`);
}

export function getAssetSignals(assetId) {
  return request(`/api/assets/${assetId}/signals`);
}

export function getAssetMappings(assetId) {
  return request(`/api/assets/${assetId}/mappings`);
}

export async function updateAssetMapping(assetId, mappingId, payload) {
  return send(`/api/assets/${assetId}/mappings/${mappingId}`, "PATCH", payload);
}

export async function rebuildAssetPassport(assetId) {
  return send(`/api/assets/${assetId}/passport/rebuild`, "POST", {});
}

export async function createAssetNote(assetId, payload) {
  return send(`/api/assets/${assetId}/notes`, "POST", payload);
}

export function getAssetComponents(assetId) {
  return request(`/api/assets/${assetId}/components`);
}

export async function createAssetComponent(assetId, payload) {
  return send(`/api/assets/${assetId}/components`, "POST", payload);
}

export function getAssetSoftware(assetId) {
  return request(`/api/assets/${assetId}/software`);
}

export async function createAssetSoftware(assetId, payload) {
  return send(`/api/assets/${assetId}/software`, "POST", payload);
}

export function getAssetMaintenance(assetId) {
  return request(`/api/assets/${assetId}/maintenance`);
}

export async function createAssetMaintenance(assetId, payload) {
  return send(`/api/assets/${assetId}/maintenance`, "POST", payload);
}

export function getAssetDocuments(assetId) {
  return request(`/api/assets/${assetId}/documents`);
}

export async function createAssetDocument(assetId, payload) {
  return send(`/api/assets/${assetId}/documents`, "POST", payload);
}

export function getAssetCompliance(assetId) {
  return request(`/api/assets/${assetId}/compliance`);
}

export async function createAssetCompliance(assetId, payload) {
  return send(`/api/assets/${assetId}/compliance`, "POST", payload);
}

export function getAssetAccess(assetId) {
  return request(`/api/assets/${assetId}/access`);
}

export async function saveAssetAccess(assetId, payload) {
  return send(`/api/assets/${assetId}/access`, "POST", payload);
}

export function getAssetIntegrity(assetId) {
  return request(`/api/assets/${assetId}/integrity`);
}

export async function saveAssetIntegrity(assetId, payload) {
  return send(`/api/assets/${assetId}/integrity`, "POST", payload);
}

export function getAssetSustainability(assetId) {
  return request(`/api/assets/${assetId}/sustainability`);
}

export async function saveAssetSustainability(assetId, payload) {
  return send(`/api/assets/${assetId}/sustainability`, "POST", payload);
}

export function getAssetOwnership(assetId) {
  return request(`/api/assets/${assetId}/ownership`);
}

export async function createAssetOwnership(assetId, payload) {
  return send(`/api/assets/${assetId}/ownership`, "POST", payload);
}

export function getSignalSeries({ assetId, signal, path = "", minutes = 30, stepSeconds = 15 }) {
  return request("/api/series", {
    asset_id: assetId,
    signal,
    path,
    minutes,
    step_seconds: stepSeconds,
  });
}

export function getTimeline(assetId, limit = 80) {
  return request("/api/timeline", {
    asset_id: assetId,
    limit,
  });
}
