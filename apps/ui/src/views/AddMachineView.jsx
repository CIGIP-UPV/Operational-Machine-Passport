import { useState } from "react";
import { createAsset, discoverAsset, testAssetConnection } from "../api.js";
import Card from "../components/Card.jsx";
import SectionLabel from "../components/SectionLabel.jsx";
import StatusBadge from "../components/StatusBadge.jsx";

const INITIAL_FORM = {
  asset_id: "",
  display_name: "",
  asset_type: "cnc",
  manufacturer: "",
  model: "",
  serial_number: "",
  location: "",
  opcua_endpoint: "",
  profile_id: "generic",
  description: "",
};

function Field({ label, children }) {
  return (
    <label className="form-field">
      <span className="form-field__label">{label}</span>
      {children}
    </label>
  );
}

export default function AddMachineView({ profiles, onAssetCreated }) {
  const profileOptions = profiles?.length ? profiles : [{ id: "generic" }];
  const [form, setForm] = useState(INITIAL_FORM);
  const [connectionResult, setConnectionResult] = useState(null);
  const [discoveryResult, setDiscoveryResult] = useState(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState("");

  function updateField(key, value) {
    setForm((current) => ({ ...current, [key]: value }));
  }

  async function handleTestConnection() {
    setBusy("connection");
    setError("");
    try {
      const result = await testAssetConnection(form);
      setConnectionResult(result);
    } catch (requestError) {
      setError("The OPC UA endpoint could not be reached. Review the endpoint, credentials or the machine status.");
      setConnectionResult(null);
    } finally {
      setBusy("");
    }
  }

  async function handleDiscovery() {
    setBusy("discovery");
    setError("");
    try {
      const result = await discoverAsset(form);
      setDiscoveryResult(result);
    } catch (requestError) {
      setError("Discovery could not be completed for this OPC UA endpoint.");
      setDiscoveryResult(null);
    } finally {
      setBusy("");
    }
  }

  async function handleSave() {
    setBusy("save");
    setError("");
    try {
      const created = await createAsset(form);
      if (form.opcua_endpoint) {
        try {
          await discoverAsset(created.asset.asset_id);
        } catch {
          // Keep the asset saved even if discovery fails; the user can retry later from the passport view.
        }
      }
      onAssetCreated(created.asset);
      setForm(INITIAL_FORM);
      setConnectionResult(null);
      setDiscoveryResult(null);
    } catch (requestError) {
      setError("The asset could not be saved. Check the required fields and try again.");
    } finally {
      setBusy("");
    }
  }

  return (
    <div className="dashboard-stack">
      <Card className="overview-hero">
        <div className="overview-hero__content">
          <SectionLabel>Add Machine</SectionLabel>
          <h2>Register a new OPC UA asset</h2>
          <p>
            Add the technical identity of a machine, test the OPC UA endpoint and run a discovery preview before saving the
            asset in the registry.
          </p>
        </div>
      </Card>

      <div className="two-column-grid">
        <Card>
          <div className="section-header">
            <div>
              <SectionLabel>Connection Setup</SectionLabel>
              <h3>Machine metadata and endpoint</h3>
            </div>
          </div>

          <div className="form-grid">
            <Field label="Asset ID">
              <input value={form.asset_id} onChange={(event) => updateField("asset_id", event.target.value)} placeholder="cnc-01" />
            </Field>
            <Field label="Display name">
              <input value={form.display_name} onChange={(event) => updateField("display_name", event.target.value)} placeholder="CNC Machine 01" />
            </Field>
            <Field label="Asset type">
              <input value={form.asset_type} onChange={(event) => updateField("asset_type", event.target.value)} placeholder="cnc" />
            </Field>
            <Field label="Semantic profile">
              <select value={form.profile_id} onChange={(event) => updateField("profile_id", event.target.value)}>
                {profileOptions.map((profile) => (
                  <option key={profile.id} value={profile.id}>
                    {profile.id}
                  </option>
                ))}
              </select>
            </Field>
            <Field label="Manufacturer">
              <input value={form.manufacturer} onChange={(event) => updateField("manufacturer", event.target.value)} placeholder="Manufacturer" />
            </Field>
            <Field label="Model">
              <input value={form.model} onChange={(event) => updateField("model", event.target.value)} placeholder="Model" />
            </Field>
            <Field label="Serial number">
              <input value={form.serial_number} onChange={(event) => updateField("serial_number", event.target.value)} placeholder="Serial number" />
            </Field>
            <Field label="Location">
              <input value={form.location} onChange={(event) => updateField("location", event.target.value)} placeholder="Plant / line / cell" />
            </Field>
            <Field label="OPC UA endpoint">
              <input
                value={form.opcua_endpoint}
                onChange={(event) => updateField("opcua_endpoint", event.target.value)}
                placeholder="opc.tcp://host:4840/freeopcua/assets/"
              />
            </Field>
            <Field label="Description">
              <textarea
                value={form.description}
                onChange={(event) => updateField("description", event.target.value)}
                placeholder="Optional operational context for this asset."
              />
            </Field>
          </div>

          <div className="form-actions">
            <button type="button" className="secondary-button" onClick={handleTestConnection} disabled={busy !== ""}>
              {busy === "connection" ? "Testing…" : "Test Connection"}
            </button>
            <button type="button" className="secondary-button" onClick={handleDiscovery} disabled={busy !== ""}>
              {busy === "discovery" ? "Discovering…" : "Run Discovery"}
            </button>
            <button type="button" className="primary-button" onClick={handleSave} disabled={busy !== ""}>
              {busy === "save" ? "Saving…" : "Save Machine"}
            </button>
          </div>

          {error ? <div className="inline-feedback inline-feedback--error">{error}</div> : null}
        </Card>

        <div className="dashboard-stack">
          <Card>
            <div className="section-header">
              <div>
                <SectionLabel>Connection Check</SectionLabel>
                <h3>Endpoint validation</h3>
              </div>
              {connectionResult ? <StatusBadge tone="nominal" label="reachable" /> : null}
            </div>
            {connectionResult ? (
              <div className="metrics-grid">
                <div className="metric-cell">
                  <span>Endpoint</span>
                  <strong>{connectionResult.endpoint}</strong>
                </div>
                <div className="metric-cell">
                  <span>Namespaces</span>
                  <strong>{connectionResult.namespace_count}</strong>
                </div>
                <div className="metric-cell">
                  <span>Checked at</span>
                  <strong>{connectionResult.checked_at}</strong>
                </div>
              </div>
            ) : (
              <div className="chart-empty">Run a connection check to validate the OPC UA endpoint before saving the asset.</div>
            )}
          </Card>

          <Card>
            <div className="section-header">
              <div>
                <SectionLabel>Discovery Preview</SectionLabel>
                <h3>Semantic mapping summary</h3>
              </div>
            </div>
            {discoveryResult ? (
              <div className="dashboard-stack">
                <div className="metrics-grid">
                  <div className="metric-cell">
                    <span>Signals</span>
                    <strong>{discoveryResult.signal_count}</strong>
                  </div>
                  <div className="metric-cell">
                    <span>Nodes</span>
                    <strong>{discoveryResult.node_count}</strong>
                  </div>
                  <div className="metric-cell">
                    <span>Namespaces</span>
                    <strong>{discoveryResult.namespace_count}</strong>
                  </div>
                </div>
                <div className="distribution-grid">
                  {Object.entries(discoveryResult.categories || {}).map(([category, count]) => (
                    <div className="distribution-card" key={category}>
                      <div className="distribution-card__row">
                        <span className="distribution-card__name">{category}</span>
                        <strong>{count}</strong>
                      </div>
                    </div>
                  ))}
                </div>
                <div className="preview-list">
                  {(discoveryResult.signals || []).slice(0, 8).map((signal) => (
                    <div className="preview-list__row" key={signal.signal_key}>
                      <strong>{signal.display_name}</strong>
                      <span>
                        {signal.category} · {signal.subsystem}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            ) : (
              <div className="chart-empty">Discovery will preview mapped signals, categories and namespaces before the asset is saved.</div>
            )}
          </Card>
        </div>
      </div>
    </div>
  );
}
