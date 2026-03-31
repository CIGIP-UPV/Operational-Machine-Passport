import { useEffect, useMemo, useState } from "react";
import {
  createAsset,
  createAssetComponent,
  createAssetCompliance,
  createAssetDocument,
  createAssetMaintenance,
  createAssetOwnership,
  createAssetSoftware,
  discoverAsset,
  getAssetMappings,
  getAssetSignals,
  rebuildAssetPassport,
  saveAssetAccess,
  saveAssetIntegrity,
  saveAssetSustainability,
  testAssetConnection,
  updateAssetMapping,
  updateAsset,
} from "../api.js";
import Card from "../components/Card.jsx";
import CategoryBadge from "../components/CategoryBadge.jsx";
import SectionLabel from "../components/SectionLabel.jsx";
import StatusBadge from "../components/StatusBadge.jsx";
import Tag from "../components/Tag.jsx";
import {
  formatNumber,
  formatPercent,
  formatRelativeTime,
  formatTimestamp,
  toneFromConnectionStatus,
  titleFromAsset,
} from "../utils.js";

const INITIAL_FORM = {
  asset_id: "",
  display_name: "",
  asset_type: "cnc",
  connection_type: "opcua",
  manufacturer: "",
  model: "",
  serial_number: "",
  location: "",
  opcua_endpoint: "",
  mqtt_broker_url: "",
  mqtt_topic_root: "",
  mqtt_qos: "0",
  mqtt_client_id: "",
  mqtt_username: "",
  mqtt_password: "",
  profile_id: "generic",
  description: "",
  manufacture_date: "",
  country_of_origin: "",
  rated_power_kw: "",
  interfaces: "",
};

const EMPTY_COMPONENT = {
  component_id: "",
  name: "",
  part_number: "",
  supplier: "",
  is_replaceable: "true",
  criticality: "medium",
  notes: "",
};

const EMPTY_SOFTWARE = {
  software_id: "",
  name: "",
  software_type: "firmware",
  version: "",
  hash: "",
  update_channel: "",
  support_start: "",
  support_end: "",
  sbom_ref: "",
};

const EMPTY_MAINTENANCE = {
  event_at: "",
  action: "",
  actor: "",
  result: "ok",
  next_due: "",
  parts_changed: "",
  notes: "",
};

const EMPTY_DOCUMENT = {
  document_type: "manual",
  title: "",
  ref: "",
  issuer: "",
  visibility: "internal",
};

const EMPTY_COMPLIANCE = {
  certificate_type: "ce",
  title: "",
  ref: "",
  issuer: "",
  valid_from: "",
  valid_until: "",
  status: "active",
  notes: "",
};

const EMPTY_ACCESS = {
  access_tier: "internal",
  audience: "operators",
  policy_ref: "",
  justification: "",
  contact: "",
};

const EMPTY_INTEGRITY = {
  revision: "1",
  record_hash: "",
  signature_ref: "",
  signed_by: "",
  last_verified_at: "",
};

const EMPTY_SUSTAINABILITY = {
  pcf_kg_co2e: "",
  energy_class: "",
  recyclable_ratio: "",
  takeback_available: false,
  end_of_life_instructions: "",
};

const EMPTY_OWNERSHIP = {
  event_type: "commissioned",
  owner_name: "",
  effective_at: "",
  location: "",
  notes: "",
};

const EMPTY_MAPPING = {
  id: null,
  source_ref: "",
  signal_key: "",
  display_name: "",
  category: "signal",
  subsystem: "",
  unit: "",
  criticality: "medium",
  is_active: true,
};

function SearchIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" aria-hidden="true">
      <circle cx="11" cy="11" r="7" fill="none" stroke="currentColor" strokeWidth="2" />
      <path d="M20 20l-3.5-3.5" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
    </svg>
  );
}

function MachineGlyph({ assetType }) {
  const tone = assetType === "plc" ? "machine-glyph--teal" : "machine-glyph--blue";
  return <span className={`machine-glyph ${tone}`}>{(assetType || "op").slice(0, 2).toUpperCase()}</span>;
}

function Field({ label, children, wide = false }) {
  return (
    <label className={`form-field${wide ? " form-field--wide" : ""}`}>
      <span className="form-field__label">{label}</span>
      {children}
    </label>
  );
}

function parseInterfaces(value) {
  return value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function keyMetric(value, fallback = "--") {
  if (value === null || value === undefined || value === "" || Number.isNaN(Number(value))) {
    return fallback;
  }
  return value;
}

function baseSignalKey(value) {
  return String(value || "").split("::", 1)[0];
}

function AddMachineModal({ open, profiles, onClose, onCreated }) {
  const [form, setForm] = useState(INITIAL_FORM);
  const [connectionResult, setConnectionResult] = useState(null);
  const [discoveryResult, setDiscoveryResult] = useState(null);
  const [busy, setBusy] = useState("");
  const [error, setError] = useState("");

  useEffect(() => {
    if (!open) {
      setForm(INITIAL_FORM);
      setConnectionResult(null);
      setDiscoveryResult(null);
      setBusy("");
      setError("");
    }
  }, [open]);

  useEffect(() => {
    function handleKeyDown(event) {
      if (event.key === "Escape") {
        onClose();
      }
    }

    if (!open) {
      return undefined;
    }

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [open, onClose]);

  if (!open) {
    return null;
  }

  function updateField(key, value) {
    setForm((current) => ({ ...current, [key]: value }));
  }

  function serializedForm() {
    const connectionConfig =
      form.connection_type === "mqtt"
        ? {
            broker_url: form.mqtt_broker_url,
            topic_root: form.mqtt_topic_root,
            qos: form.mqtt_qos ? Number(form.mqtt_qos) : 0,
            client_id: form.mqtt_client_id,
            username: form.mqtt_username,
            password: form.mqtt_password,
          }
        : {
            endpoint: form.opcua_endpoint,
            security_mode: "none",
          };
    return {
      ...form,
      interfaces: parseInterfaces(form.interfaces),
      rated_power_kw: form.rated_power_kw ? Number(form.rated_power_kw) : null,
      connection_config: connectionConfig,
    };
  }

  async function handleTestConnection() {
    setBusy("connection");
    setError("");
    try {
      const result = await testAssetConnection(serializedForm());
      setConnectionResult(result);
    } catch {
      setConnectionResult(null);
      setError("The configured connection could not be reached. Review the endpoint, broker or credentials and try again.");
    } finally {
      setBusy("");
    }
  }

  async function handleDiscovery() {
    setBusy("discovery");
    setError("");
    try {
      const result = await discoverAsset(serializedForm());
      setDiscoveryResult(result);
    } catch {
      setDiscoveryResult(null);
      setError("Discovery could not be completed for this connection.");
    } finally {
      setBusy("");
    }
  }

  async function handleSave() {
    setBusy("save");
    setError("");
    try {
      const created = await createAsset(serializedForm());
      if (form.opcua_endpoint || form.mqtt_broker_url) {
        try {
          await discoverAsset(created.asset.asset_id);
        } catch {
          // Asset registration should not fail if discovery is temporarily unavailable.
        }
      }
      onCreated(created.asset);
      onClose();
    } catch {
      setError("The machine could not be saved. Check the required fields and try again.");
    } finally {
      setBusy("");
    }
  }

  return (
    <div className="modal-backdrop" onClick={onClose} role="presentation">
      <div className="modal-shell modal-shell--wide" onClick={(event) => event.stopPropagation()} role="dialog" aria-modal="true" aria-label="Add machine">
        <div className="modal-shell__header">
          <div>
            <SectionLabel>Add Machine</SectionLabel>
            <h3>Register a new industrial asset</h3>
            <p className="card-copy">The onboarding flow captures registry metadata, connection details and the first serious nameplate fields of the machine passport.</p>
          </div>
          <button type="button" className="modal-close" onClick={onClose}>
            Close
          </button>
        </div>

        <div className="modal-machine-layout">
          <div className="modal-machine-form">
            <div className="form-grid">
              <Field label="Asset ID">
                <input value={form.asset_id} onChange={(event) => updateField("asset_id", event.target.value)} placeholder="cnc-02" />
              </Field>
              <Field label="Display name">
                <input value={form.display_name} onChange={(event) => updateField("display_name", event.target.value)} placeholder="CNC Machine 02" />
              </Field>
              <Field label="Asset type">
                <input value={form.asset_type} onChange={(event) => updateField("asset_type", event.target.value)} placeholder="cnc" />
              </Field>
              <Field label="Connection type">
                <select value={form.connection_type} onChange={(event) => updateField("connection_type", event.target.value)}>
                  <option value="opcua">opcua</option>
                  <option value="mqtt">mqtt</option>
                </select>
              </Field>
              <Field label="Semantic profile">
                <select value={form.profile_id} onChange={(event) => updateField("profile_id", event.target.value)}>
                  {(profiles?.length ? profiles : [{ id: "generic" }]).map((profile) => (
                    <option key={profile.id} value={profile.id}>
                      {profile.id}
                    </option>
                  ))}
                </select>
              </Field>
              <Field label="Manufacturer">
                <input value={form.manufacturer} onChange={(event) => updateField("manufacturer", event.target.value)} placeholder="Nakamura" />
              </Field>
              <Field label="Model">
                <input value={form.model} onChange={(event) => updateField("model", event.target.value)} placeholder="WT-150" />
              </Field>
              <Field label="Serial number">
                <input value={form.serial_number} onChange={(event) => updateField("serial_number", event.target.value)} placeholder="SN-0002" />
              </Field>
              <Field label="Location">
                <input value={form.location} onChange={(event) => updateField("location", event.target.value)} placeholder="Plant / line / cell" />
              </Field>
              <Field label="Manufacture date">
                <input type="date" value={form.manufacture_date} onChange={(event) => updateField("manufacture_date", event.target.value)} />
              </Field>
              <Field label="Country of origin">
                <input value={form.country_of_origin} onChange={(event) => updateField("country_of_origin", event.target.value)} placeholder="ES" />
              </Field>
              <Field label="Rated power (kW)">
                <input value={form.rated_power_kw} onChange={(event) => updateField("rated_power_kw", event.target.value)} placeholder="12.5" />
              </Field>
              <Field label="Interfaces">
                <input value={form.interfaces} onChange={(event) => updateField("interfaces", event.target.value)} placeholder="opcua, mqtt, ethernet-ip" />
              </Field>
              {form.connection_type === "opcua" ? (
                <Field label="OPC UA endpoint" wide>
                  <input
                    value={form.opcua_endpoint}
                    onChange={(event) => updateField("opcua_endpoint", event.target.value)}
                    placeholder="opc.tcp://opcua-simulator-cnc-02:4840/freeopcua/assets/"
                  />
                </Field>
              ) : (
                <>
                  <Field label="MQTT broker URL" wide>
                    <input value={form.mqtt_broker_url} onChange={(event) => updateField("mqtt_broker_url", event.target.value)} placeholder="mqtt://broker:1883" />
                  </Field>
                  <Field label="Topic root">
                    <input value={form.mqtt_topic_root} onChange={(event) => updateField("mqtt_topic_root", event.target.value)} placeholder="factory/cnc-02" />
                  </Field>
                  <Field label="QoS">
                    <select value={form.mqtt_qos} onChange={(event) => updateField("mqtt_qos", event.target.value)}>
                      <option value="0">0</option>
                      <option value="1">1</option>
                      <option value="2">2</option>
                    </select>
                  </Field>
                  <Field label="Client ID">
                    <input value={form.mqtt_client_id} onChange={(event) => updateField("mqtt_client_id", event.target.value)} placeholder="opc-observe-cnc-02" />
                  </Field>
                  <Field label="MQTT username">
                    <input value={form.mqtt_username} onChange={(event) => updateField("mqtt_username", event.target.value)} />
                  </Field>
                  <Field label="MQTT password">
                    <input type="password" value={form.mqtt_password} onChange={(event) => updateField("mqtt_password", event.target.value)} />
                  </Field>
                </>
              )}
              <Field label="Description" wide>
                <textarea value={form.description} onChange={(event) => updateField("description", event.target.value)} placeholder="Operational context, product family, cell or maintenance notes." />
              </Field>
            </div>

            <div className="form-actions">
              <button type="button" className="secondary-button" onClick={handleTestConnection} disabled={busy !== ""}>
                {busy === "connection" ? "Testing…" : "Test Connection"}
              </button>
              <button type="button" className="secondary-button" onClick={handleDiscovery} disabled={busy !== ""}>
                {busy === "discovery" ? "Running…" : "Run Discovery"}
              </button>
              <button type="button" className="primary-button primary-button--coral" onClick={handleSave} disabled={busy !== ""}>
                {busy === "save" ? "Saving…" : "Save Machine"}
              </button>
            </div>

            {error ? <div className="inline-feedback inline-feedback--error">{error}</div> : null}
          </div>

          <div className="modal-machine-preview">
            <Card className="mini-card">
              <div className="mini-card__header">
                <div>
                  <SectionLabel>Connection Check</SectionLabel>
                  <h4>Endpoint validation</h4>
                </div>
                {connectionResult?.reachable ? <StatusBadge tone="nominal" label="reachable" /> : null}
              </div>
              {connectionResult ? (
                <div className="mini-grid">
                  <div className="mini-grid__cell">
                    <span>{connectionResult.connection_type === "mqtt" ? "Broker" : "Endpoint"}</span>
                    <strong>{connectionResult.endpoint}</strong>
                  </div>
                  <div className="mini-grid__cell">
                    <span>{connectionResult.connection_type === "mqtt" ? "Topic root" : "Namespaces"}</span>
                    <strong>{connectionResult.connection_type === "mqtt" ? connectionResult.topic_root || "#" : connectionResult.namespace_count}</strong>
                  </div>
                  <div className="mini-grid__cell">
                    <span>Checked at</span>
                    <strong>{connectionResult.checked_at}</strong>
                  </div>
                </div>
              ) : (
                <div className="empty-block">Run a connection test before saving the machine.</div>
              )}
            </Card>

            <Card className="mini-card">
              <div className="mini-card__header">
                <div>
                  <SectionLabel>Discovery Preview</SectionLabel>
                  <h4>Semantic mapping summary</h4>
                </div>
              </div>
              {discoveryResult ? (
                <div className="discovery-preview">
                  <div className="mini-grid">
                    <div className="mini-grid__cell">
                      <span>Signals</span>
                      <strong>{discoveryResult.signal_count}</strong>
                    </div>
                    <div className="mini-grid__cell">
                      <span>Nodes</span>
                      <strong>{discoveryResult.node_count}</strong>
                    </div>
                    <div className="mini-grid__cell">
                      <span>{discoveryResult.connection_type === "mqtt" ? "Topic root" : "Namespaces"}</span>
                      <strong>{discoveryResult.connection_type === "mqtt" ? discoveryResult.topic_root || "#" : discoveryResult.namespace_count}</strong>
                    </div>
                  </div>

                  <div className="machine-inline-list">
                    {(discoveryResult.signals || []).slice(0, 6).map((signal) => (
                      <div className="machine-inline-list__row" key={signal.signal_key}>
                        <strong>{signal.display_name}</strong>
                        <span>
                          {signal.category} · {signal.subsystem}
                        </span>
                      </div>
                    ))}
                  </div>
                </div>
              ) : (
                <div className="empty-block">Discovery will preview mapped signals and namespaces here.</div>
              )}
            </Card>
          </div>
        </div>
      </div>
    </div>
  );
}

function InlineForm({ title, label, children, onSubmit, busyLabel, idleLabel }) {
  return (
    <div className="passport-inline-form">
      <div className="passport-inline-form__header">
        <strong>{title}</strong>
      </div>
      <div className="passport-inline-form__fields">{children}</div>
      <div className="form-actions form-actions--tight">
        <button type="button" className="primary-button primary-button--sm" onClick={onSubmit}>
          {busyLabel || idleLabel}
        </button>
      </div>
    </div>
  );
}

function ListEmpty({ text }) {
  return <div className="empty-block empty-block--tight">{text}</div>;
}

function PassportSectionTabs({ value, onChange }) {
  const sections = [
    ["overview", "Overview"],
    ["semantic", "Semantic"],
    ["technical", "Technical"],
    ["maintenance", "Maintenance"],
    ["compliance", "Compliance"],
    ["sustainability", "Sustainability"],
  ];

  return (
    <div className="passport-section-tabs" role="tablist" aria-label="Machine passport sections">
      {sections.map(([id, label]) => (
        <button
          key={id}
          type="button"
          className={`passport-section-tab${value === id ? " passport-section-tab--active" : ""}`}
          onClick={() => onChange(id)}
        >
          {label}
        </button>
      ))}
    </div>
  );
}

export default function MachinesTab({
  assets,
  profiles,
  selectedAssetId,
  onSelectAsset,
  onOpenAsset,
  passportPayload,
  onPassportRefresh,
  onAssetCreated,
  registryView,
}) {
  const [search, setSearch] = useState("");
  const [modalOpen, setModalOpen] = useState(false);
  const [storedSignals, setStoredSignals] = useState([]);
  const [storedMappings, setStoredMappings] = useState([]);
  const [signalsLoading, setSignalsLoading] = useState(false);
  const [mappingsLoading, setMappingsLoading] = useState(false);
  const [rebuilding, setRebuilding] = useState(false);
  const [passportSection, setPassportSection] = useState("overview");
  const [nameplateForm, setNameplateForm] = useState({ manufacture_date: "", country_of_origin: "", rated_power_kw: "", interfaces: "" });
  const [componentForm, setComponentForm] = useState(EMPTY_COMPONENT);
  const [softwareForm, setSoftwareForm] = useState(EMPTY_SOFTWARE);
  const [maintenanceForm, setMaintenanceForm] = useState(EMPTY_MAINTENANCE);
  const [documentForm, setDocumentForm] = useState(EMPTY_DOCUMENT);
  const [complianceForm, setComplianceForm] = useState(EMPTY_COMPLIANCE);
  const [accessForm, setAccessForm] = useState(EMPTY_ACCESS);
  const [integrityForm, setIntegrityForm] = useState(EMPTY_INTEGRITY);
  const [sustainabilityForm, setSustainabilityForm] = useState(EMPTY_SUSTAINABILITY);
  const [ownershipForm, setOwnershipForm] = useState(EMPTY_OWNERSHIP);
  const [mappingForm, setMappingForm] = useState(EMPTY_MAPPING);
  const [selectedMappingId, setSelectedMappingId] = useState(null);
  const [savingBlock, setSavingBlock] = useState("");

  const selectedAsset = assets.find((asset) => asset.asset_id === selectedAssetId) || assets[0] || null;
  const passport = passportPayload?.passport || {};
  const diagnostics = passport.diagnostics || {};
  const semantic = passport.semantic || {};
  const baseline = passport.baseline || {};
  const connectivity = passport.connectivity || {};
  const identity = passport.identity || {};
  const nameplate = passport.nameplate || {};
  const maintenance = passport.maintenance || {};
  const software = passport.software || {};
  const components = passport.components || {};
  const documents = passport.documents || {};
  const compliance = passport.compliance || {};
  const access = passport.access || {};
  const integrity = passport.integrity || {};
  const sustainability = passport.sustainability || {};
  const custody = passport.custody || {};

  const filteredAssets = useMemo(() => {
    if (!search.trim()) {
      return assets;
    }
    const needle = search.trim().toLowerCase();
    return assets.filter((asset) => {
      const haystack = [asset.asset_id, asset.display_name, asset.asset_type, asset.connection_status, asset.description].filter(Boolean).join(" ").toLowerCase();
      return haystack.includes(needle);
    });
  }, [assets, search]);

  useEffect(() => {
    let cancelled = false;

    async function loadSemanticInventory() {
      if (!selectedAsset?.asset_id) {
        setStoredSignals([]);
        setStoredMappings([]);
        return;
      }
      setSignalsLoading(true);
      setMappingsLoading(true);
      try {
        const [signalsPayload, mappingsPayload] = await Promise.all([
          getAssetSignals(selectedAsset.asset_id),
          getAssetMappings(selectedAsset.asset_id),
        ]);
        if (!cancelled) {
          setStoredSignals(signalsPayload.signals || []);
          setStoredMappings(mappingsPayload.mappings || []);
        }
      } catch {
        if (!cancelled) {
          setStoredSignals([]);
          setStoredMappings([]);
        }
      } finally {
        if (!cancelled) {
          setSignalsLoading(false);
          setMappingsLoading(false);
        }
      }
    }

    loadSemanticInventory();
    return () => {
      cancelled = true;
    };
  }, [selectedAsset?.asset_id]);

  useEffect(() => {
    if (!storedMappings.length) {
      setSelectedMappingId(null);
      setMappingForm(EMPTY_MAPPING);
      return;
    }
    const selected = storedMappings.find((item) => item.id === selectedMappingId) || storedMappings[0];
    setSelectedMappingId(selected.id);
    setMappingForm({
      id: selected.id,
      source_ref: selected.source_ref || "",
      signal_key: baseSignalKey(selected.signal_key),
      display_name: selected.display_name || "",
      category: selected.category || "signal",
      subsystem: selected.subsystem || "",
      unit: selected.unit || "",
      criticality: selected.criticality || "medium",
      is_active: Boolean(selected.is_active),
    });
  }, [selectedMappingId, storedMappings]);

  useEffect(() => {
    setNameplateForm({
      manufacture_date: nameplate.manufacture_date || "",
      country_of_origin: nameplate.country_of_origin === "Unknown" ? "" : nameplate.country_of_origin || "",
      rated_power_kw: nameplate.rated_power_kw ?? "",
      interfaces: (nameplate.interfaces || []).join(", "),
    });
  }, [nameplate.manufacture_date, nameplate.country_of_origin, nameplate.rated_power_kw, JSON.stringify(nameplate.interfaces || [])]);

  useEffect(() => {
    setAccessForm({
      access_tier: access.tier || "internal",
      audience: access.audience || "operators",
      policy_ref: access.policy_ref || "",
      justification: access.justification || "",
      contact: access.contact || "",
    });
  }, [access.tier, access.audience, access.policy_ref, access.justification, access.contact]);

  useEffect(() => {
    setIntegrityForm({
      revision: integrity.revision || "1",
      record_hash: integrity.record_hash || "",
      signature_ref: integrity.signature_ref || "",
      signed_by: integrity.signed_by || "",
      last_verified_at: integrity.last_verified_at ? String(integrity.last_verified_at).slice(0, 16) : "",
    });
  }, [integrity.revision, integrity.record_hash, integrity.signature_ref, integrity.signed_by, integrity.last_verified_at]);

  useEffect(() => {
    setSustainabilityForm({
      pcf_kg_co2e: sustainability.pcf_kg_co2e ?? "",
      energy_class: sustainability.energy_class || "",
      recyclable_ratio: sustainability.recyclable_ratio ?? "",
      takeback_available: Boolean(sustainability.takeback_available),
      end_of_life_instructions: sustainability.end_of_life_instructions || "",
    });
  }, [sustainability.pcf_kg_co2e, sustainability.energy_class, sustainability.recyclable_ratio, sustainability.takeback_available, sustainability.end_of_life_instructions]);

  async function refreshSemanticInventory(assetId = selectedAsset?.asset_id) {
    if (!assetId) {
      return;
    }
    setSignalsLoading(true);
    setMappingsLoading(true);
    try {
      const [signalsPayload, mappingsPayload] = await Promise.all([
        getAssetSignals(assetId),
        getAssetMappings(assetId),
      ]);
      setStoredSignals(signalsPayload.signals || []);
      setStoredMappings(mappingsPayload.mappings || []);
    } finally {
      setSignalsLoading(false);
      setMappingsLoading(false);
    }
  }

  async function refreshCurrentPassport() {
    if (selectedAsset?.asset_id) {
      await onPassportRefresh(selectedAsset.asset_id);
    }
  }

  async function handleRebuildPassport() {
    if (!selectedAsset?.asset_id) {
      return;
    }
    setRebuilding(true);
    try {
      await rebuildAssetPassport(selectedAsset.asset_id);
      await refreshCurrentPassport();
    } finally {
      setRebuilding(false);
    }
  }

  async function handleSaveNameplate() {
    if (!selectedAsset?.asset_id) {
      return;
    }
    setSavingBlock("nameplate");
    try {
      await updateAsset(selectedAsset.asset_id, {
        manufacture_date: nameplateForm.manufacture_date || null,
        country_of_origin: nameplateForm.country_of_origin || null,
        rated_power_kw: nameplateForm.rated_power_kw === "" ? null : Number(nameplateForm.rated_power_kw),
        interfaces: parseInterfaces(nameplateForm.interfaces),
      });
      await refreshCurrentPassport();
    } finally {
      setSavingBlock("");
    }
  }

  async function handleCreateComponent() {
    if (!selectedAsset?.asset_id || !componentForm.component_id || !componentForm.name) {
      return;
    }
    setSavingBlock("component");
    try {
      await createAssetComponent(selectedAsset.asset_id, {
        ...componentForm,
        is_replaceable: componentForm.is_replaceable === "true",
      });
      setComponentForm(EMPTY_COMPONENT);
      await refreshCurrentPassport();
    } finally {
      setSavingBlock("");
    }
  }

  async function handleCreateSoftware() {
    if (!selectedAsset?.asset_id || !softwareForm.software_id || !softwareForm.name || !softwareForm.version) {
      return;
    }
    setSavingBlock("software");
    try {
      await createAssetSoftware(selectedAsset.asset_id, softwareForm);
      setSoftwareForm(EMPTY_SOFTWARE);
      await refreshCurrentPassport();
    } finally {
      setSavingBlock("");
    }
  }

  async function handleCreateMaintenance() {
    if (!selectedAsset?.asset_id || !maintenanceForm.action) {
      return;
    }
    setSavingBlock("maintenance");
    try {
      await createAssetMaintenance(selectedAsset.asset_id, maintenanceForm);
      setMaintenanceForm(EMPTY_MAINTENANCE);
      await refreshCurrentPassport();
    } finally {
      setSavingBlock("");
    }
  }

  async function handleCreateDocument() {
    if (!selectedAsset?.asset_id || !documentForm.document_type || !documentForm.title || !documentForm.ref) {
      return;
    }
    setSavingBlock("document");
    try {
      await createAssetDocument(selectedAsset.asset_id, documentForm);
      setDocumentForm(EMPTY_DOCUMENT);
      await refreshCurrentPassport();
    } finally {
      setSavingBlock("");
    }
  }

  async function handleCreateCompliance() {
    if (!selectedAsset?.asset_id || !complianceForm.certificate_type || !complianceForm.title || !complianceForm.ref) {
      return;
    }
    setSavingBlock("compliance");
    try {
      await createAssetCompliance(selectedAsset.asset_id, complianceForm);
      setComplianceForm(EMPTY_COMPLIANCE);
      await refreshCurrentPassport();
    } finally {
      setSavingBlock("");
    }
  }

  async function handleSaveAccess() {
    if (!selectedAsset?.asset_id) {
      return;
    }
    setSavingBlock("access");
    try {
      await saveAssetAccess(selectedAsset.asset_id, accessForm);
      await refreshCurrentPassport();
    } finally {
      setSavingBlock("");
    }
  }

  async function handleSaveIntegrity() {
    if (!selectedAsset?.asset_id) {
      return;
    }
    setSavingBlock("integrity");
    try {
      await saveAssetIntegrity(selectedAsset.asset_id, {
        ...integrityForm,
        last_verified_at: integrityForm.last_verified_at || null,
      });
      await refreshCurrentPassport();
    } finally {
      setSavingBlock("");
    }
  }

  async function handleSaveSustainability() {
    if (!selectedAsset?.asset_id) {
      return;
    }
    setSavingBlock("sustainability");
    try {
      await saveAssetSustainability(selectedAsset.asset_id, {
        ...sustainabilityForm,
        pcf_kg_co2e: sustainabilityForm.pcf_kg_co2e === "" ? null : Number(sustainabilityForm.pcf_kg_co2e),
        recyclable_ratio: sustainabilityForm.recyclable_ratio === "" ? null : Number(sustainabilityForm.recyclable_ratio),
        takeback_available: Boolean(sustainabilityForm.takeback_available),
      });
      await refreshCurrentPassport();
    } finally {
      setSavingBlock("");
    }
  }

  async function handleCreateOwnership() {
    if (!selectedAsset?.asset_id || !ownershipForm.event_type || !ownershipForm.owner_name) {
      return;
    }
    setSavingBlock("ownership");
    try {
      await createAssetOwnership(selectedAsset.asset_id, ownershipForm);
      setOwnershipForm(EMPTY_OWNERSHIP);
      await refreshCurrentPassport();
    } finally {
      setSavingBlock("");
    }
  }

  async function handleSaveMapping() {
    if (!selectedAsset?.asset_id || !selectedMappingId || !mappingForm.signal_key.trim()) {
      return;
    }
    setSavingBlock("mapping");
    try {
      await updateAssetMapping(selectedAsset.asset_id, selectedMappingId, {
        signal_key: mappingForm.signal_key.trim(),
        display_name: mappingForm.display_name.trim(),
        category: mappingForm.category,
        subsystem: mappingForm.subsystem.trim(),
        unit: mappingForm.unit.trim(),
        criticality: mappingForm.criticality,
        is_active: Boolean(mappingForm.is_active),
        mapping_source: "manual",
      });
      await Promise.all([refreshCurrentPassport(), refreshSemanticInventory()]);
    } finally {
      setSavingBlock("");
    }
  }

  const semanticRows = storedMappings.length
    ? storedMappings
    : (semantic.signals_preview || []).map((signal, index) => ({
        signal_key: `${signal.display_name || "signal"}-${index}`,
        source_ref: signal.source_ref || "",
        display_name: signal.display_name,
        category: signal.category,
        subsystem: signal.subsystem,
        unit: signal.unit,
        criticality: signal.criticality,
        mapping_source: signal.mapping_source || "auto",
        is_active: signal.is_active ?? true,
      }));

  const selectedMapping = storedMappings.find((item) => item.id === selectedMappingId) || null;
  const semanticHealthTone =
    Number(semantic.mapping_confidence || 0) >= 80
      ? "nominal"
      : Number(semantic.mapping_confidence || 0) >= 50
        ? "warning"
        : "critical";

  return (
    <>
      {registryView ? (
        <div className="machines-registry-page">
          <Card className="machine-registry-card machine-registry-card--page">
            <div className="machine-registry-card__header">
              <div>
                <SectionLabel>Machine Registry</SectionLabel>
                <h3>Registered assets</h3>
                <p className="card-copy">Choose a machine to open its passport, or register a new industrial asset from here.</p>
              </div>
              <button type="button" className="primary-button primary-button--sm" onClick={() => setModalOpen(true)}>
                + Add Machine
              </button>
            </div>

            <label className="search-field">
              <span className="search-field__icon">
                <SearchIcon />
              </span>
              <input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search machines..." />
            </label>

            <div className="machine-list">
              {filteredAssets.map((asset) => {
                const summary = asset.passport_summary || {};
                const selected = asset.asset_id === selectedAsset?.asset_id;
                return (
                  <button
                    type="button"
                    key={asset.asset_id}
                    className={`machine-list__item${selected ? " machine-list__item--selected" : ""}`}
                    onClick={() => onOpenAsset(asset.asset_id)}
                  >
                    <MachineGlyph assetType={asset.asset_type} />
                    <div className="machine-list__text">
                      <strong>{titleFromAsset(asset)}</strong>
                    <span>
                      {(asset.asset_type || "generic").toUpperCase()} · {((asset.primary_connection_type || asset.primary_connection?.connection_type || "unknown")).toUpperCase()} · {(asset.connection_status || "unknown").toLowerCase()} · {asset.live?.signals_tracked ?? summary.signal_count ?? 0} signals
                    </span>
                  </div>
                    <StatusBadge tone={toneFromConnectionStatus(asset.connection_status)} label={asset.connection_status || "unknown"} />
                  </button>
                );
              })}

              {!filteredAssets.length ? <div className="empty-block">No machines match the current search.</div> : null}
            </div>
          </Card>
        </div>
      ) : (
        <div className="passport-stack">
          {!selectedAsset ? (
            <Card>
              <div className="empty-block">Select a registered machine to open its digital passport.</div>
            </Card>
          ) : (
            <>
              <Card className="passport-identity-card">
                <div className="passport-identity-card__left">
                  <h2>{identity.display_name || titleFromAsset(selectedAsset)}</h2>
                  <p className="card-copy">{identity.description || "Digital passport built from registry metadata, semantic discovery and live observability."}</p>
                  <div className="passport-identity-card__tags">
                    <Tag tone="blue" label={(identity.asset_type || selectedAsset.asset_type || "generic").toUpperCase()} />
                    <Tag tone={connectivity.connection_status === "connected" || connectivity.connection_status === "monitored" ? "green" : "orange"} label={(connectivity.connection_status || "unknown").toUpperCase()} />
                  </div>
                </div>

                <dl className="passport-identity-card__meta">
                  <div>
                    <dt>Manufacturer</dt>
                    <dd>{identity.manufacturer || "Unknown"}</dd>
                  </div>
                  <div>
                    <dt>Model</dt>
                    <dd>{identity.model || "Unknown"}</dd>
                  </div>
                  <div>
                    <dt>Serial</dt>
                    <dd>{identity.serial_number || "Unknown"}</dd>
                  </div>
                  <div>
                    <dt>Location</dt>
                    <dd>{identity.location || "Unassigned"}</dd>
                  </div>
                </dl>
              </Card>

              <div className="metrics-row metrics-row--passport">
                <article className="metric-card metric-card--blue">
                  <SectionLabel>Health Score</SectionLabel>
                  <strong className="metric-card__value">{formatNumber(keyMetric(diagnostics.health_score), 1)}</strong>
                  <span className="metric-card__detail">Composite score</span>
                </article>
                <article className="metric-card metric-card--teal">
                  <SectionLabel>Coverage</SectionLabel>
                  <strong className="metric-card__value">{formatPercent(keyMetric(semantic.coverage_ratio), 1)}</strong>
                  <span className="metric-card__detail">{semantic.active_mapping_count || 0} active mappings</span>
                </article>
                <article className="metric-card metric-card--coral">
                  <SectionLabel>Signals</SectionLabel>
                  <strong className="metric-card__value">{semantic.signal_count || 0}</strong>
                  <span className="metric-card__detail">{semantic.manual_mapping_count || 0} manual overrides</span>
                </article>
                <article className="metric-card metric-card--green">
                  <SectionLabel>Baseline</SectionLabel>
                  <strong className="metric-card__value">{formatPercent(keyMetric(baseline.confidence), 0)}</strong>
                  <span className="metric-card__detail">{formatPercent(keyMetric(semantic.mapping_confidence), 0)} mapping confidence</span>
                </article>
              </div>

              <PassportSectionTabs value={passportSection} onChange={setPassportSection} />

              {passportSection === "overview" ? (
                <>
                  <Card>
                  <div className="card-header-row">
                      <div>
                        <SectionLabel>Semantic Coverage</SectionLabel>
                        <h3>Signals and subsystems</h3>
                      </div>
                      <StatusBadge tone={semanticHealthTone} label={`${formatPercent(semantic.mapping_confidence || 0, 0)} confidence`} />
                    </div>

                    {signalsLoading || mappingsLoading ? (
                      <div className="empty-block">Loading discovered signals…</div>
                    ) : (
                      <div className="table-shell">
                        <table className="semantic-table">
                          <thead>
                            <tr>
                              <th>Signal</th>
                              <th>Semantic ID</th>
                              <th>Category</th>
                              <th>Type</th>
                            </tr>
                          </thead>
                          <tbody>
                            {semanticRows.map((signal) => (
                              <tr key={signal.signal_key}>
                                <td className="table-title-cell">{signal.display_name || signal.signal_key}</td>
                                <td>
                                  <code>{signal.signal_key || signal.display_name}</code>
                                </td>
                                <td>
                                  <CategoryBadge category={signal.category || "signal"} />
                                </td>
                                <td>{signal.unit === "boolean" ? "boolean" : "numeric"}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </Card>

                  <Card>
                    <div className="card-header-row">
                      <div>
                        <SectionLabel>Operational Fingerprint</SectionLabel>
                        <h3>Learned baseline</h3>
                      </div>
                      <StatusBadge tone={baseline.status === "ready" ? "nominal" : "warning"} label={`${baseline.status || "learning"} · ${formatPercent(baseline.confidence || 0, 0)}`} />
                    </div>

                    <div className="baseline-list">
                      {(baseline.signals || []).length ? (
                        baseline.signals.map((signal) => (
                          <div className="baseline-list__item" key={signal.signal_key}>
                            <strong>{signal.display_name}</strong>
                            <span className="baseline-values">min {formatNumber(signal.min_value, 2)} · max {formatNumber(signal.max_value, 2)} · avg {formatNumber(signal.avg_value, 2)}</span>
                          </div>
                        ))
                      ) : (
                        <div className="empty-block">The baseline will appear here as soon as the machine accumulates enough signal history.</div>
                      )}
                    </div>
                  </Card>
                </>
              ) : null}

              {passportSection === "semantic" ? (
                <>
                  <Card>
                    <div className="card-header-row">
                      <div>
                        <SectionLabel>Semantic Quality</SectionLabel>
                        <h3>Mapping coverage and confidence</h3>
                      </div>
                      <StatusBadge tone={semanticHealthTone} label={`${formatPercent(semantic.mapping_confidence || 0, 0)} confidence`} />
                    </div>

                    <div className="pipeline-grid">
                      <div className="pipeline-grid__cell">
                        <dt>Mapped signals</dt>
                        <dd>{semantic.active_mapping_count || 0}</dd>
                      </div>
                      <div className="pipeline-grid__cell">
                        <dt>Manual overrides</dt>
                        <dd>{semantic.manual_mapping_count || 0}</dd>
                      </div>
                      <div className="pipeline-grid__cell">
                        <dt>Inactive mappings</dt>
                        <dd>{semantic.inactive_mapping_count || 0}</dd>
                      </div>
                      <div className="pipeline-grid__cell">
                        <dt>Unmapped signals</dt>
                        <dd>{semantic.unmapped_signal_count || 0}</dd>
                      </div>
                      <div className="pipeline-grid__cell">
                        <dt>Coverage ratio</dt>
                        <dd>{formatPercent(semantic.coverage_ratio || 0, 1)}</dd>
                      </div>
                      <div className="pipeline-grid__cell">
                        <dt>Signal inventory</dt>
                        <dd>{semantic.signal_count || 0}</dd>
                      </div>
                    </div>
                  </Card>

                  <div className="passport-secondary-grid">
                    <Card>
                      <div className="card-header-row">
                        <div>
                          <SectionLabel>Semantic Mappings</SectionLabel>
                          <h3>Editable mapping inventory</h3>
                        </div>
                      </div>

                      {mappingsLoading ? (
                        <div className="empty-block">Loading signal mappings…</div>
                      ) : semanticRows.length ? (
                        <div className="table-shell">
                          <table className="semantic-table">
                            <thead>
                              <tr>
                                <th>Source</th>
                                <th>Semantic ID</th>
                                <th>Category</th>
                                <th>Status</th>
                                <th>Mode</th>
                                <th>Action</th>
                              </tr>
                            </thead>
                            <tbody>
                              {semanticRows.map((mapping) => {
                                const isSelected = mapping.id === selectedMappingId;
                                return (
                                  <tr key={mapping.id || mapping.signal_key}>
                                    <td className="table-title-cell">
                                      <div className="mapping-source-cell">
                                        <strong>{mapping.display_name || baseSignalKey(mapping.signal_key)}</strong>
                                        <small>{mapping.source_ref || mapping.path || "--"}</small>
                                      </div>
                                    </td>
                                    <td>
                                      <code>{baseSignalKey(mapping.signal_key)}</code>
                                    </td>
                                    <td>
                                      <CategoryBadge category={mapping.category || "signal"} />
                                    </td>
                                    <td>
                                      <StatusBadge tone={mapping.is_active ? "nominal" : "warning"} label={mapping.is_active ? "active" : "inactive"} />
                                    </td>
                                    <td>{(mapping.mapping_source || "auto").toUpperCase()}</td>
                                    <td>
                                      {mapping.id ? (
                                        <button
                                          type="button"
                                          className={`secondary-button mapping-select-button${isSelected ? " mapping-select-button--active" : ""}`}
                                          onClick={() => {
                                            setSelectedMappingId(mapping.id);
                                            setMappingForm({
                                              id: mapping.id,
                                              source_ref: mapping.source_ref || "",
                                              signal_key: baseSignalKey(mapping.signal_key),
                                              display_name: mapping.display_name || "",
                                              category: mapping.category || "signal",
                                              subsystem: mapping.subsystem || "",
                                              unit: mapping.unit || "",
                                              criticality: mapping.criticality || "medium",
                                              is_active: Boolean(mapping.is_active),
                                            });
                                          }}
                                        >
                                          {isSelected ? "Editing" : "Edit"}
                                        </button>
                                      ) : (
                                        <span className="mapping-select-placeholder">Preview</span>
                                      )}
                                    </td>
                                  </tr>
                                );
                              })}
                            </tbody>
                          </table>
                        </div>
                      ) : (
                        <div className="empty-block">No semantic mappings are available yet. Run discovery or wait for live signal sync.</div>
                      )}
                    </Card>

                    <Card>
                      <div className="card-header-row">
                        <div>
                          <SectionLabel>Manual Override</SectionLabel>
                          <h3>Adjust the selected mapping</h3>
                        </div>
                        {selectedMapping ? <StatusBadge tone={selectedMapping.is_active ? "nominal" : "warning"} label={selectedMapping.mapping_source || "auto"} /> : null}
                      </div>

                      {selectedMapping ? (
                        <div className="passport-inline-form passport-inline-form--semantic">
                          <div className="passport-inline-form__header">
                            <strong>{selectedMapping.display_name || baseSignalKey(selectedMapping.signal_key)}</strong>
                            <small>{selectedMapping.source_ref}</small>
                          </div>
                          <div className="form-grid">
                            <Field label="Semantic ID">
                              <input value={mappingForm.signal_key} onChange={(event) => setMappingForm((current) => ({ ...current, signal_key: event.target.value }))} placeholder="spindle_temperature" />
                            </Field>
                            <Field label="Display name">
                              <input value={mappingForm.display_name} onChange={(event) => setMappingForm((current) => ({ ...current, display_name: event.target.value }))} placeholder="Spindle Temperature" />
                            </Field>
                            <Field label="Category">
                              <select value={mappingForm.category} onChange={(event) => setMappingForm((current) => ({ ...current, category: event.target.value }))}>
                                <option value="signal">signal</option>
                                <option value="sensor">sensor</option>
                                <option value="status">status</option>
                                <option value="production">production</option>
                                <option value="maintenance">maintenance</option>
                                <option value="alarm">alarm</option>
                                <option value="energy">energy</option>
                              </select>
                            </Field>
                            <Field label="Criticality">
                              <select value={mappingForm.criticality} onChange={(event) => setMappingForm((current) => ({ ...current, criticality: event.target.value }))}>
                                <option value="low">low</option>
                                <option value="medium">medium</option>
                                <option value="high">high</option>
                                <option value="critical">critical</option>
                              </select>
                            </Field>
                            <Field label="Subsystem">
                              <input value={mappingForm.subsystem} onChange={(event) => setMappingForm((current) => ({ ...current, subsystem: event.target.value }))} placeholder="spindle" />
                            </Field>
                            <Field label="Unit">
                              <input value={mappingForm.unit} onChange={(event) => setMappingForm((current) => ({ ...current, unit: event.target.value }))} placeholder="celsius" />
                            </Field>
                            <Field label="Mapping active">
                              <select value={mappingForm.is_active ? "true" : "false"} onChange={(event) => setMappingForm((current) => ({ ...current, is_active: event.target.value === "true" }))}>
                                <option value="true">yes</option>
                                <option value="false">no</option>
                              </select>
                            </Field>
                            <Field label="Source reference" wide>
                              <input value={mappingForm.source_ref} readOnly />
                            </Field>
                          </div>
                          <div className="form-actions form-actions--tight">
                            <button type="button" className="primary-button primary-button--sm" onClick={handleSaveMapping} disabled={savingBlock === "mapping"}>
                              {savingBlock === "mapping" ? "Saving…" : "Save mapping"}
                            </button>
                          </div>
                        </div>
                      ) : (
                        <ListEmpty text="Select a mapping from the table to edit its semantic metadata." />
                      )}
                    </Card>
                  </div>
                </>
              ) : null}

              {passportSection === "technical" ? (
                <>
                  <Card>
                    <div className="card-header-row">
                      <div>
                        <SectionLabel>Technical Identity</SectionLabel>
                        <h3>Connection and profile</h3>
                      </div>
                      <button type="button" className="secondary-button" onClick={handleRebuildPassport} disabled={rebuilding}>
                        {rebuilding ? "Rebuilding…" : "Rebuild passport"}
                      </button>
                    </div>

                    <div className="passport-connection-grid">
                      <div className="passport-connection-grid__cell"><dt>Primary connection</dt><dd>{(connectivity.primary_connection_type || "unknown").toUpperCase()}</dd></div>
                      <div className="passport-connection-grid__cell"><dt>Endpoint</dt><dd>{connectivity.endpoint || "Not configured"}</dd></div>
                      <div className="passport-connection-grid__cell"><dt>Profile</dt><dd>{connectivity.profile_id || "generic"}</dd></div>
                      <div className="passport-connection-grid__cell"><dt>Security</dt><dd>{connectivity.security_mode || "none"}</dd></div>
                      <div className="passport-connection-grid__cell"><dt>Passport ID</dt><dd>{passport.passport_id || "--"}</dd></div>
                      <div className="passport-connection-grid__cell"><dt>Schema version</dt><dd>{passport.schema_version || "--"}</dd></div>
                      <div className="passport-connection-grid__cell"><dt>Issued</dt><dd>{formatRelativeTime(passport.issued_at)}</dd></div>
                      <div className="passport-connection-grid__cell"><dt>Updated</dt><dd>{formatRelativeTime(passport.updated_at)}</dd></div>
                      <div className="passport-connection-grid__cell"><dt>Identification link</dt><dd>{passport.identification_link || "--"}</dd></div>
                      <div className="passport-connection-grid__cell"><dt>Last discovery</dt><dd>{formatRelativeTime(connectivity.last_discovered_at)}</dd></div>
                      <div className="passport-connection-grid__cell"><dt>Status</dt><dd><StatusBadge tone={toneFromConnectionStatus(connectivity.connection_status)} label={connectivity.status || connectivity.connection_status || "unknown"} /></dd></div>
                    </div>

                    <div className="passport-record-list passport-record-list--compact">
                      {(connectivity.connections || []).length ? (
                        connectivity.connections.map((connection, index) => (
                          <div className="passport-record-list__item" key={`${connection.connection_type}-${connection.endpoint_or_host}-${index}`}>
                            <strong>{`${connection.connection_type.toUpperCase()}${connection.is_primary ? " · primary" : ""}`}</strong>
                            <span>{connection.endpoint_or_host}</span>
                            <small>{connection.status || "unknown"}</small>
                          </div>
                        ))
                      ) : (
                        <ListEmpty text="No connection records are attached to this asset yet." />
                      )}
                    </div>
                  </Card>

                  <Card>
                    <div className="card-header-row">
                      <div>
                        <SectionLabel>Nameplate</SectionLabel>
                        <h3>Technical machine passport</h3>
                      </div>
                    </div>

                    <div className="nameplate-grid">
                      <div className="nameplate-grid__cell"><dt>Product name</dt><dd>{nameplate.product_name || identity.display_name || "--"}</dd></div>
                      <div className="nameplate-grid__cell"><dt>Manufacture date</dt><dd>{nameplate.manufacture_date || "--"}</dd></div>
                      <div className="nameplate-grid__cell"><dt>Country of origin</dt><dd>{nameplate.country_of_origin || "--"}</dd></div>
                      <div className="nameplate-grid__cell"><dt>Rated power</dt><dd>{nameplate.rated_power_kw ? `${formatNumber(nameplate.rated_power_kw, 1)} kW` : "--"}</dd></div>
                      <div className="nameplate-grid__cell nameplate-grid__cell--wide"><dt>Interfaces</dt><dd>{(nameplate.interfaces || []).length ? nameplate.interfaces.join(", ") : "--"}</dd></div>
                    </div>

                    <div className="passport-inline-form passport-inline-form--spaced">
                      <div className="passport-inline-form__header"><strong>Edit nameplate</strong></div>
                      <div className="form-grid">
                        <Field label="Manufacture date"><input type="date" value={nameplateForm.manufacture_date} onChange={(event) => setNameplateForm((current) => ({ ...current, manufacture_date: event.target.value }))} /></Field>
                        <Field label="Country of origin"><input value={nameplateForm.country_of_origin} onChange={(event) => setNameplateForm((current) => ({ ...current, country_of_origin: event.target.value }))} placeholder="ES" /></Field>
                        <Field label="Rated power (kW)"><input value={nameplateForm.rated_power_kw} onChange={(event) => setNameplateForm((current) => ({ ...current, rated_power_kw: event.target.value }))} placeholder="12.5" /></Field>
                        <Field label="Interfaces"><input value={nameplateForm.interfaces} onChange={(event) => setNameplateForm((current) => ({ ...current, interfaces: event.target.value }))} placeholder="opcua, mqtt" /></Field>
                      </div>
                      <div className="form-actions form-actions--tight">
                        <button type="button" className="primary-button primary-button--sm" onClick={handleSaveNameplate} disabled={savingBlock === "nameplate"}>
                          {savingBlock === "nameplate" ? "Saving…" : "Save nameplate"}
                        </button>
                      </div>
                    </div>
                  </Card>

                  <div className="passport-secondary-grid">
                    <Card>
                      <div className="card-header-row"><div><SectionLabel>Components</SectionLabel><h3>Critical replaceable parts</h3></div></div>
                      <div className="passport-record-list">
                        {(components.items || []).length ? (
                          components.items.map((item) => (
                            <div className="passport-record-list__item" key={`${item.id}-${item.component_id}`}>
                              <strong>{item.name}</strong>
                              <span>{item.component_id} · {item.part_number || "No part number"} · {item.supplier || "No supplier"}</span>
                              <small>{item.criticality} · {item.is_replaceable ? "replaceable" : "fixed component"}</small>
                            </div>
                          ))
                        ) : <ListEmpty text="No critical components have been registered yet." />}
                      </div>
                      <div className="passport-inline-form">
                        <div className="passport-inline-form__header"><strong>Add component</strong></div>
                        <div className="form-grid">
                          <Field label="Component ID"><input value={componentForm.component_id} onChange={(event) => setComponentForm((current) => ({ ...current, component_id: event.target.value }))} /></Field>
                          <Field label="Name"><input value={componentForm.name} onChange={(event) => setComponentForm((current) => ({ ...current, name: event.target.value }))} /></Field>
                          <Field label="Part number"><input value={componentForm.part_number} onChange={(event) => setComponentForm((current) => ({ ...current, part_number: event.target.value }))} /></Field>
                          <Field label="Supplier"><input value={componentForm.supplier} onChange={(event) => setComponentForm((current) => ({ ...current, supplier: event.target.value }))} /></Field>
                          <Field label="Criticality"><select value={componentForm.criticality} onChange={(event) => setComponentForm((current) => ({ ...current, criticality: event.target.value }))}><option value="low">low</option><option value="medium">medium</option><option value="high">high</option><option value="critical">critical</option></select></Field>
                          <Field label="Replaceable"><select value={componentForm.is_replaceable} onChange={(event) => setComponentForm((current) => ({ ...current, is_replaceable: event.target.value }))}><option value="true">yes</option><option value="false">no</option></select></Field>
                          <Field label="Notes" wide><textarea value={componentForm.notes} onChange={(event) => setComponentForm((current) => ({ ...current, notes: event.target.value }))} placeholder="Why this part matters operationally." /></Field>
                        </div>
                        <div className="form-actions form-actions--tight">
                          <button type="button" className="primary-button primary-button--sm" onClick={handleCreateComponent} disabled={savingBlock === "component"}>
                            {savingBlock === "component" ? "Saving…" : "Add component"}
                          </button>
                        </div>
                      </div>
                    </Card>

                    <Card>
                      <div className="card-header-row"><div><SectionLabel>Software & Firmware</SectionLabel><h3>Runtime inventory</h3></div></div>
                      <div className="passport-record-list">
                        {(software.items || []).length ? (
                          software.items.map((item) => (
                            <div className="passport-record-list__item" key={`${item.id}-${item.software_id}`}>
                              <strong>{item.name}</strong>
                              <span>{item.software_type} · v{item.version} · support until {item.support_end || "--"}</span>
                              <small>{item.update_channel || "no channel"} · {item.sbom_ref || "no SBOM ref"}</small>
                            </div>
                          ))
                        ) : <ListEmpty text="No software or firmware inventory is registered yet." />}
                      </div>
                      <div className="passport-inline-form">
                        <div className="passport-inline-form__header"><strong>Add software item</strong></div>
                        <div className="form-grid">
                          <Field label="Software ID"><input value={softwareForm.software_id} onChange={(event) => setSoftwareForm((current) => ({ ...current, software_id: event.target.value }))} /></Field>
                          <Field label="Name"><input value={softwareForm.name} onChange={(event) => setSoftwareForm((current) => ({ ...current, name: event.target.value }))} /></Field>
                          <Field label="Type"><select value={softwareForm.software_type} onChange={(event) => setSoftwareForm((current) => ({ ...current, software_type: event.target.value }))}><option value="firmware">firmware</option><option value="software">software</option></select></Field>
                          <Field label="Version"><input value={softwareForm.version} onChange={(event) => setSoftwareForm((current) => ({ ...current, version: event.target.value }))} /></Field>
                          <Field label="Update channel"><input value={softwareForm.update_channel} onChange={(event) => setSoftwareForm((current) => ({ ...current, update_channel: event.target.value }))} /></Field>
                          <Field label="Integrity hash"><input value={softwareForm.hash} onChange={(event) => setSoftwareForm((current) => ({ ...current, hash: event.target.value }))} placeholder="sha256:..." /></Field>
                          <Field label="Support start"><input type="date" value={softwareForm.support_start} onChange={(event) => setSoftwareForm((current) => ({ ...current, support_start: event.target.value }))} /></Field>
                          <Field label="Support end"><input type="date" value={softwareForm.support_end} onChange={(event) => setSoftwareForm((current) => ({ ...current, support_end: event.target.value }))} /></Field>
                          <Field label="SBOM reference" wide><input value={softwareForm.sbom_ref} onChange={(event) => setSoftwareForm((current) => ({ ...current, sbom_ref: event.target.value }))} placeholder="doc://sbom/controller-fw.spdx.json" /></Field>
                        </div>
                        <div className="form-actions form-actions--tight">
                          <button type="button" className="primary-button primary-button--sm" onClick={handleCreateSoftware} disabled={savingBlock === "software"}>
                            {savingBlock === "software" ? "Saving…" : "Add software"}
                          </button>
                        </div>
                      </div>
                    </Card>
                  </div>

                  <Card>
                    <div className="card-header-row"><div><SectionLabel>Documents</SectionLabel><h3>Linked handover references</h3></div></div>
                    <div className="passport-record-list">
                      {(documents.items || []).length ? (
                        documents.items.map((item) => (
                          <div className="passport-record-list__item" key={`${item.id}-${item.ref}`}>
                            <strong>{item.title}</strong>
                            <span>{item.document_type} · {item.visibility} · {item.issuer || "No issuer"}</span>
                            <small>{item.ref}</small>
                          </div>
                        ))
                      ) : <ListEmpty text="No linked documents are attached to this passport yet." />}
                    </div>
                    <div className="passport-inline-form">
                      <div className="passport-inline-form__header"><strong>Add document</strong></div>
                      <div className="form-grid">
                        <Field label="Type"><select value={documentForm.document_type} onChange={(event) => setDocumentForm((current) => ({ ...current, document_type: event.target.value }))}><option value="manual">manual</option><option value="handover">handover</option><option value="datasheet">datasheet</option><option value="procedure">procedure</option></select></Field>
                        <Field label="Visibility"><select value={documentForm.visibility} onChange={(event) => setDocumentForm((current) => ({ ...current, visibility: event.target.value }))}><option value="internal">internal</option><option value="public">public</option><option value="service">service</option></select></Field>
                        <Field label="Title" wide><input value={documentForm.title} onChange={(event) => setDocumentForm((current) => ({ ...current, title: event.target.value }))} /></Field>
                        <Field label="Reference" wide><input value={documentForm.ref} onChange={(event) => setDocumentForm((current) => ({ ...current, ref: event.target.value }))} placeholder="doc://manuals/cnc-01.pdf" /></Field>
                        <Field label="Issuer"><input value={documentForm.issuer} onChange={(event) => setDocumentForm((current) => ({ ...current, issuer: event.target.value }))} placeholder="OEM Example S.A." /></Field>
                      </div>
                      <div className="form-actions form-actions--tight">
                        <button type="button" className="primary-button primary-button--sm" onClick={handleCreateDocument} disabled={savingBlock === "document"}>
                          {savingBlock === "document" ? "Saving…" : "Add document"}
                        </button>
                      </div>
                    </div>
                  </Card>
                </>
              ) : null}

              {passportSection === "maintenance" ? (
                <Card>
                  <div className="card-header-row">
                    <div>
                      <SectionLabel>Maintenance</SectionLabel>
                      <h3>Structured service history</h3>
                    </div>
                  </div>
                  <div className="passport-record-list">
                    {(maintenance.events || []).length ? (
                      maintenance.events.map((item) => (
                        <div className="passport-record-list__item" key={`${item.id}-${item.event_at}`}>
                          <strong>{item.action}</strong>
                          <span>{item.actor} · {item.result} · {formatTimestamp(item.event_at)}</span>
                          <small>{item.parts_changed ? `Parts: ${item.parts_changed}` : "No parts declared"}{item.next_due ? ` · Next due ${item.next_due}` : ""}</small>
                        </div>
                      ))
                    ) : <ListEmpty text="No structured maintenance events have been captured yet." />}
                  </div>
                  <div className="passport-inline-form">
                    <div className="passport-inline-form__header"><strong>Add maintenance event</strong></div>
                    <div className="form-grid">
                      <Field label="Event timestamp"><input type="datetime-local" value={maintenanceForm.event_at} onChange={(event) => setMaintenanceForm((current) => ({ ...current, event_at: event.target.value }))} /></Field>
                      <Field label="Action"><input value={maintenanceForm.action} onChange={(event) => setMaintenanceForm((current) => ({ ...current, action: event.target.value }))} placeholder="commissioning" /></Field>
                      <Field label="Actor"><input value={maintenanceForm.actor} onChange={(event) => setMaintenanceForm((current) => ({ ...current, actor: event.target.value }))} placeholder="Integrator ABC" /></Field>
                      <Field label="Result"><select value={maintenanceForm.result} onChange={(event) => setMaintenanceForm((current) => ({ ...current, result: event.target.value }))}><option value="ok">ok</option><option value="warning">warning</option><option value="failed">failed</option></select></Field>
                      <Field label="Next due"><input type="date" value={maintenanceForm.next_due} onChange={(event) => setMaintenanceForm((current) => ({ ...current, next_due: event.target.value }))} /></Field>
                      <Field label="Parts changed" wide><input value={maintenanceForm.parts_changed} onChange={(event) => setMaintenanceForm((current) => ({ ...current, parts_changed: event.target.value }))} placeholder="tool insert, coolant filter" /></Field>
                      <Field label="Notes" wide><textarea value={maintenanceForm.notes} onChange={(event) => setMaintenanceForm((current) => ({ ...current, notes: event.target.value }))} placeholder="Details of the service activity." /></Field>
                    </div>
                    <div className="form-actions form-actions--tight">
                      <button type="button" className="primary-button primary-button--sm" onClick={handleCreateMaintenance} disabled={savingBlock === "maintenance"}>
                        {savingBlock === "maintenance" ? "Saving…" : "Add maintenance"}
                      </button>
                    </div>
                  </div>
                </Card>
              ) : null}

              {passportSection === "compliance" ? (
                <>
                  <Card>
                    <div className="card-header-row"><div><SectionLabel>Compliance</SectionLabel><h3>Certificates and declarations</h3></div></div>
                    <div className="passport-record-list">
                      {(compliance.items || []).length ? (
                        compliance.items.map((item) => (
                          <div className="passport-record-list__item" key={`${item.id}-${item.ref}`}>
                            <strong>{item.title}</strong>
                            <span>{item.certificate_type} · {item.status} · {item.issuer || "No issuer"}</span>
                            <small>{item.ref}{item.valid_until ? ` · valid until ${item.valid_until}` : ""}</small>
                          </div>
                        ))
                      ) : <ListEmpty text="No compliance artefacts are registered yet." />}
                    </div>
                    <div className="passport-inline-form">
                      <div className="passport-inline-form__header"><strong>Add certificate</strong></div>
                      <div className="form-grid">
                        <Field label="Certificate type"><select value={complianceForm.certificate_type} onChange={(event) => setComplianceForm((current) => ({ ...current, certificate_type: event.target.value }))}><option value="ce">ce</option><option value="declaration">declaration</option><option value="audit">audit</option><option value="test-report">test-report</option></select></Field>
                        <Field label="Status"><select value={complianceForm.status} onChange={(event) => setComplianceForm((current) => ({ ...current, status: event.target.value }))}><option value="active">active</option><option value="expired">expired</option><option value="draft">draft</option></select></Field>
                        <Field label="Title" wide><input value={complianceForm.title} onChange={(event) => setComplianceForm((current) => ({ ...current, title: event.target.value }))} /></Field>
                        <Field label="Reference" wide><input value={complianceForm.ref} onChange={(event) => setComplianceForm((current) => ({ ...current, ref: event.target.value }))} placeholder="doc://certificates/ce-cnc-01.pdf" /></Field>
                        <Field label="Issuer"><input value={complianceForm.issuer} onChange={(event) => setComplianceForm((current) => ({ ...current, issuer: event.target.value }))} /></Field>
                        <Field label="Valid from"><input type="date" value={complianceForm.valid_from} onChange={(event) => setComplianceForm((current) => ({ ...current, valid_from: event.target.value }))} /></Field>
                        <Field label="Valid until"><input type="date" value={complianceForm.valid_until} onChange={(event) => setComplianceForm((current) => ({ ...current, valid_until: event.target.value }))} /></Field>
                        <Field label="Notes" wide><textarea value={complianceForm.notes} onChange={(event) => setComplianceForm((current) => ({ ...current, notes: event.target.value }))} /></Field>
                      </div>
                      <div className="form-actions form-actions--tight">
                        <button type="button" className="primary-button primary-button--sm" onClick={handleCreateCompliance} disabled={savingBlock === "compliance"}>
                          {savingBlock === "compliance" ? "Saving…" : "Add certificate"}
                        </button>
                      </div>
                    </div>
                  </Card>

                  <div className="passport-secondary-grid">
                    <Card>
                      <div className="card-header-row"><div><SectionLabel>Access</SectionLabel><h3>Access policy</h3></div></div>
                      <div className="passport-record-list">
                        <div className="passport-record-list__item">
                          <strong>{(access.tier || "internal").toUpperCase()}</strong>
                          <span>{access.audience || "operators"}</span>
                          <small>{access.policy_ref || "No policy reference"}{access.contact ? ` · ${access.contact}` : ""}</small>
                        </div>
                      </div>
                      <div className="passport-inline-form">
                        <div className="passport-inline-form__header"><strong>Update access policy</strong></div>
                        <div className="form-grid">
                          <Field label="Tier"><select value={accessForm.access_tier} onChange={(event) => setAccessForm((current) => ({ ...current, access_tier: event.target.value }))}><option value="public">public</option><option value="internal">internal</option><option value="legitimate_interest">legitimate_interest</option><option value="regulator">regulator</option></select></Field>
                          <Field label="Audience"><input value={accessForm.audience} onChange={(event) => setAccessForm((current) => ({ ...current, audience: event.target.value }))} /></Field>
                          <Field label="Policy reference" wide><input value={accessForm.policy_ref} onChange={(event) => setAccessForm((current) => ({ ...current, policy_ref: event.target.value }))} placeholder="doc://policies/passport-access.pdf" /></Field>
                          <Field label="Contact"><input value={accessForm.contact} onChange={(event) => setAccessForm((current) => ({ ...current, contact: event.target.value }))} /></Field>
                          <Field label="Justification" wide><textarea value={accessForm.justification} onChange={(event) => setAccessForm((current) => ({ ...current, justification: event.target.value }))} /></Field>
                        </div>
                        <div className="form-actions form-actions--tight">
                          <button type="button" className="primary-button primary-button--sm" onClick={handleSaveAccess} disabled={savingBlock === "access"}>
                            {savingBlock === "access" ? "Saving…" : "Save access policy"}
                          </button>
                        </div>
                      </div>
                    </Card>

                    <Card>
                      <div className="card-header-row"><div><SectionLabel>Integrity</SectionLabel><h3>Revision and verification</h3></div></div>
                      <div className="passport-record-list">
                        <div className="passport-record-list__item">
                          <strong>Revision {integrity.revision || "1"}</strong>
                          <span>{integrity.signed_by || "Unsigned record"}</span>
                          <small>{integrity.record_hash || "No hash"}{integrity.last_verified_at ? ` · verified ${formatTimestamp(integrity.last_verified_at)}` : ""}</small>
                        </div>
                      </div>
                      <div className="passport-inline-form">
                        <div className="passport-inline-form__header"><strong>Update integrity record</strong></div>
                        <div className="form-grid">
                          <Field label="Revision"><input value={integrityForm.revision} onChange={(event) => setIntegrityForm((current) => ({ ...current, revision: event.target.value }))} /></Field>
                          <Field label="Signed by"><input value={integrityForm.signed_by} onChange={(event) => setIntegrityForm((current) => ({ ...current, signed_by: event.target.value }))} /></Field>
                          <Field label="Record hash" wide><input value={integrityForm.record_hash} onChange={(event) => setIntegrityForm((current) => ({ ...current, record_hash: event.target.value }))} placeholder="sha256:..." /></Field>
                          <Field label="Signature reference" wide><input value={integrityForm.signature_ref} onChange={(event) => setIntegrityForm((current) => ({ ...current, signature_ref: event.target.value }))} placeholder="doc://signatures/passport.sig" /></Field>
                          <Field label="Last verified at"><input type="datetime-local" value={integrityForm.last_verified_at} onChange={(event) => setIntegrityForm((current) => ({ ...current, last_verified_at: event.target.value }))} /></Field>
                        </div>
                        <div className="form-actions form-actions--tight">
                          <button type="button" className="primary-button primary-button--sm" onClick={handleSaveIntegrity} disabled={savingBlock === "integrity"}>
                            {savingBlock === "integrity" ? "Saving…" : "Save integrity"}
                          </button>
                        </div>
                      </div>
                    </Card>
                  </div>
                </>
              ) : null}

              {passportSection === "sustainability" ? (
                <>
                  <div className="passport-secondary-grid">
                    <Card>
                      <div className="card-header-row"><div><SectionLabel>Sustainability</SectionLabel><h3>Environmental profile</h3></div></div>
                      <div className="nameplate-grid">
                        <div className="nameplate-grid__cell"><dt>PCF</dt><dd>{sustainability.pcf_kg_co2e ? `${formatNumber(sustainability.pcf_kg_co2e, 2)} kgCO2e` : "--"}</dd></div>
                        <div className="nameplate-grid__cell"><dt>Energy class</dt><dd>{sustainability.energy_class || "--"}</dd></div>
                        <div className="nameplate-grid__cell"><dt>Recyclable ratio</dt><dd>{sustainability.recyclable_ratio !== undefined && sustainability.recyclable_ratio !== null ? formatPercent(sustainability.recyclable_ratio, 0) : "--"}</dd></div>
                        <div className="nameplate-grid__cell"><dt>Take-back</dt><dd>{sustainability.takeback_available ? "Available" : "Not declared"}</dd></div>
                        <div className="nameplate-grid__cell nameplate-grid__cell--wide"><dt>End of life</dt><dd>{sustainability.end_of_life_instructions || "--"}</dd></div>
                      </div>
                      <div className="passport-inline-form passport-inline-form--spaced">
                        <div className="passport-inline-form__header"><strong>Update sustainability profile</strong></div>
                        <div className="form-grid">
                          <Field label="PCF (kgCO2e)"><input value={sustainabilityForm.pcf_kg_co2e} onChange={(event) => setSustainabilityForm((current) => ({ ...current, pcf_kg_co2e: event.target.value }))} /></Field>
                          <Field label="Energy class"><input value={sustainabilityForm.energy_class} onChange={(event) => setSustainabilityForm((current) => ({ ...current, energy_class: event.target.value }))} placeholder="A" /></Field>
                          <Field label="Recyclable ratio"><input value={sustainabilityForm.recyclable_ratio} onChange={(event) => setSustainabilityForm((current) => ({ ...current, recyclable_ratio: event.target.value }))} placeholder="75" /></Field>
                          <Field label="Take-back available"><select value={String(sustainabilityForm.takeback_available)} onChange={(event) => setSustainabilityForm((current) => ({ ...current, takeback_available: event.target.value === "true" }))}><option value="false">no</option><option value="true">yes</option></select></Field>
                          <Field label="End of life instructions" wide><textarea value={sustainabilityForm.end_of_life_instructions} onChange={(event) => setSustainabilityForm((current) => ({ ...current, end_of_life_instructions: event.target.value }))} /></Field>
                        </div>
                        <div className="form-actions form-actions--tight">
                          <button type="button" className="primary-button primary-button--sm" onClick={handleSaveSustainability} disabled={savingBlock === "sustainability"}>
                            {savingBlock === "sustainability" ? "Saving…" : "Save sustainability"}
                          </button>
                        </div>
                      </div>
                    </Card>

                    <Card>
                      <div className="card-header-row"><div><SectionLabel>Custody</SectionLabel><h3>Ownership and lifecycle</h3></div></div>
                      <div className="passport-record-list">
                        {(custody.events || []).length ? (
                          custody.events.map((item) => (
                            <div className="passport-record-list__item" key={`${item.id}-${item.effective_at}`}>
                              <strong>{item.owner_name}</strong>
                              <span>{item.event_type} · {formatTimestamp(item.effective_at)}</span>
                              <small>{item.location || "No location"}{item.notes ? ` · ${item.notes}` : ""}</small>
                            </div>
                          ))
                        ) : <ListEmpty text="No custody history has been registered yet." />}
                      </div>
                      <div className="passport-inline-form">
                        <div className="passport-inline-form__header"><strong>Add custody event</strong></div>
                        <div className="form-grid">
                          <Field label="Event type"><select value={ownershipForm.event_type} onChange={(event) => setOwnershipForm((current) => ({ ...current, event_type: event.target.value }))}><option value="commissioned">commissioned</option><option value="transferred">transferred</option><option value="leased">leased</option><option value="refurbished">refurbished</option></select></Field>
                          <Field label="Owner name"><input value={ownershipForm.owner_name} onChange={(event) => setOwnershipForm((current) => ({ ...current, owner_name: event.target.value }))} /></Field>
                          <Field label="Effective at"><input type="datetime-local" value={ownershipForm.effective_at} onChange={(event) => setOwnershipForm((current) => ({ ...current, effective_at: event.target.value }))} /></Field>
                          <Field label="Location"><input value={ownershipForm.location} onChange={(event) => setOwnershipForm((current) => ({ ...current, location: event.target.value }))} /></Field>
                          <Field label="Notes" wide><textarea value={ownershipForm.notes} onChange={(event) => setOwnershipForm((current) => ({ ...current, notes: event.target.value }))} /></Field>
                        </div>
                        <div className="form-actions form-actions--tight">
                          <button type="button" className="primary-button primary-button--sm" onClick={handleCreateOwnership} disabled={savingBlock === "ownership"}>
                            {savingBlock === "ownership" ? "Saving…" : "Add custody event"}
                          </button>
                        </div>
                      </div>
                    </Card>
                  </div>
                </>
              ) : null}
            </>
          )}
        </div>
      )}

      <AddMachineModal
        open={modalOpen}
        profiles={profiles}
        onClose={() => setModalOpen(false)}
        onCreated={(assetRecord) => {
          onAssetCreated(assetRecord);
          onPassportRefresh(assetRecord.asset_id);
        }}
      />
    </>
  );
}
