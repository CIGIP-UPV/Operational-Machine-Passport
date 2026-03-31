import Card from "../components/Card.jsx";
import SectionLabel from "../components/SectionLabel.jsx";
import StatusBadge from "../components/StatusBadge.jsx";
import Tag from "../components/Tag.jsx";
import { formatRelativeTime, toneFromConnectionStatus, titleFromAsset } from "../utils.js";

function AssetRegistryCard({ asset, onOpenAsset }) {
  const connectionTone = toneFromConnectionStatus(asset.connection_status);
  const live = asset.live || {};

  return (
    <Card className="registry-card" hoverable>
      <div className="registry-card__header">
        <div>
          <SectionLabel>Registered Asset</SectionLabel>
          <h3>{titleFromAsset(asset)}</h3>
          <p className="card-copy">{asset.description || "OPC UA asset registered in the platform and ready for discovery or live monitoring."}</p>
        </div>
        <div className="registry-card__badges">
          <StatusBadge tone={connectionTone} label={asset.connection_status || "unknown"} />
          <Tag tone="blue" label={(asset.asset_type || "generic").toUpperCase()} />
        </div>
      </div>

      <div className="registry-card__meta-grid">
        <div className="metric-cell">
          <span>Endpoint</span>
          <strong>{asset.opcua_endpoint || "Not configured"}</strong>
        </div>
        <div className="metric-cell">
          <span>Profile</span>
          <strong>{asset.profile_id || "generic"}</strong>
        </div>
        <div className="metric-cell">
          <span>Signals tracked</span>
          <strong>{live.signals_tracked ?? 0}</strong>
        </div>
        <div className="metric-cell">
          <span>Health score</span>
          <strong>{asset.passport_summary?.health_score ?? 0}</strong>
        </div>
        <div className="metric-cell">
          <span>Last seen</span>
          <strong>{formatRelativeTime(asset.last_seen_at)}</strong>
        </div>
        <div className="metric-cell">
          <span>Coverage</span>
          <strong>{asset.passport_summary?.coverage_ratio ?? 0}%</strong>
        </div>
      </div>

      <div className="registry-card__footer">
        <button type="button" className="secondary-button" onClick={() => onOpenAsset(asset.asset_id, "passport")}>
          Open Passport
        </button>
        <button type="button" className="secondary-button" onClick={() => onOpenAsset(asset.asset_id, "overview")}>
          Open Operations
        </button>
      </div>
    </Card>
  );
}

export default function MachineRegistryView({ assets, onOpenAsset, onAddMachine }) {
  const connected = assets.filter((asset) => ["connected", "monitored"].includes(asset.connection_status)).length;

  return (
    <div className="dashboard-stack">
      <Card className="overview-hero">
        <div className="overview-hero__content">
          <SectionLabel>Machine Registry</SectionLabel>
          <h2>Persistent catalog of OPC UA assets</h2>
          <p>
            Register machines, store their connection profile, review their current observability state and open the digital
            passport for each asset from a single workspace.
          </p>
          <div className="overview-hero__tags">
            <Tag tone="blue" label={`${assets.length} ASSETS`} />
            <Tag tone={connected ? "green" : "orange"} label={`${connected} CONNECTED`} />
          </div>
        </div>

        <div className="registry-hero__actions">
          <button type="button" className="primary-button" onClick={onAddMachine}>
            Add OPC UA Machine
          </button>
        </div>
      </Card>

      <div className="kpi-row">
        <Card className="kpi-card">
          <SectionLabel>Assets Registered</SectionLabel>
          <div className="kpi-card__value">{assets.length}</div>
          <p className="kpi-card__detail">Machines stored in the persistent registry.</p>
        </Card>
        <Card className="kpi-card">
          <SectionLabel>Assets Connected</SectionLabel>
          <div className="kpi-card__value kpi-card__value--green">{connected}</div>
          <p className="kpi-card__detail">Registered machines with a valid recent connection status.</p>
        </Card>
        <Card className="kpi-card">
          <SectionLabel>Observed Signals</SectionLabel>
          <div className="kpi-card__value">{assets.reduce((total, asset) => total + (asset.live?.signals_tracked || 0), 0)}</div>
          <p className="kpi-card__detail">Semantic signals currently available across the registry.</p>
        </Card>
      </div>

      <div className="dashboard-stack">
        {assets.map((asset) => (
          <AssetRegistryCard asset={asset} key={asset.asset_id} onOpenAsset={onOpenAsset} />
        ))}
      </div>
    </div>
  );
}
