import Card from "./Card.jsx";
import SectionLabel from "./SectionLabel.jsx";
import StatusBadge from "./StatusBadge.jsx";
import { titleFromAsset, toneFromConnectionStatus } from "../utils.js";

export default function Sidebar({ sections, activeView, onSelectView, currentAsset }) {
  return (
    <aside className="sidebar">
      <div className="sidebar__brand">
        <div className="brand-mark">
          <span className="brand-mark__opc">OPC</span>
          <span className="brand-mark__observe"> Observe</span>
        </div>
      </div>

      <Card className="sidebar__asset-card">
        <SectionLabel>Active Asset</SectionLabel>
        <strong className="sidebar__asset-name">{currentAsset ? titleFromAsset(currentAsset) : "Waiting for asset"}</strong>
        <div className="sidebar__asset-meta">
          <StatusBadge
            tone={currentAsset?.connection_status ? toneFromConnectionStatus(currentAsset.connection_status) : "warning"}
            label={currentAsset?.connection_status || currentAsset?.status || "No data"}
          />
          <span className="sidebar__asset-type">{currentAsset?.asset_type || "generic"}</span>
        </div>
      </Card>

      <nav className="sidebar__nav" aria-label="Main navigation">
        {sections.map((section) => (
          <div className="sidebar__section" key={section.title}>
            <span className="sidebar__section-heading">{section.title}</span>
            {section.items.map((item) => (
              <button
                className={`sidebar__item${activeView === item.id ? " sidebar__item--active" : ""}`}
                key={item.id}
                onClick={() => onSelectView(item.id)}
                type="button"
              >
                <span className="sidebar__item-label">{item.label}</span>
                <span className="sidebar__item-copy">{item.copy}</span>
              </button>
            ))}
          </div>
        ))}
      </nav>
    </aside>
  );
}
