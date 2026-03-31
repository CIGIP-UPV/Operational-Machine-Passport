function MachinesIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" aria-hidden="true">
      <rect x="4" y="5" width="16" height="12" rx="2" fill="none" stroke="currentColor" strokeWidth="1.8" />
      <path d="M8 19h8" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
    </svg>
  );
}

function MonitorIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" aria-hidden="true">
      <path
        d="M3 12h4l2.2-4.5L13 16l2.2-4H21"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.8"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function DiagnoseIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" aria-hidden="true">
      <path d="M12 4 21 20H3L12 4Z" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinejoin="round" />
      <path d="M12 9v4" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
      <circle cx="12" cy="16.5" r="1" fill="currentColor" />
    </svg>
  );
}

const TAB_ICONS = {
  machines: MachinesIcon,
  monitor: MonitorIcon,
  diagnose: DiagnoseIcon,
};

export default function TabNav({ activeTab, onChangeTab, anomalyCount }) {
  const tabs = [
    { id: "machines", label: "Machines" },
    { id: "monitor", label: "Monitor" },
    { id: "diagnose", label: "Diagnose" },
  ];

  return (
    <nav className="tab-nav" aria-label="Primary navigation">
      {tabs.map((tab) => {
        const Icon = TAB_ICONS[tab.id];
        return (
          <button
            key={tab.id}
            type="button"
            className={`tab-btn${activeTab === tab.id ? " active" : ""}`}
            data-tab={tab.id}
            onClick={() => onChangeTab(tab.id)}
          >
            <span className="tab-icon">
              <Icon />
            </span>
            <span>{tab.label}</span>
            {tab.id === "diagnose" && anomalyCount > 0 ? <span className="tab-badge">{anomalyCount}</span> : null}
          </button>
        );
      })}
    </nav>
  );
}
