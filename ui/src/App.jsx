import { startTransition, useEffect, useState } from "react";
import { getAssetPassport, getAssets, getDashboardState } from "./api.js";
import TabNav from "./components/TabNav.jsx";
import Topbar from "./components/Topbar.jsx";
import DiagnoseTab from "./views/DiagnoseTab.jsx";
import MachinesTab from "./views/MachinesTab.jsx";
import MonitorTab from "./views/MonitorTab.jsx";

const ACTIVE_TAB_KEY = "opc-observe-active-tab";

function initialTab() {
  if (typeof window === "undefined") {
    return "machines";
  }
  return window.sessionStorage.getItem(ACTIVE_TAB_KEY) || "machines";
}

export default function App() {
  const [registry, setRegistry] = useState({ generated_at: null, profiles: [], assets: [] });
  const [dashboard, setDashboard] = useState({ generated_at: null, pipeline: {}, assets: [] });
  const [passportPayload, setPassportPayload] = useState(null);
  const [activeTab, setActiveTab] = useState(initialTab);
  const [machineRegistryView, setMachineRegistryView] = useState(true);
  const [selectedAssetId, setSelectedAssetId] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    window.sessionStorage.setItem(ACTIVE_TAB_KEY, activeTab);
  }, [activeTab]);

  useEffect(() => {
    let cancelled = false;

    async function loadRegistry() {
      try {
        const payload = await getAssets();
        if (cancelled) {
          return;
        }
        startTransition(() => {
          setRegistry(payload);
          setError("");
          setLoading(false);
        });
      } catch (requestError) {
        if (!cancelled) {
          setError("Unable to load the asset registry. Ensure the analytics service is reachable.");
          setLoading(false);
        }
      }
    }

    loadRegistry();
    const timer = window.setInterval(loadRegistry, 8000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, []);

  useEffect(() => {
    if (!registry.assets.length) {
      return;
    }
    const exists = registry.assets.some((asset) => asset.asset_id === selectedAssetId);
    if (!selectedAssetId || !exists) {
      setSelectedAssetId(registry.assets[0].asset_id);
    }
  }, [registry.assets, selectedAssetId]);

  useEffect(() => {
    let cancelled = false;

    async function loadDashboard() {
      try {
        const payload = await getDashboardState(selectedAssetId || undefined);
        if (cancelled) {
          return;
        }
        startTransition(() => {
          setDashboard(payload);
          setError("");
        });
      } catch (requestError) {
        if (!cancelled) {
          setError("Unable to load the operational state for the selected machine.");
        }
      }
    }

    loadDashboard();
    const timer = window.setInterval(loadDashboard, 5000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [selectedAssetId]);

  async function loadPassport(assetId = selectedAssetId) {
    if (!assetId) {
      setPassportPayload(null);
      return;
    }
    try {
      const payload = await getAssetPassport(assetId);
      setPassportPayload(payload);
      setError("");
    } catch (requestError) {
      setPassportPayload(null);
    }
  }

  useEffect(() => {
    let cancelled = false;

    async function refreshPassport() {
      if (!selectedAssetId) {
        if (!cancelled) {
          setPassportPayload(null);
        }
        return;
      }
      try {
        const payload = await getAssetPassport(selectedAssetId);
        if (!cancelled) {
          setPassportPayload(payload);
        }
      } catch (requestError) {
        if (!cancelled) {
          setPassportPayload(null);
        }
      }
    }

    refreshPassport();
    const timer = window.setInterval(refreshPassport, 10000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [selectedAssetId]);

  const registryAsset = registry.assets.find((asset) => asset.asset_id === selectedAssetId) || registry.assets[0] || null;
  const currentAsset = dashboard.assets.find((asset) => asset.asset_id === selectedAssetId) || dashboard.assets[0] || null;
  const topbarAsset = currentAsset
    ? {
        ...(registryAsset || {}),
        ...currentAsset,
        connection: currentAsset.connection || registryAsset?.connection,
        primary_connection_type: currentAsset.primary_connection_type || registryAsset?.primary_connection_type,
      }
    : registryAsset;
  const anomalyCount = currentAsset?.diagnosis?.active_anomalies ?? currentAsset?.kpis?.active_anomalies ?? 0;

  function handleAssetCreated(assetRecord) {
    setRegistry((current) => {
      const exists = current.assets.some((asset) => asset.asset_id === assetRecord.asset_id);
      return {
        ...current,
        assets: exists ? current.assets : [assetRecord, ...current.assets],
      };
    });
    setSelectedAssetId(assetRecord.asset_id);
    setMachineRegistryView(false);
    setActiveTab("machines");
  }

  function handleOpenAsset(assetId) {
    setSelectedAssetId(assetId);
    setMachineRegistryView(false);
    setActiveTab("machines");
  }

  function handleOpenRegistry() {
    setMachineRegistryView(true);
    setActiveTab("machines");
  }

  return (
    <div className="app">
      <Topbar
        assetOptions={registry.assets}
        selectedAssetId={selectedAssetId}
        onChangeAsset={setSelectedAssetId}
        currentAsset={topbarAsset}
        pipeline={dashboard.pipeline}
        onOpenRegistry={handleOpenRegistry}
        registryView={activeTab === "machines" && machineRegistryView}
      />

      <TabNav activeTab={activeTab} onChangeTab={setActiveTab} anomalyCount={anomalyCount} />

      <main className="content-shell">
        {loading ? <div className="screen-state">Loading monitoring workspace…</div> : null}
        {!loading && error ? <div className="screen-state screen-state--error">{error}</div> : null}
        {!loading && !error && !registry.assets.length ? (
          <div className="screen-state">No machines are registered yet. Open the Machines tab and add the first OPC UA asset.</div>
        ) : null}

        {!loading && !error ? (
          <div className="tab-panels">
            <section className={`tab-panel${activeTab === "machines" ? " active" : ""}`}>
              <MachinesTab
                assets={registry.assets}
                profiles={registry.profiles}
                selectedAssetId={selectedAssetId}
                onSelectAsset={setSelectedAssetId}
                onOpenAsset={handleOpenAsset}
                passportPayload={passportPayload}
                onPassportRefresh={loadPassport}
                onAssetCreated={handleAssetCreated}
                registryView={machineRegistryView}
              />
            </section>

            <section className={`tab-panel${activeTab === "monitor" ? " active" : ""}`}>
              <MonitorTab
                asset={currentAsset}
                registryAsset={registryAsset}
                pipeline={dashboard.pipeline}
                passportPayload={passportPayload}
                onPassportRefresh={loadPassport}
              />
            </section>

            <section className={`tab-panel${activeTab === "diagnose" ? " active" : ""}`}>
              <DiagnoseTab asset={currentAsset} registryAsset={registryAsset} passportPayload={passportPayload} />
            </section>
          </div>
        ) : null}
      </main>
    </div>
  );
}
