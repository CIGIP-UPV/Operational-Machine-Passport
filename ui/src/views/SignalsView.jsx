import { useDeferredValue, useEffect, useState } from "react";
import { getSignalSeries } from "../api.js";
import Card from "../components/Card.jsx";
import SectionLabel from "../components/SectionLabel.jsx";
import Sparkline from "../components/Sparkline.jsx";
import StatusBadge from "../components/StatusBadge.jsx";
import { formatNumber, formatValue, toneFromStatus } from "../utils.js";

const CATEGORY_FILTERS = ["all", "sensor", "status", "production", "maintenance", "alarm", "energy", "signal"];

function SearchIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" aria-hidden="true">
      <circle cx="11" cy="11" r="7" fill="none" stroke="currentColor" strokeWidth="2" />
      <path d="M20 20l-3.5-3.5" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
    </svg>
  );
}

export default function SignalsView({ asset, selectedSignal, onSelectSignal }) {
  const [category, setCategory] = useState("all");
  const [search, setSearch] = useState("");
  const [minutes, setMinutes] = useState(30);
  const [series, setSeries] = useState(null);
  const [loading, setLoading] = useState(false);
  const [modalOpen, setModalOpen] = useState(false);
  const deferredSearch = useDeferredValue(search);

  const filteredSignals = asset.signals.filter((signal) => {
    const matchesCategory = category === "all" || signal.category === category;
    const matchesSearch =
      !deferredSearch ||
      signal.display_name.toLowerCase().includes(deferredSearch.toLowerCase()) ||
      signal.signal.toLowerCase().includes(deferredSearch.toLowerCase()) ||
      signal.subsystem.toLowerCase().includes(deferredSearch.toLowerCase());
    return matchesCategory && matchesSearch;
  });

  const activeSignal = asset.signals.find((signal) => signal.signal_key === selectedSignal) || null;

  useEffect(() => {
    function onKeyDown(event) {
      if (event.key === "Escape") {
        setModalOpen(false);
      }
    }

    if (modalOpen) {
      window.addEventListener("keydown", onKeyDown);
      return () => window.removeEventListener("keydown", onKeyDown);
    }

    return undefined;
  }, [modalOpen]);

  useEffect(() => {
    let cancelled = false;

    async function loadSeries() {
      if (!asset?.asset_id || !activeSignal) {
        setSeries(null);
        return;
      }
      setLoading(true);
      try {
        const payload = await getSignalSeries({
          assetId: asset.asset_id,
          signal: activeSignal.signal,
          path: activeSignal.path || "",
          minutes,
        });
        if (!cancelled) {
          setSeries(payload);
        }
      } catch (error) {
        if (!cancelled) {
          setSeries(null);
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    loadSeries();
    return () => {
      cancelled = true;
    };
  }, [activeSignal, asset?.asset_id, minutes]);

  const seriesValues = (series?.points || []).map((point) => Number(point.value));
  const min = seriesValues.length ? Math.min(...seriesValues) : null;
  const max = seriesValues.length ? Math.max(...seriesValues) : null;
  const latest = seriesValues.length ? seriesValues[seriesValues.length - 1] : null;

  return (
    <div className="signals-page">
      <Card className="signals-filters">
        <div className="section-header">
          <div>
            <SectionLabel>Signals Explorer</SectionLabel>
            <h3>Semantic filters</h3>
          </div>
        </div>

        <div className="search-field">
          <span className="search-field__icon">
            <SearchIcon />
          </span>
          <input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search signal or subsystem" />
        </div>

        <div className="filter-block">
          <label className="filter-label">Category</label>
          <div className="pill-row">
            {CATEGORY_FILTERS.map((item) => (
              <button
                key={item}
                type="button"
                className={`pill-button${category === item ? " pill-button--active" : ""}`}
                onClick={() => setCategory(item)}
              >
                {item}
              </button>
            ))}
          </div>
        </div>

        <div className="filter-block">
          <label className="filter-label">Window</label>
          <div className="pill-row">
            {[15, 30, 60].map((item) => (
              <button
                key={item}
                type="button"
                className={`pill-button${minutes === item ? " pill-button--active" : ""}`}
                onClick={() => setMinutes(item)}
              >
                {item}m
              </button>
            ))}
          </div>
        </div>

        <div className="signal-list-cards">
          {filteredSignals.map((signal) => (
            <Card
              className={`signal-list-card${selectedSignal === signal.signal_key ? " signal-list-card--selected" : ""}`}
              hoverable
              key={signal.signal_key}
            >
              <button
                type="button"
                className="signal-list-card__button"
                onClick={() => onSelectSignal(signal.signal_key)}
                onDoubleClick={() => {
                  onSelectSignal(signal.signal_key);
                  setModalOpen(true);
                }}
              >
                <div className="signal-list-card__spark">
                  <Sparkline
                    data={signal.trend || [signal.value]}
                    width={60}
                    height={24}
                    color={selectedSignal === signal.signal_key ? "#3b82f6" : "#94a3b8"}
                  />
                </div>
                <div className="signal-list-card__meta">
                  <strong>{signal.display_name}</strong>
                  <span>
                    {signal.category} · {signal.subsystem}
                  </span>
                </div>
                <StatusBadge tone={toneFromStatus(signal.severity)} label={signal.severity} />
              </button>
            </Card>
          ))}
        </div>
      </Card>

      <Card>
        <div className="chart-empty">
          Double click any signal to open the detailed trace in a modal. Single click keeps the signal selected in the list.
        </div>
      </Card>

      {modalOpen && activeSignal ? (
        <div className="modal-backdrop" onClick={() => setModalOpen(false)} role="presentation">
          <div className="modal-shell" onClick={(event) => event.stopPropagation()} role="dialog" aria-modal="true" aria-label={`${activeSignal.display_name} detail`}>
            <div className="modal-shell__header">
              <div>
                <SectionLabel>Historical Trace</SectionLabel>
                <h3>{activeSignal.display_name}</h3>
              </div>
              <div className="modal-shell__header-actions">
                <StatusBadge tone="medium" label={activeSignal.criticality} />
                <button type="button" className="modal-close" onClick={() => setModalOpen(false)}>
                  Close
                </button>
              </div>
            </div>

            <div className="signal-detail-grid signal-detail-grid--modal">
              <Card className="signal-detail-card signal-detail-card--trace">
                <div className="detail-metrics-row">
                  <div className="detail-metric">
                    <span>Latest value</span>
                    <strong>{formatValue(activeSignal.value, activeSignal.unit)}</strong>
                  </div>
                  <div className="detail-metric">
                    <span>Subsystem</span>
                    <strong>{activeSignal.subsystem}</strong>
                  </div>
                  <div className="detail-metric">
                    <span>Detector votes</span>
                    <strong>{activeSignal.detector_vote_total}</strong>
                  </div>
                  <div className="detail-metric">
                    <span>Normalized score</span>
                    <strong>{formatNumber(activeSignal.anomaly_score, 2)}</strong>
                  </div>
                </div>

                {loading ? (
                  <div className="chart-empty">Loading time series…</div>
                ) : (
                  <>
                    <Sparkline data={seriesValues} width={340} height={100} color="#3b82f6" filled className="sparkline--large" />
                    <div className="trace-summary">
                      <span>Min {min === null ? "--" : formatNumber(min, 2)}</span>
                      <span>Max {max === null ? "--" : formatNumber(max, 2)}</span>
                      <span>Latest {latest === null ? "--" : formatNumber(latest, 2)}</span>
                    </div>
                  </>
                )}
              </Card>

              <Card className="signal-detail-card">
                <div className="section-header">
                  <div>
                    <SectionLabel>Detector State</SectionLabel>
                    <h3>Current detector outputs</h3>
                  </div>
                </div>

                <div className="detector-state-row">
                  <div className="detector-state-box">
                    <span className="detector-state-box__label">RULES</span>
                    <strong>{formatNumber(activeSignal.detectors.rules.score, 2)}</strong>
                    <StatusBadge tone={activeSignal.detectors.rules.flag ? "warning" : "nominal"} label={activeSignal.detectors.rules.flag ? "triggered" : "nominal"} />
                  </div>
                  <div className="detector-state-box detector-state-box--amber">
                    <span className="detector-state-box__label">ZSCORE</span>
                    <strong>{formatNumber(activeSignal.detectors.zscore.score, 2)}</strong>
                    <StatusBadge tone={activeSignal.detectors.zscore.flag ? "warning" : "nominal"} label={activeSignal.detectors.zscore.flag ? "triggered" : "nominal"} />
                  </div>
                  <div className="detector-state-box detector-state-box--violet">
                    <span className="detector-state-box__label">MAD</span>
                    <strong>{formatNumber(activeSignal.detectors.mad.score, 2)}</strong>
                    <StatusBadge tone={activeSignal.detectors.mad.flag ? "warning" : "nominal"} label={activeSignal.detectors.mad.flag ? "triggered" : "nominal"} />
                  </div>
                </div>
              </Card>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
