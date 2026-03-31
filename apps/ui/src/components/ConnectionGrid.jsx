export default function ConnectionGrid({ cells = [] }) {
  return (
    <div className="connection-grid">
      {cells.map((cell) => (
        <div className="connection-grid__cell" key={cell.label}>
          <dt>{cell.label}</dt>
          <dd>{cell.value}</dd>
        </div>
      ))}
    </div>
  );
}
