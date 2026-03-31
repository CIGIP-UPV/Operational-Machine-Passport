export default function Card({ children, className = "", hoverable = false }) {
  return <section className={`card${hoverable ? " card--hoverable" : ""}${className ? ` ${className}` : ""}`}>{children}</section>;
}
