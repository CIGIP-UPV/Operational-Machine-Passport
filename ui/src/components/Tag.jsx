export default function Tag({ tone = "green", label }) {
  return <span className={`tag tag--${tone}`}>{label}</span>;
}
