import argparse
import json
from pathlib import Path

from analysis import aggregate_summaries, summarize_run, write_summary_csv


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate experiment outputs against ground truth.")
    parser.add_argument("inputs", nargs="+", help="Result JSON files or directories containing them.")
    args = parser.parse_args()

    result_files = []
    for raw_input in args.inputs:
        path = Path(raw_input)
        if path.is_dir():
            result_files.extend(sorted(path.glob("*.json")))
        else:
            result_files.append(path)

    summaries = [summarize_run(path) for path in result_files]
    aggregate = aggregate_summaries(summaries)

    if result_files:
        target_dir = result_files[0].parent
        write_summary_csv(summaries, target_dir / "evaluation_summary.csv")
        with (target_dir / "evaluation_summary.json").open("w", encoding="utf-8") as handle:
            json.dump({"runs": summaries, "aggregate": aggregate}, handle, indent=2)

    print(json.dumps({"runs": summaries, "aggregate": aggregate}, indent=2))


if __name__ == "__main__":
    main()
