"""Run the 5 business questions in sql/analytics/ and save results to reports/analytics/*.csv."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROOT, banner, get_spark  # noqa: E402

if __name__ == "__main__":
    spark = get_spark("analytics")
    out_dir = ROOT / "reports" / "analytics"
    out_dir.mkdir(parents=True, exist_ok=True)
    for f in sorted((ROOT / "sql" / "analytics").glob("*.sql")):
        banner(f"sql/analytics/{f.name}")
        df = spark.sql(f.read_text())
        pdf = df.toPandas()            # results are small aggregates -> safe to collect
        pdf.to_csv(out_dir / f"{f.stem}.csv", index=False)
        print(pdf.head(8).to_string(index=False))
        print(f"  ... {len(pdf)} rows -> reports/analytics/{f.stem}.csv")
    spark.stop()
