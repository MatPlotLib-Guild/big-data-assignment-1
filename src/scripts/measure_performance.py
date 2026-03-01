from dataclasses import dataclass
import time
import sys
from pathlib import Path
from typing import Literal, Optional
import tyro
import polars as pl

sys.path.append(str(Path(__file__).parent.parent.parent))


@dataclass
class Args:
    db: Literal["postgres", "citus", "scylla", "mongo"]
    query: Literal[1, 2, 3, 4]
    runs: int = 3
    out: str = "report/benchmark_results.tsv"
    print_result: bool = False
    save_results_dir: Optional[str] = None
    q4_category: Literal["Restaurant", "Club", "Museum", "Shop", "Others"] = "Restaurant"
    warmup: bool = True


def main():
    args = tyro.cli(Args)

    if args.db == "postgres":
        import src.queries.postgres_queries as q_mod
    elif args.db == "citus":
        import src.queries.citus_queries as q_mod
    elif args.db == "scylla":
        import src.queries.scylla_queries as q_mod
    elif args.db == "mongo":
        import src.queries.mongodb_queries as q_mod
    else:
        raise ValueError(f"Invalid database: {args.db}")

    conn = q_mod.connect()
    run_fn = getattr(q_mod, f"run_q{args.query}")

    results = []

    if args.query == 4:
        run_kwargs = {"category": args.q4_category}
    else:
        run_kwargs = {}

    if args.warmup:
        print(f"[{args.db.upper()}] Warming up Q{args.query}...")
        run_fn(conn, **run_kwargs)

    for i in range(args.runs):
        start = time.perf_counter()
        query_result: pl.DataFrame = run_fn(conn, **run_kwargs)
        elapsed = time.perf_counter() - start
        if i == 0 and args.print_result:
            print(query_result)

        row = {"database": args.db, "query": f"Q{args.query}", "run": i + 1, "execution_time_s": elapsed}
        if args.save_results_dir:
            save_result_path = Path(args.save_results_dir) / f"{args.db}_Q{args.query}_run{i + 1}.parquet"
            save_result_path.parent.mkdir(parents=True, exist_ok=True)
            query_result.write_parquet(save_result_path)
            row["save_result_path"] = str(save_result_path)

        results.append(row)
        print(f"  Run {i + 1}: {elapsed:.3f}s")

    q_mod.close(conn)

    df = pl.DataFrame(results)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists():
        with open(out_path, "ab") as f:
            df.write_csv(f, separator="\t", include_header=False)
    else:
        with open(out_path, "wb") as f:
            df.write_csv(f, separator="\t", include_header=True)


if __name__ == "__main__":
    main()
