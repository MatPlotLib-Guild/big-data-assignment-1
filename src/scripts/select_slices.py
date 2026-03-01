from pathlib import Path
import polars as pl

CHECKINS_COLS = ["user_id", "venue_id", "utc_time", "timezone_offset_mins"]
FRIENDSHIP_COLS = ["user_id", "friend_id"]
POIS_COLS = ["venue_id", "latitude", "longitude", "category", "country"]


def scan_tsv(path: Path, columns: list[str]) -> pl.LazyFrame:
    return pl.scan_csv(path, separator="\t", has_header=False, new_columns=columns)


def sink_tsv(lf: pl.LazyFrame, path: Path) -> None:
    lf.sink_csv(path, separator="\t")
    n = pl.scan_csv(path, separator="\t").select(pl.len()).collect().item()
    print(f"  -> saved {path.name} ({n:,} rows)")


def main():
    data_dir = Path("data")

    user_ids = pl.read_csv(data_dir / "my_users.csv")["userid"].implode()
    print(f"Loaded {len(user_ids[0]):,} user IDs from my_users.csv")
    print("Filtering checkins...")
    out_checkins = data_dir / "my_checkins_anonymized.tsv"
    sink_tsv(scan_tsv(data_dir / "checkins_anonymized.txt", CHECKINS_COLS).filter(pl.col("user_id").is_in(user_ids)), out_checkins)
    print("Filtering friendships_before...")
    sink_tsv(
        scan_tsv(data_dir / "friendship_before_old.txt", FRIENDSHIP_COLS).filter(pl.col("user_id").is_in(user_ids) & pl.col("friend_id").is_in(user_ids)),
        data_dir / "my_friendship_before.tsv",
    )
    print("Filtering friendships_after...")
    sink_tsv(
        scan_tsv(data_dir / "friendship_after_new.txt", FRIENDSHIP_COLS).filter(pl.col("user_id").is_in(user_ids) & pl.col("friend_id").is_in(user_ids)),
        data_dir / "my_friendship_after.tsv",
    )
    print("Filtering POIs (by venues in my checkins)...")
    venue_ids = pl.scan_csv(out_checkins, separator="\t").select("venue_id").unique().collect()["venue_id"].implode()
    print(f"  -> {len(venue_ids[0]):,} unique venues in my checkins")
    sink_tsv(scan_tsv(data_dir / "POIs.txt", POIS_COLS).filter(pl.col("venue_id").is_in(venue_ids)), data_dir / "my_POIs.tsv")
    print("Done.")


if __name__ == "__main__":
    main()
