import argparse
from src.benchmark import print_comparison_tables
from src.generator import generate_synthetic_batches
from src.pipeline import IngestionEngine

def main():
    parser = argparse.ArgumentParser(description="Compare Data Load Strategies in DuckDB")
    parser.add_argument("--scale", choices=["small", "large"], default="small")
    args = parser.parse_args()

    base_cnt = 5000 if args.scale == "small" else 100000
    delta_cnt = 1000 if args.scale == "small" else 20000

    print(f"[*] Benchmark Running (Scale: {args.scale}, Base: {base_cnt}, Delta: {delta_cnt})...")
    batch_1, batch_2 = generate_synthetic_batches(seed=42, base_count=base_cnt, delta_count=delta_cnt)
    engine = IngestionEngine(":memory:")

    # 1. Full Refresh: In real architectures, full refresh processes all historical source snapshots
    print("[+] Strategy 1: Full Refresh")
    engine.load_full_refresh(batch_1, "BATCH_1")
    # For full refresh after delta, latest records naturally supersede prior versions:
    full_accumulated = batch_1 + batch_2
    engine.load_full_refresh(full_accumulated, "BATCH_2")

    # 2. Incremental Merge: processes only delta
    print("[+] Strategy 2: Incremental Merge")
    engine.load_incremental_merge(batch_1, "BATCH_1")
    engine.load_incremental_merge(batch_2, "BATCH_2")

    # 3. Partition Replacement: processes only affected partitions
    print("[+] Strategy 3: Partition Replacement")
    engine.load_partition_replacement(batch_1, "BATCH_1")
    engine.load_partition_replacement(batch_2, "BATCH_2")

    print_comparison_tables(engine.con)

if __name__ == "__main__":
    main()
