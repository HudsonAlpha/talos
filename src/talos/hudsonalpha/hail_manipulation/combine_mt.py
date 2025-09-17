#!/usr/bin/env python3
import argparse
import sys
import os
import hail as hl


def init_hail():
    """Initialize Hail with Spark settings taken from env vars."""
    hl.init(
        master=f"local[{os.environ['SPARK_CORES']}]",
        spark_conf={
            "spark.driver.memory": "200g",   # adjust as needed
            "spark.local.dir": os.environ["SPARK_LOCAL_DIRS"],
            "spark.sql.shuffle.partitions": "256",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        },
        log="hail.log"
    )


def drop_duplicate_samples(datasets):
    """
    Drop duplicate samples across datasets according to priority order.

    Parameters
    ----------
    datasets : dict[str, MatrixTable]

    Returns
    -------
    prioritized_datasets : list[(str, MatrixTable)]
    dropped_samples : dict[str, list[str]]
    """
    all_seen_samples = set()
    dropped_samples = {}
    prioritized_datasets = []

    for label, mt in datasets.items():
        current_samples = set(mt.s.collect())
        duplicates_here = current_samples & all_seen_samples

        if duplicates_here:
            print(f"{label}: dropping {len(duplicates_here)} duplicate samples "
                  f"(seen already in higher‑priority dataset)")
            dropped_samples[label] = sorted(duplicates_here)
            mt = mt.filter_cols(~hl.literal(duplicates_here).contains(mt.s))

        all_seen_samples |= current_samples
        prioritized_datasets.append((label, mt))

    return prioritized_datasets, dropped_samples


def align_row_fields(prioritized_datasets, mode="intersection"):
    """
    Align non-key row fields across datasets.
    Row keys (e.g. locus, alleles) are preserved automatically.

    Returns
    -------
    row_aligned_datasets : list[(str, MatrixTable)]
    shared_row_fields : set[str]
    """
    first_label, first_mt = prioritized_datasets[0]
    row_key_fields = set(first_mt.row_key)

    if mode == "intersection":
        shared_row_fields = set(first_mt.row.dtype.fields) - row_key_fields
        for _, mt in prioritized_datasets[1:]:
            shared_row_fields &= set(mt.row.dtype.fields) - row_key_fields
    else:  # union mode
        shared_row_fields = set()
        for _, mt in prioritized_datasets:
            shared_row_fields |= set(mt.row.dtype.fields) - row_key_fields

    row_aligned_datasets = []
    for label, mt in prioritized_datasets:
        selected_row_exprs = {}
        for field in shared_row_fields:
            if field in mt.row:
                selected_row_exprs[field] = mt.row[field]
            else:
                # Fill missing with null of appropriate dtype
                for _, other_mt in prioritized_datasets:
                    if field in other_mt.row:
                        selected_row_exprs[field] = hl.null(other_mt.row[field].dtype)
                        break
        row_aligned_datasets.append((label, mt.select_rows(**selected_row_exprs)))

    return row_aligned_datasets, shared_row_fields


def align_entry_fields(row_aligned_datasets, mode="intersection"):
    """
    Align entry fields across datasets (intersection or union).

    Returns
    -------
    entry_aligned_datasets : list[(str, MatrixTable)]
    shared_entry_fields : set[str]
    """
    first_label, first_mt = row_aligned_datasets[0]

    if mode == "intersection":
        shared_entry_fields = set(first_mt.entry.dtype.fields)
        for _, mt in row_aligned_datasets[1:]:
            shared_entry_fields &= set(mt.entry.dtype.fields)
    else:  # union mode
        shared_entry_fields = set()
        for _, mt in row_aligned_datasets:
            shared_entry_fields |= set(mt.entry.dtype.fields)

    entry_aligned_datasets = []
    for label, mt in row_aligned_datasets:
        selected_entry_exprs = {}
        for field in shared_entry_fields:
            if field in mt.entry:
                selected_entry_exprs[field] = mt.entry[field]
            else:
                # Fill missing with null of appropriate dtype
                for _, other_mt in row_aligned_datasets:
                    if field in other_mt.entry:
                        selected_entry_exprs[field] = hl.null(other_mt.entry[field].dtype)
                        break
        entry_aligned_datasets.append((label, mt.select_entries(**selected_entry_exprs)))

    return entry_aligned_datasets, shared_entry_fields


def harmonize_and_union_named(datasets: dict, mode: str = "intersection"):
    """
    Harmonize schemas and combine multiple MatrixTables by columns.
    Respects dataset order for sample priority (first wins).
    """
    # Step 1: remove duplicate samples
    prioritized_datasets, dropped_samples = drop_duplicate_samples(datasets)

    # Step 2: align row fields
    row_aligned_datasets, shared_row_fields = align_row_fields(prioritized_datasets, mode=mode)

    # Step 3: align entry fields
    entry_aligned_datasets, shared_entry_fields = align_entry_fields(row_aligned_datasets, mode=mode)

    # Step 4: perform union along columns (outer join on rows)
    _, combined_mt = entry_aligned_datasets[0]
    for _, mt in entry_aligned_datasets[1:]:
        combined_mt = combined_mt.union_cols(mt, row_join_type="outer")

    return combined_mt, dropped_samples


def main(args):
    init_hail()

    # Build dataset dict with priority defined by CLI order
    datasets = {}
    for i, path in enumerate(args.inputs):
        name = f"input{i+1}"
        print(f"Reading {path} as {name}")
        datasets[name] = hl.read_matrix_table(path)

    combined, dropped = harmonize_and_union_named(datasets, mode=args.mode)

    print("\nDropped samples summary:")
    for ds, samples in dropped.items():
        print(f"  {ds}: {samples}")

    print(f"\nWriting combined MatrixTable to {args.output} ...")
    combined.write(args.output, overwrite=True)
    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Combine multiple Hail MatrixTables with schema alignment."
    )
    parser.add_argument(
        "--output", required=True,
        help="Output MatrixTable path (e.g. combined.mt/)"
    )
    parser.add_argument(
        "--mode", choices=["intersection", "union"], default="intersection",
        help="Schema harmonization mode: intersection (default) or union"
    )
    parser.add_argument(
        "inputs", nargs="+",
        help="Input MatrixTable paths (.mt/). "
             "Order = priority (first wins in duplicate samples)."
    )
    args = parser.parse_args()
    sys.exit(main(args))