#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "google-cloud-monitoring>=2.0.0",
# ]
# ///
"""
Google Cloud Monitoring Metrics Query Tool

Query metrics from Google Cloud Monitoring API with customizable
metric types and filters.
"""

import argparse
import csv
import json
import sys
from datetime import datetime, timedelta, timezone
from io import StringIO
from typing import Optional


from google.cloud import monitoring_v3


def parse_duration(duration_str: str) -> timedelta:
    """Parse duration string like '1h', '30m', '7d' into timedelta."""
    unit = duration_str[-1].lower()
    value = int(duration_str[:-1])

    if unit == 'm':
        return timedelta(minutes=value)
    elif unit == 'h':
        return timedelta(hours=value)
    elif unit == 'd':
        return timedelta(days=value)
    else:
        raise ValueError(f"Unknown duration unit: {unit}. Use 'm', 'h', or 'd'")


def parse_timestamp(ts_str: str) -> datetime:
    """Parse ISO format timestamp string into datetime."""
    ts_str = ts_str.strip()

    for fmt in [
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]:
        try:
            dt = datetime.strptime(ts_str.replace("Z", "+0000"), fmt.replace("Z", "%z"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue

    raise ValueError(f"Unable to parse timestamp: {ts_str}")


def extract_value(point_value) -> float | int | str:
    """Extract numeric value from a TypedValue."""
    val = point_value.double_value
    if point_value.int64_value != 0:
        val = point_value.int64_value
    elif point_value.string_value:
        val = point_value.string_value
    elif point_value.distribution_value and point_value.distribution_value.count:
        val = point_value.distribution_value.mean
    return val


def compute_stats(values: list[float]) -> dict:
    """Compute statistics for a list of values."""
    if not values:
        return {}

    sorted_vals = sorted(values)
    n = len(sorted_vals)

    def percentile(p: float) -> float:
        idx = int(p * (n - 1))
        return sorted_vals[idx]

    return {
        "min": min(values),
        "max": max(values),
        "avg": sum(values) / n,
        "p50": percentile(0.50),
        "p95": percentile(0.95),
        "p99": percentile(0.99),
        "count": n,
    }


def build_series_label(series, group_by: Optional[list[str]] = None) -> str:
    """Build a label string for a time series."""
    labels = {}
    labels.update(series.metric.labels or {})
    labels.update(series.resource.labels or {})

    if group_by:
        parts = [f"{k}={labels.get(k, '?')}" for k in group_by]
    else:
        # Default: use container_name, pod_name, namespace_name if available
        key_labels = ["container_name", "pod_name", "namespace_name"]
        parts = [f"{k}={labels[k]}" for k in key_labels if k in labels]
        if not parts:
            parts = [f"{k}={v}" for k, v in list(labels.items())[:3]]

    return ", ".join(parts)


def query_metrics(
    project_id: str,
    metric_type: str,
    filters: Optional[list[str]] = None,
    duration: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    alignment_period: int = 60,
    per_series_aligner: str = "ALIGN_RATE",
    reducer: Optional[str] = None,
    group_by: Optional[list[str]] = None,
    output_format: str = "table",
    top_n: Optional[int] = None,
    show_stats: bool = False,
    latest_only: bool = False,
) -> None:
    """Query metrics from Google Cloud Monitoring."""
    client = monitoring_v3.MetricServiceClient()
    project_name = f"projects/{project_id}"

    # Build the filter string
    filter_parts = [f'metric.type = "{metric_type}"']
    if filters:
        filter_parts.extend(filters)
    filter_str = " AND ".join(filter_parts)

    # Set up the time interval
    now = datetime.now(timezone.utc)

    if start:
        start_time = parse_timestamp(start)
        end_time = parse_timestamp(end) if end else now
    else:
        duration = duration or "1h"
        delta = parse_duration(duration)
        start_time = now - delta
        end_time = now

    interval = monitoring_v3.TimeInterval(
        start_time=start_time,
        end_time=end_time,
    )

    # Set up aggregation
    aligner_map = {
        "ALIGN_RATE": monitoring_v3.Aggregation.Aligner.ALIGN_RATE,
        "ALIGN_MEAN": monitoring_v3.Aggregation.Aligner.ALIGN_MEAN,
        "ALIGN_SUM": monitoring_v3.Aggregation.Aligner.ALIGN_SUM,
        "ALIGN_MIN": monitoring_v3.Aggregation.Aligner.ALIGN_MIN,
        "ALIGN_MAX": monitoring_v3.Aggregation.Aligner.ALIGN_MAX,
        "ALIGN_NONE": monitoring_v3.Aggregation.Aligner.ALIGN_NONE,
        "ALIGN_DELTA": monitoring_v3.Aggregation.Aligner.ALIGN_DELTA,
    }

    reducer_map = {
        "REDUCE_NONE": monitoring_v3.Aggregation.Reducer.REDUCE_NONE,
        "REDUCE_MEAN": monitoring_v3.Aggregation.Reducer.REDUCE_MEAN,
        "REDUCE_SUM": monitoring_v3.Aggregation.Reducer.REDUCE_SUM,
        "REDUCE_MIN": monitoring_v3.Aggregation.Reducer.REDUCE_MIN,
        "REDUCE_MAX": monitoring_v3.Aggregation.Reducer.REDUCE_MAX,
        "REDUCE_COUNT": monitoring_v3.Aggregation.Reducer.REDUCE_COUNT,
        "REDUCE_PERCENTILE_99": monitoring_v3.Aggregation.Reducer.REDUCE_PERCENTILE_99,
        "REDUCE_PERCENTILE_95": monitoring_v3.Aggregation.Reducer.REDUCE_PERCENTILE_95,
        "REDUCE_PERCENTILE_50": monitoring_v3.Aggregation.Reducer.REDUCE_PERCENTILE_50,
    }

    aligner = aligner_map.get(per_series_aligner.upper())
    if aligner is None:
        print(f"Unknown aligner: {per_series_aligner}", file=sys.stderr)
        print(f"Available: {', '.join(aligner_map.keys())}", file=sys.stderr)
        sys.exit(1)

    aggregation_params = {
        "alignment_period": {"seconds": alignment_period},
        "per_series_aligner": aligner,
    }

    if reducer:
        cross_reducer = reducer_map.get(reducer.upper())
        if cross_reducer is None:
            print(f"Unknown reducer: {reducer}", file=sys.stderr)
            print(f"Available: {', '.join(reducer_map.keys())}", file=sys.stderr)
            sys.exit(1)
        aggregation_params["cross_series_reducer"] = cross_reducer
        if group_by:
            aggregation_params["group_by_fields"] = [f"resource.labels.{g}" if not g.startswith("resource.") and not g.startswith("metric.") else g for g in group_by]

    aggregation = monitoring_v3.Aggregation(**aggregation_params)

    # Print header for non-JSON/CSV output
    if output_format == "table":
        print(f"Querying metrics...", file=sys.stderr)
        print(f"  Project: {project_id}", file=sys.stderr)
        print(f"  Metric:  {metric_type}", file=sys.stderr)
        print(f"  Filter:  {filter_str}", file=sys.stderr)
        print(f"  Range:   {start_time.isoformat()} to {end_time.isoformat()}", file=sys.stderr)
        print(f"  Aligner: {per_series_aligner}", file=sys.stderr)
        if reducer:
            print(f"  Reducer: {reducer}", file=sys.stderr)
        print("-" * 60, file=sys.stderr)

    try:
        results = client.list_time_series(
            request={
                "name": project_name,
                "filter": filter_str,
                "interval": interval,
                "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
                "aggregation": aggregation,
            }
        )

        # Collect all series data
        all_series = []
        for series in results:
            labels = {}
            labels.update(series.metric.labels or {})
            labels.update(series.resource.labels or {})

            points = []
            values = []
            for point in series.points:
                ts = point.interval.end_time
                val = extract_value(point.value)
                points.append({"timestamp": ts.isoformat(), "value": val})
                if isinstance(val, (int, float)):
                    values.append(val)

            series_data = {
                "metric_type": series.metric.type,
                "labels": labels,
                "label_str": build_series_label(series, group_by),
                "points": points,
                "values": values,
            }

            if values:
                series_data["latest"] = values[0] if points else None
                series_data["max"] = max(values)
                series_data["avg"] = sum(values) / len(values)
                if show_stats:
                    series_data["stats"] = compute_stats(values)

            all_series.append(series_data)

        # Sort by max value (descending) for top-N
        all_series.sort(key=lambda s: s.get("max", 0), reverse=True)

        # Apply top-N filter
        if top_n:
            all_series = all_series[:top_n]

        # Output
        if output_format == "json":
            output_json(all_series, show_stats, latest_only)
        elif output_format == "csv":
            output_csv(all_series, latest_only)
        else:
            output_table(all_series, show_stats, latest_only)

        if output_format == "table":
            print(f"\nTotal: {len(all_series)} time series", file=sys.stderr)

    except Exception as e:
        print(f"Error querying metrics: {e}", file=sys.stderr)
        sys.exit(1)


def output_json(all_series: list, show_stats: bool, latest_only: bool) -> None:
    """Output results as JSON."""
    output = []
    for series in all_series:
        item = {
            "labels": series["labels"],
        }
        if latest_only:
            item["value"] = series.get("latest")
        else:
            item["points"] = series["points"]

        if show_stats and "stats" in series:
            item["stats"] = series["stats"]

        output.append(item)

    print(json.dumps(output, separators=(',', ':'), default=str))


def output_csv(all_series: list, latest_only: bool) -> None:
    """Output results as CSV."""
    output = StringIO()

    if latest_only:
        # Wide format: one row per series
        if not all_series:
            return

        # Collect all label keys
        all_labels = set()
        for s in all_series:
            all_labels.update(s["labels"].keys())
        label_cols = sorted(all_labels)

        writer = csv.writer(output)
        writer.writerow(label_cols + ["value"])

        for series in all_series:
            row = [series["labels"].get(k, "") for k in label_cols]
            row.append(series.get("latest", ""))
            writer.writerow(row)
    else:
        # Long format: one row per data point
        writer = csv.writer(output)
        writer.writerow(["series", "timestamp", "value"])

        for series in all_series:
            label_str = series["label_str"]
            for point in series["points"]:
                writer.writerow([label_str, point["timestamp"], point["value"]])

    print(output.getvalue(), end="")


def format_value(val: float | int | str) -> str:
    """Format a value for human-readable display."""
    if isinstance(val, str):
        return val
    if val == 0:
        return "0"

    abs_val = abs(val)

    # For very small values, use fixed decimal that shows significant digits
    if abs_val < 0.0001:
        return f"{val:.8f}".rstrip('0').rstrip('.')
    elif abs_val < 0.01:
        return f"{val:.6f}".rstrip('0').rstrip('.')
    elif abs_val < 1:
        return f"{val:.4f}".rstrip('0').rstrip('.')
    elif abs_val < 1000:
        return f"{val:.2f}".rstrip('0').rstrip('.')
    else:
        return f"{val:,.0f}"


def output_table(all_series: list, show_stats: bool, latest_only: bool) -> None:
    """Output results as formatted table."""
    if not all_series:
        print("No time series found matching the query.")
        return

    if latest_only or show_stats:
        # Summary view
        print(f"\n{'Series':<60} {'Latest':>14} {'Max':>14} {'Avg':>14}", end="")
        if show_stats:
            print(f" {'P95':>14} {'P99':>14}", end="")
        print()
        print("-" * (60 + 14 * 3 + (28 if show_stats else 0)))

        for series in all_series:
            label = series["label_str"][:58]
            latest = series.get("latest", 0)
            max_val = series.get("max", 0)
            avg_val = series.get("avg", 0)

            print(f"{label:<60} {format_value(latest):>14} {format_value(max_val):>14} {format_value(avg_val):>14}", end="")

            if show_stats and "stats" in series:
                stats = series["stats"]
                print(f" {format_value(stats['p95']):>14} {format_value(stats['p99']):>14}", end="")
            print()
    else:
        # Full data points view
        for i, series in enumerate(all_series, 1):
            print(f"\n[{i}] {series['label_str']}")
            for point in series["points"][:20]:  # Limit to 20 points
                ts = datetime.fromisoformat(point['timestamp']).strftime('%m-%d %H:%M:%S')
                print(f"  {ts}  {format_value(point['value'])}")
            if len(series["points"]) > 20:
                print(f"  ... ({len(series['points']) - 20} more points)")


def describe_metric(project_id: str, metric_type: str) -> None:
    """Describe a metric type and show available labels."""
    client = monitoring_v3.MetricServiceClient()
    project_name = f"projects/{project_id}"

    metric_kind_names = {
        0: "METRIC_KIND_UNSPECIFIED",
        1: "GAUGE",
        2: "DELTA",
        3: "CUMULATIVE",
    }
    value_type_names = {
        0: "VALUE_TYPE_UNSPECIFIED",
        1: "BOOL",
        2: "INT64",
        3: "DOUBLE",
        4: "STRING",
        5: "DISTRIBUTION",
        6: "MONEY",
    }

    try:
        # Get metric descriptor
        descriptor = client.get_metric_descriptor(
            name=f"{project_name}/metricDescriptors/{metric_type}"
        )

        print(f"Metric: {descriptor.type}")
        print(f"Display Name: {descriptor.display_name}")
        print(f"Description: {descriptor.description}")
        print(f"Kind: {metric_kind_names.get(descriptor.metric_kind, str(descriptor.metric_kind))}")
        print(f"Value Type: {value_type_names.get(descriptor.value_type, str(descriptor.value_type))}")
        print(f"Unit: {descriptor.unit or 'none'}")

        print("\nMetric Labels:")
        if descriptor.labels:
            for label in descriptor.labels:
                print(f"  metric.labels.{label.key}")
                if label.description:
                    print(f"    {label.description}")
        else:
            print("  (none)")

        # Get a sample time series to discover resource labels
        print("\nResource Labels (from sample data):")
        now = datetime.now(timezone.utc)
        interval = monitoring_v3.TimeInterval(
            start_time=now - timedelta(minutes=5),
            end_time=now,
        )

        results = client.list_time_series(
            request={
                "name": project_name,
                "filter": f'metric.type = "{metric_type}"',
                "interval": interval,
                "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
            }
        )

        resource_labels = set()
        resource_type = None
        sample_values = {}

        for series in results:
            resource_type = series.resource.type
            for key, value in series.resource.labels.items():
                resource_labels.add(key)
                if key not in sample_values:
                    sample_values[key] = set()
                sample_values[key].add(value)
                if len(sample_values[key]) > 5:
                    sample_values[key] = set(list(sample_values[key])[:5])

        # Discover system metadata labels by trying common ones
        system_labels = set()
        system_sample_values = {}
        common_system_labels = ["node_name", "node_zone", "node_region", "state", "machine_type"]

        for sys_label in common_system_labels:
            try:
                aggregation = monitoring_v3.Aggregation(
                    alignment_period={"seconds": 60},
                    per_series_aligner=monitoring_v3.Aggregation.Aligner.ALIGN_RATE,
                    cross_series_reducer=monitoring_v3.Aggregation.Reducer.REDUCE_SUM,
                    group_by_fields=[f"metadata.system_labels.{sys_label}"],
                )
                sys_results = client.list_time_series(
                    request={
                        "name": project_name,
                        "filter": f'metric.type = "{metric_type}"',
                        "interval": interval,
                        "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
                        "aggregation": aggregation,
                    }
                )
                for series in sys_results:
                    if series.metadata and series.metadata.system_labels:
                        for key, value in series.metadata.system_labels.items():
                            system_labels.add(key)
                            if key not in system_sample_values:
                                system_sample_values[key] = set()
                            system_sample_values[key].add(value)
                            if len(system_sample_values[key]) > 5:
                                system_sample_values[key] = set(list(system_sample_values[key])[:5])
                    break  # Only need one sample per label
            except Exception:
                pass  # Label doesn't exist for this metric

        if resource_type:
            print(f"  Resource Type: {resource_type}")
        for label in sorted(resource_labels):
            samples = list(sample_values.get(label, []))[:3]
            sample_str = f" (e.g., {', '.join(repr(s) for s in samples)})" if samples else ""
            print(f"  resource.labels.{label}{sample_str}")

        if system_labels:
            print("\nSystem Metadata Labels:")
            for label in sorted(system_labels):
                samples = list(system_sample_values.get(label, []))[:3]
                sample_str = f" (e.g., {', '.join(repr(s) for s in samples)})" if samples else ""
                print(f"  metadata.system_labels.{label}{sample_str}")

    except Exception as e:
        print(f"Error describing metric: {e}")
        sys.exit(1)


def list_metrics(project_id: str, filter_prefix: Optional[str] = None) -> None:
    """List available metric types in the project."""
    client = monitoring_v3.MetricServiceClient()
    project_name = f"projects/{project_id}"

    metric_kind_names = {
        0: "METRIC_KIND_UNSPECIFIED",
        1: "GAUGE",
        2: "DELTA",
        3: "CUMULATIVE",
    }
    value_type_names = {
        0: "VALUE_TYPE_UNSPECIFIED",
        1: "BOOL",
        2: "INT64",
        3: "DOUBLE",
        4: "STRING",
        5: "DISTRIBUTION",
        6: "MONEY",
    }

    print(f"Listing metrics for project: {project_id}")
    if filter_prefix:
        print(f"Filtering by prefix: {filter_prefix}")
    print("-" * 60)

    try:
        metrics = client.list_metric_descriptors(name=project_name)
        count = 0
        for descriptor in metrics:
            if filter_prefix and not descriptor.type.startswith(filter_prefix):
                continue
            count += 1
            kind = metric_kind_names.get(descriptor.metric_kind, str(descriptor.metric_kind))
            value = value_type_names.get(descriptor.value_type, str(descriptor.value_type))
            print(f"{descriptor.type}  [{kind}/{value}]  {descriptor.display_name}")

        print(f"Total: {count} metrics")
    except Exception as e:
        print(f"Error listing metrics: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Query Google Cloud Monitoring metrics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Query CPU usage (last hour, default)
  %(prog)s query -p my-project -m kubernetes.io/container/cpu/core_usage_time

  # Find top 10 CPU consumers with stats
  %(prog)s query -p my-project -m kubernetes.io/container/cpu/core_usage_time \\
    --top 10 --stats

  # Get latest values only (good for dashboards)
  %(prog)s query -p my-project -m kubernetes.io/container/cpu/core_usage_time \\
    --latest --top 20

  # Aggregate CPU across all containers per namespace
  %(prog)s query -p my-project -m kubernetes.io/container/cpu/core_usage_time \\
    --reducer REDUCE_SUM --group-by namespace_name

  # Output as JSON for further processing
  %(prog)s query -p my-project -m kubernetes.io/container/cpu/core_usage_time \\
    --output json --stats | jq '.[] | select(.stats.max > 0.5)'

  # Output as CSV
  %(prog)s query -p my-project -m kubernetes.io/container/cpu/core_usage_time \\
    --output csv --latest > cpu_usage.csv

  # Query specific time range
  %(prog)s query -p my-project -m kubernetes.io/container/cpu/core_usage_time \\
    --start 2025-12-28T10:00:00Z --end 2025-12-28T12:00:00Z

  # Describe a metric (show available labels for filtering)
  %(prog)s describe -p my-project -m kubernetes.io/container/cpu/core_usage_time

  # List kubernetes metrics
  %(prog)s list -p my-project --prefix kubernetes.io
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Query command
    query_parser = subparsers.add_parser("query", help="Query metrics")
    query_parser.add_argument(
        "-p", "--project", required=True,
        help="GCP project ID",
    )
    query_parser.add_argument(
        "-m", "--metric", required=True,
        help="Metric type (e.g., kubernetes.io/container/cpu/core_usage_time)",
    )
    query_parser.add_argument(
        "-f", "--filter", action="append", dest="filters",
        help="Filter expression (can be specified multiple times)",
    )

    # Time options
    time_group = query_parser.add_argument_group("time options")
    time_group.add_argument(
        "-d", "--duration", default=None,
        help="Time range (e.g., 30m, 1h, 7d). Default: 1h",
    )
    time_group.add_argument(
        "--start",
        help="Start time in ISO format",
    )
    time_group.add_argument(
        "--end",
        help="End time in ISO format (defaults to now)",
    )

    # Aggregation options
    agg_group = query_parser.add_argument_group("aggregation options")
    agg_group.add_argument(
        "--align-period", type=int, default=60,
        help="Alignment period in seconds. Default: 60",
    )
    agg_group.add_argument(
        "--aligner", default="ALIGN_RATE",
        help="Per-series aligner: ALIGN_RATE, ALIGN_MEAN, ALIGN_SUM, ALIGN_MAX, ALIGN_MIN, ALIGN_DELTA, ALIGN_NONE",
    )
    agg_group.add_argument(
        "--reducer",
        help="Cross-series reducer: REDUCE_SUM, REDUCE_MEAN, REDUCE_MAX, REDUCE_MIN, REDUCE_COUNT, REDUCE_PERCENTILE_99/95/50",
    )
    agg_group.add_argument(
        "--group-by", action="append",
        help="Group by label when using reducer (e.g., namespace_name, container_name)",
    )

    # Output options
    out_group = query_parser.add_argument_group("output options")
    out_group.add_argument(
        "-o", "--output", choices=["table", "json", "csv"], default="table",
        help="Output format. Default: table",
    )
    out_group.add_argument(
        "--top", type=int, dest="top_n",
        help="Show only top N series by max value",
    )
    out_group.add_argument(
        "--stats", action="store_true",
        help="Show statistics (min, max, avg, p50, p95, p99)",
    )
    out_group.add_argument(
        "--latest", action="store_true",
        help="Show only the latest value per series",
    )

    # Describe command
    describe_parser = subparsers.add_parser("describe", help="Describe a metric and show available labels")
    describe_parser.add_argument(
        "-p", "--project", required=True,
        help="GCP project ID",
    )
    describe_parser.add_argument(
        "-m", "--metric", required=True,
        help="Metric type to describe",
    )

    # List command
    list_parser = subparsers.add_parser("list", help="List available metrics")
    list_parser.add_argument(
        "-p", "--project", required=True,
        help="GCP project ID",
    )
    list_parser.add_argument(
        "--prefix",
        help="Filter metrics by type prefix",
    )

    args = parser.parse_args()

    if args.command == "query":
        query_metrics(
            project_id=args.project,
            metric_type=args.metric,
            filters=args.filters,
            duration=args.duration,
            start=args.start,
            end=args.end,
            alignment_period=args.align_period,
            per_series_aligner=args.aligner,
            reducer=args.reducer,
            group_by=args.group_by,
            output_format=args.output,
            top_n=args.top_n,
            show_stats=args.stats,
            latest_only=args.latest,
        )
    elif args.command == "describe":
        describe_metric(
            project_id=args.project,
            metric_type=args.metric,
        )
    elif args.command == "list":
        list_metrics(
            project_id=args.project,
            filter_prefix=args.prefix,
        )
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
