import argparse
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Extract workout-related data from an Apple Health export XML file "
            "without loading the full file into memory."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("input_path", help="Path to export.xml from Apple Health.")
    parser.add_argument(
        "output_path",
        nargs="?",
        help="Path for the filtered XML output. Defaults next to the input file.",
    )
    parser.add_argument(
        "--types",
        nargs="*",
        default=[],
        help=(
            "Optional Record types to include even if they do not overlap a workout. "
            "Example: HKQuantityTypeIdentifierStepCount"
        ),
    )
    parser.add_argument(
        "--include-activity-summaries",
        action="store_true",
        help="Include ActivitySummary elements in the filtered output.",
    )
    return parser.parse_args()


def strip_tag(tag):
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def quote_attr(value):
    return (
        str(value)
        .replace("&", "&amp;")
        .replace('"', "&quot;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def clear_element(element):
    element.clear()


def merge_intervals(intervals):
    if not intervals:
        return []

    intervals.sort()
    merged = [intervals[0]]

    for start, end in intervals[1:]:
        last_start, last_end = merged[-1]
        if start <= last_end:
            merged[-1] = (last_start, max(last_end, end))
        else:
            merged.append((start, end))

    return merged


def overlaps(intervals, start, end):
    if start is None or end is None:
        return False

    for interval_start, interval_end in intervals:
        if interval_end < start:
            continue
        if interval_start > end:
            return False
        return True
    return False


def write_element(output_handle, element, level=1):
    indent = "  " * level
    serialized = ET.tostring(element, encoding="unicode")
    lines = serialized.splitlines() or [serialized]
    for line in lines:
        output_handle.write(f"{indent}{line}\n")


def resolve_output_path(input_path, output_path):
    if output_path:
        return Path(output_path)

    input_file = Path(input_path)
    if input_file.suffix:
        return input_file.with_name(f"{input_file.stem}_workouts_only.xml")
    return input_file.with_name(f"{input_file.name}_workouts_only.xml")


def collect_workout_intervals(input_path):
    intervals = []
    workout_count = 0

    context = ET.iterparse(input_path, events=("end",))
    for _, element in context:
        if strip_tag(element.tag) != "Workout":
            continue

        start = element.attrib.get("startDate")
        end = element.attrib.get("endDate")
        if start and end:
            intervals.append((start, end))
            workout_count += 1
        clear_element(element)

    return merge_intervals(intervals), workout_count


def write_filtered_export(
    input_path,
    output_path,
    workout_intervals,
    record_types,
    include_activity_summaries,
):
    total_written = 0
    written_counts = {
        "Workout": 0,
        "Record": 0,
        "Correlation": 0,
        "ActivitySummary": 0,
    }

    context = ET.iterparse(input_path, events=("start", "end"))
    root_started = False
    output_handle = None
    tag_stack = []

    for event, element in context:
        tag = strip_tag(element.tag)

        if event == "start" and tag == "HealthData" and not root_started:
            output_handle = open(output_path, "w", encoding="utf-8")
            output_handle.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            output_handle.write("<HealthData")
            for key, value in element.attrib.items():
                output_handle.write(f' {key}="{quote_attr(value)}"')
            output_handle.write(">\n")
            root_started = True
        if event == "start":
            tag_stack.append(tag)
            continue

        is_top_level_child = len(tag_stack) == 2 and tag_stack[0] == "HealthData"

        if tag in {"ExportDate", "Me"} and is_top_level_child:
            write_element(output_handle, element)
            clear_element(element)
        elif tag == "Workout" and is_top_level_child:
            write_element(output_handle, element)
            written_counts["Workout"] += 1
            total_written += 1
            clear_element(element)
        elif tag == "Record" and is_top_level_child:
            keep = (
                element.attrib.get("type") in record_types
                or overlaps(
                    workout_intervals,
                    element.attrib.get("startDate"),
                    element.attrib.get("endDate"),
                )
            )
            if keep:
                write_element(output_handle, element)
                written_counts["Record"] += 1
                total_written += 1
            clear_element(element)
        elif tag == "Correlation" and is_top_level_child:
            keep = overlaps(
                workout_intervals,
                element.attrib.get("startDate"),
                element.attrib.get("endDate"),
            )
            if keep:
                write_element(output_handle, element)
                written_counts["Correlation"] += 1
                total_written += 1
            clear_element(element)
        elif tag == "ActivitySummary" and is_top_level_child:
            if include_activity_summaries:
                write_element(output_handle, element)
                written_counts["ActivitySummary"] += 1
                total_written += 1
            clear_element(element)
        elif is_top_level_child and tag in {
            "ClinicalRecord",
            "Audiogram",
            "VisionPrescription",
        }:
            clear_element(element)

        tag_stack.pop()

    if output_handle is None:
        raise ValueError("Could not find a HealthData root element in the input XML.")

    output_handle.write("</HealthData>\n")
    output_handle.close()

    return written_counts, total_written


def main():
    args = parse_args()
    output_path = resolve_output_path(args.input_path, args.output_path)

    workout_intervals, workout_count = collect_workout_intervals(args.input_path)
    if workout_count == 0:
        print("No Workout elements found in the input file.", file=sys.stderr)
        sys.exit(1)

    written_counts, total_written = write_filtered_export(
        args.input_path,
        output_path,
        workout_intervals,
        set(args.types),
        args.include_activity_summaries,
    )

    print(f"Found {workout_count} workouts across {len(workout_intervals)} merged windows.")
    print(f"Wrote {total_written} workout-related elements to {output_path}")
    print(
        "Breakdown: "
        f"{written_counts['Workout']} workouts, "
        f"{written_counts['Record']} records, "
        f"{written_counts['Correlation']} correlations, "
        f"{written_counts['ActivitySummary']} activity summaries"
    )


if __name__ == "__main__":
    main()
