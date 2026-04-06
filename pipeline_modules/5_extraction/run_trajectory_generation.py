#!/usr/bin/env python3
"""
Generate patient trajectory markdown files for all patients.

This module creates human-readable markdown summaries of patient medical histories,
organized chronologically by visits and including all medical events.
Output files are saved as patient_trajectory.md in each patient's folder.
"""
from __future__ import annotations

import csv
import json
import logging
import multiprocessing as mp
import platform
import time
import warnings
from collections import defaultdict, deque
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from tqdm import tqdm

# Platform-specific settings for performance optimization
if platform.system() == 'Windows':
    PROGRESS_INTERVAL = 30.0  # Less frequent updates
else:
    PROGRESS_INTERVAL = 10.0  # Default for macOS/Linux

# Suppress warnings
warnings.filterwarnings('ignore')

# Setup paths
base_dir = Path(__file__).resolve().parent
project_root = base_dir.parent.parent  # Go up to crisp_pipeline_public

# Input/output directories
patient_data_dir = (project_root / "extracted_patient_data").resolve()
mapping_dir = (project_root / "mapping_resources").resolve()
output_dir = (project_root / "output" / "5_extraction").resolve()

# Create output directory
output_dir.mkdir(parents=True, exist_ok=True)

# Parallel processing configuration
MAX_WORKERS = max(1, mp.cpu_count() - 2)  # Reserve 2 cores for system


@dataclass
class Event:
    """Represents a single medical event."""
    timestamp: Optional[datetime]
    description: str
    visit_occurrence_id: Optional[str]
    visit_detail_id: Optional[str]


@dataclass
class VisitSection:
    """Represents a visit period with associated events."""
    index: int
    label: str
    start: Optional[datetime]
    end: Optional[datetime]
    events: List[Event]


# ============================================================================
# Utility Functions
# ============================================================================

def safe_int(value: Optional[str]) -> Optional[int]:
    """Safely convert a string to int, handling NaN and empty values."""
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def safe_float(value: Optional[str]) -> Optional[float]:
    """Safely convert a string to float, handling NaN and empty values."""
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_datetime(value: Optional[str]) -> Optional[datetime]:
    """Parse datetime string with multiple format support."""
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    normalized = text.replace("T", " ")
    # Attempt full timestamp first, then fall back to date-only
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    # Let datetime.fromisoformat try a final parse
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def format_datetime(dt: Optional[datetime]) -> str:
    """Format datetime to string."""
    if not dt:
        return "NA"
    return dt.strftime("%Y-%m-%d %H:%M")


def normalize_id(value: Optional[str]) -> Optional[str]:
    """Normalize ID values, removing .0 suffixes."""
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    if text.endswith(".0"):
        text = text[:-2]
    return text


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    """Read CSV file and return list of row dictionaries."""
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        return [row for row in reader]


def first_timestamp(row: Dict[str, str], columns: Sequence[str]) -> Optional[datetime]:
    """Get first non-null timestamp from a list of columns."""
    for col in columns:
        ts = parse_datetime(row.get(col))
        if ts:
            return ts
    return None


def format_parts(parts: List[Optional[str]]) -> Optional[str]:
    """Join non-empty parts with semicolon."""
    cleaned = [part for part in parts if part and str(part).strip()]
    return "; ".join(cleaned) if cleaned else None


# ============================================================================
# Concept Mapping Functions
# ============================================================================

def load_mapping_csv(path: Path) -> Dict[int, str]:
    """Load concept ID to name mapping from CSV."""
    mapping: Dict[int, str] = {}
    if not path.exists():
        return mapping
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            cid = safe_int(row.get("Id"))
            name = (row.get("Name") or "").strip()
            if cid is not None and name:
                mapping[cid] = name
    return mapping


def load_mappings(mapping_root: Path) -> Dict[str, Dict[int, str]]:
    """Load all concept mappings."""
    return {
        "visit": load_mapping_csv(
            mapping_root / "VISIT_OCCURRENCE_visit_concept_mapping.csv"
        ),
        "visit_detail": load_mapping_csv(
            mapping_root / "VISIT_DETAIL_visit_detail_concept_mapping.csv"
        ),
        "condition": load_mapping_csv(
            mapping_root / "CONDITION_OCCURRENCE_condition_concept_mapping.csv"
        ),
        "condition_era": load_mapping_csv(
            mapping_root / "CONDITION_ERA_condition_concept_mapping.csv"
        ),
        "drug": load_mapping_csv(
            mapping_root / "DRUG_EXPOSURE_drug_concept_mapping.csv"
        ),
        "drug_era": load_mapping_csv(
            mapping_root / "DRUG_ERA_drug_concept_mapping.csv"
        ),
        "measurement": load_mapping_csv(
            mapping_root / "MEASUREMENT_concept_mapping.csv"
        ),
        "observation": load_mapping_csv(
            mapping_root / "OBSERVATION_concept_mapping.csv"
        ),
        "procedure": load_mapping_csv(
            mapping_root / "PROCEDURE_OCCURRENCE_concept_mapping.csv"
        ),
        "device": load_mapping_csv(
            mapping_root / "DEVICE_EXPOSURE_concept_mapping.csv"
        ),
        "specimen": load_mapping_csv(
            mapping_root / "SPECIMEN_specimen_concept_mapping.csv"
        ),
        "gender": load_mapping_csv(
            mapping_root / "PERSON_gender_concept_mapping.csv"
        ),
        "race": load_mapping_csv(
            mapping_root / "PERSON_race_concept_mapping.csv"
        ),
        "ethnicity": load_mapping_csv(
            mapping_root / "PERSON_ethnicity_concept_mapping.csv"
        ),
    }


def concept_name(concept_id: Optional[int], mapping: Dict[int, str], fallback: str) -> str:
    """Get concept name from mapping or return fallback."""
    if concept_id is None:
        return f"Unknown {fallback}"
    return mapping.get(concept_id, f"{fallback} concept {concept_id}")


# ============================================================================
# Patient Path Functions
# ============================================================================

def get_patient_path(person_id: str) -> Path:
    """Generate patient data storage path with hierarchical structure."""
    prefix = person_id[:9]

    if prefix == "600000071":
        # Special handling for 600000071 prefix
        suffix_int = int(person_id[-6:])
        group_start = (suffix_int // 1000) * 1000
        group_end = group_start + 999
        group_folder = f"{group_start:06d}-{group_end:06d}"
        return patient_data_dir / prefix / group_folder / person_id
    else:
        # Standard two-level structure
        return patient_data_dir / prefix / person_id


def find_all_patients() -> List[Path]:
    """Find all patient directories with PERSON.csv."""
    patients: List[Path] = []
    if not patient_data_dir.exists():
        return patients

    for site_dir in sorted(patient_data_dir.iterdir()):
        if not site_dir.is_dir():
            continue
        for item in sorted(site_dir.iterdir()):
            if item.is_dir():
                # Check for PERSON.csv directly or in subdirectories (for 600000071 special case)
                if (item / "PERSON.csv").exists():
                    patients.append(item)
                else:
                    # Check subdirectories (for special 600000071 structure)
                    for subitem in item.iterdir():
                        if subitem.is_dir() and (subitem / "PERSON.csv").exists():
                            patients.append(subitem)

    return patients


# ============================================================================
# Patient Data Processing
# ============================================================================

def summarize_person(patient_dir: Path, mappings: Dict[str, Dict[int, str]]) -> Dict[str, str]:
    """Extract patient demographic information."""
    info = {
        "gender": "Unknown gender",
        "race": "Unknown race",
        "ethnicity": "Unknown ethnicity",
        "birth_year": "Unknown",
        "birthdate": "Unknown",
    }
    person_rows = read_csv_rows(patient_dir / "PERSON.csv")
    if not person_rows:
        return info
    row = person_rows[0]
    info["gender"] = concept_name(
        safe_int(row.get("gender_concept_id")), mappings.get("gender", {}), "gender"
    )
    info["race"] = concept_name(
        safe_int(row.get("race_concept_id")), mappings.get("race", {}), "race"
    )
    info["ethnicity"] = concept_name(
        safe_int(row.get("ethnicity_concept_id")),
        mappings.get("ethnicity", {}),
        "ethnicity",
    )
    birth_year = safe_int(row.get("year_of_birth"))
    if birth_year is not None:
        info["birth_year"] = str(birth_year)
        month = safe_int(row.get("month_of_birth")) or 1
        day = safe_int(row.get("day_of_birth")) or 1
        info["birthdate"] = f"{birth_year:04d}-{month:02d}-{day:02d}"
    birth_datetime = row.get("birth_datetime")
    if birth_datetime and birth_datetime.strip():
        info["birthdate"] = birth_datetime.split(" ")[0]
    return info


def measurement_value_string(row: Dict[str, str]) -> Optional[str]:
    """Extract measurement value with unit."""
    value_num = row.get("value_as_number")
    if value_num is None or str(value_num).strip() == "":
        return None
    unit = row.get("unit_source_value")
    parts = [f"value {value_num}"]
    if unit:
        parts.append(f"unit {unit}")
    return "; ".join(parts)


def collect_events_for_patient(
    patient_dir: Path, mappings: Dict[str, Dict[int, str]]
) -> Tuple[List[Event], List[str], List[Dict[str, str]]]:
    """Collect all medical events for a patient."""
    events: List[Event] = []
    no_timestamp: List[str] = []

    def add_event(
        ts: Optional[datetime],
        concept: str,
        value: Optional[str],
        visit_occurrence_id: Optional[str],
        visit_detail_id: Optional[str],
    ) -> None:
        value_text = str(value).strip() if value is not None else None
        if not value_text:
            text = f"{concept} (No Value)"
        elif value_text.startswith("end time:"):
            text = f"{concept} (No Value) ({value_text})"
        else:
            text = f"{concept}, {value_text}"
        if ts:
            events.append(Event(ts, text, visit_occurrence_id, visit_detail_id))
        else:
            no_timestamp.append(text)

    def process_table(
        rows: List[Dict[str, str]],
        timestamp_cols: Sequence[str],
        concept_col: str,
        mapping_key: str,
        fallback_label: str,
        value_fn,
    ) -> None:
        mapping = mappings.get(mapping_key, {})
        for row in rows:
            ts = first_timestamp(row, timestamp_cols)
            concept = concept_name(safe_int(row.get(concept_col)), mapping, fallback_label)
            value = value_fn(row)
            visit_occurrence_id = normalize_id(row.get("visit_occurrence_id"))
            visit_detail_id = normalize_id(row.get("visit_detail_id"))
            add_event(ts, concept, value, visit_occurrence_id, visit_detail_id)

    # Read all relevant tables
    visits = read_csv_rows(patient_dir / "VISIT_OCCURRENCE.csv")
    visit_details = read_csv_rows(patient_dir / "VISIT_DETAIL.csv")
    conditions = read_csv_rows(patient_dir / "CONDITION_OCCURRENCE.csv")
    condition_eras = read_csv_rows(patient_dir / "CONDITION_ERA.csv")
    drugs = read_csv_rows(patient_dir / "DRUG_EXPOSURE.csv")
    drug_eras = read_csv_rows(patient_dir / "DRUG_ERA.csv")
    measurements = read_csv_rows(patient_dir / "MEASUREMENT.csv")
    observations = read_csv_rows(patient_dir / "OBSERVATION.csv")
    procedures = read_csv_rows(patient_dir / "PROCEDURE_OCCURRENCE.csv")
    devices = read_csv_rows(patient_dir / "DEVICE_EXPOSURE.csv")
    specimens = read_csv_rows(patient_dir / "SPECIMEN.csv")
    icu_episodes = read_csv_rows(patient_dir / "icu_episodes.csv")

    # Process visits
    process_table(
        visits,
        ("visit_start_datetime", "visit_start_date"),
        "visit_concept_id",
        "visit",
        "visit",
        lambda row: format_parts(
            [
                f"ended {row.get('visit_end_datetime') or row.get('visit_end_date')}"
                if row.get("visit_end_datetime") or row.get("visit_end_date")
                else None,
                f"care site {row.get('care_site_id')}"
                if row.get("care_site_id")
                else None,
            ]
        ),
    )

    # Process visit details
    process_table(
        visit_details,
        ("visit_detail_start_datetime", "visit_detail_start_date"),
        "visit_detail_concept_id",
        "visit_detail",
        "visit detail",
        lambda row: format_parts(
            [
                f"ended {row.get('visit_detail_end_datetime') or row.get('visit_detail_end_date')}"
                if row.get("visit_detail_end_datetime")
                or row.get("visit_detail_end_date")
                else None,
            ]
        ),
    )

    # Process conditions (diagnoses don't have a numeric value — the concept name IS the value)
    process_table(
        conditions,
        ("condition_start_datetime", "condition_start_date"),
        "condition_concept_id",
        "condition",
        "condition",
        lambda row: "",
    )

    # Process condition eras
    process_table(
        condition_eras,
        ("condition_era_start_date",),
        "condition_concept_id",
        "condition_era",
        "condition",
        lambda row: format_parts(
            [
                f"condition occurrence count: {row.get('condition_occurrence_count')}"
                if row.get("condition_occurrence_count")
                else None,
                f"end time: {row.get('condition_era_end_date')}"
                if row.get("condition_era_end_date")
                else None,
            ]
        ),
    )

    # Process drugs
    process_table(
        drugs,
        ("drug_exposure_start_datetime", "drug_exposure_start_date"),
        "drug_concept_id",
        "drug",
        "drug",
        lambda row: format_parts(
            [
                f"quantity {row.get('quantity')}" if row.get("quantity") else None,
                f"days supplied {row.get('days_supply')}"
                if row.get("days_supply")
                else None,
            ]
        ),
    )

    # Process drug eras
    process_table(
        drug_eras,
        ("drug_era_start_date",),
        "drug_concept_id",
        "drug_era",
        "drug",
        lambda row: format_parts(
            [
                f"drug exposure count: {row.get('drug_exposure_count')}"
                if row.get("drug_exposure_count")
                else None,
                f"end time: {row.get('drug_era_end_date')}"
                if row.get("drug_era_end_date")
                else None,
            ]
        ),
    )

    # Process measurements
    process_table(
        measurements,
        ("measurement_datetime", "measurement_date"),
        "measurement_concept_id",
        "measurement",
        "measurement",
        lambda row: measurement_value_string(row),
    )

    # Process observations (include both numeric and non-numeric observations)
    process_table(
        observations,
        ("observation_datetime", "observation_date"),
        "observation_concept_id",
        "observation",
        "observation",
        lambda row: (
            f"value {row.get('value_as_number')}"
            if row.get("value_as_number")
            and str(row.get("value_as_number")).strip() != ""
            else ""
        ),
    )

    # Process procedures
    process_table(
        procedures,
        ("procedure_datetime", "procedure_date"),
        "procedure_concept_id",
        "procedure",
        "procedure",
        lambda row: (
            f"quantity {row.get('quantity')}" if row.get("quantity") else ""
        ),
    )

    # Process devices
    process_table(
        devices,
        ("device_exposure_start_datetime", "device_exposure_start_date"),
        "device_concept_id",
        "device",
        "device",
        lambda row: (
            f"quantity {row.get('quantity')}" if row.get("quantity") else ""
        ),
    )

    # Process specimens
    process_table(
        specimens,
        ("specimen_datetime", "specimen_date"),
        "specimen_concept_id",
        "specimen",
        "specimen",
        lambda row: (
            f"quantity {row.get('quantity')}" if row.get("quantity") else ""
        ),
    )

    # Process ICU episodes
    for row in icu_episodes:
        start_dt = parse_datetime(row.get("episode_start"))
        concept = f"ICU episode #{row.get('episode_number')}"
        value = format_parts(
            [
                f"ended {row.get('episode_end')}" if row.get("episode_end") else None,
                f"duration {row.get('duration_hours')} hours"
                if row.get("duration_hours")
                else None,
                "first ICU stay"
                if row.get("is_first_icu") in ("True", "true", "1")
                else None,
            ]
        )
        add_event(
            start_dt,
            concept,
            value,
            visit_occurrence_id=None,
            visit_detail_id=None,
        )

    events.sort(key=lambda event: event.timestamp or datetime.min)
    return events, no_timestamp, visit_details


def organize_events_by_visit_details(
    events: List[Event],
    visit_details: List[Dict[str, str]],
    mappings: Dict[str, Dict[int, str]],
) -> Tuple[List[VisitSection], List[Event]]:
    """Organize events into visit sections."""
    if not visit_details:
        return [], events

    detail_mapping = mappings.get("visit_detail", {})
    sections: List[VisitSection] = []
    detail_map: Dict[str, VisitSection] = {}
    visits_to_details: Dict[str, List[VisitSection]] = {}

    sorted_details = sorted(
        visit_details,
        key=lambda row: first_timestamp(
            row, ("visit_detail_start_datetime", "visit_detail_start_date")
        )
        or datetime.min,
    )
    section_index = 1
    for row in sorted_details:
        visit_detail_id = normalize_id(row.get("visit_detail_id"))
        visit_occurrence_id = normalize_id(row.get("visit_occurrence_id"))
        start_dt = first_timestamp(
            row, ("visit_detail_start_datetime", "visit_detail_start_date")
        )
        end_dt = first_timestamp(
            row, ("visit_detail_end_datetime", "visit_detail_end_date")
        )
        concept = concept_name(
            safe_int(row.get("visit_detail_concept_id")),
            detail_mapping,
            "visit detail",
        )

        if start_dt and end_dt and start_dt == end_dt:
            checkpoint_description = (
                f"{concept} detail checkpoint "
                f"[{format_datetime(start_dt)} -> {format_datetime(end_dt)}; zero-duration]"
            )
            events.append(
                Event(
                    start_dt,
                    checkpoint_description,
                    visit_occurrence_id,
                    None,
                )
            )
            continue

        label = (
            f"{format_datetime(start_dt)} to {format_datetime(end_dt)} — {concept}"
        )
        section = VisitSection(
            index=section_index,
            label=label,
            start=start_dt,
            end=end_dt,
            events=[],
        )
        section_index += 1
        sections.append(section)
        if visit_detail_id:
            detail_map[visit_detail_id] = section
        if visit_occurrence_id:
            visits_to_details.setdefault(visit_occurrence_id, []).append(section)

    overflow: List[Event] = []
    for event in events:
        assigned = False
        if event.visit_detail_id and event.visit_detail_id in detail_map:
            detail_map[event.visit_detail_id].events.append(event)
            assigned = True
        elif event.visit_occurrence_id and event.visit_occurrence_id in visits_to_details:
            candidate_sections = visits_to_details[event.visit_occurrence_id]
            if event.timestamp:
                for section in candidate_sections:
                    if (
                        section.start
                        and section.end
                        and section.start <= event.timestamp <= section.end
                    ):
                        section.events.append(event)
                        assigned = True
                        break
            if not assigned and candidate_sections:
                candidate_sections[0].events.append(event)
                assigned = True
        if not assigned:
            overflow.append(event)

    for section in sections:
        section.events.sort(key=lambda e: e.timestamp or datetime.min)
    overflow.sort(key=lambda e: e.timestamp or datetime.min)

    return sections, overflow


# ============================================================================
# Markdown Generation
# ============================================================================

def render_patient_trajectory(
    patient_id: str,
    person_info: Dict[str, str],
    visit_sections: List[VisitSection],
    overflow_events: List[Event],
    no_timestamp: List[str],
) -> str:
    """Render complete patient trajectory as markdown."""
    lines = [
        f"# Patient {patient_id} Trajectory",
        "",
        f"Generated on {format_datetime(datetime.now())}",
        "",
        "## Demographics",
        f"- Gender: {person_info['gender']}",
        f"- Race: {person_info['race']}",
        f"- Ethnicity: {person_info['ethnicity']}",
        f"- Birth year: {person_info['birth_year']}",
        f"- Birthdate: {person_info['birthdate']}",
        "",
    ]

    # Add visit sections
    if visit_sections:
        lines.append("## Medical History by Visits")
        lines.append("")
        visit_sections = sorted(
            visit_sections, key=lambda s: s.start or datetime.min
        )
        overflow_queue = deque(
            sorted(overflow_events, key=lambda e: e.timestamp or datetime.min)
        )

        def emit_extra(label: str, cutoff: Optional[datetime]) -> None:
            block: List[Event] = []
            while overflow_queue and (
                cutoff is None
                or (overflow_queue[0].timestamp or datetime.max) <= cutoff
            ):
                block.append(overflow_queue.popleft())
            if block:
                lines.append(f"#### Additional events {label}")
                for event in block:
                    prefix = (
                        format_datetime(event.timestamp)
                        if event.timestamp
                        else "No timestamp"
                    )
                    lines.append(f"- {prefix}: {event.description}")
                lines.append("")

        emit_extra("before first visit", visit_sections[0].start)

        for idx, section in enumerate(visit_sections):
            lines.append(f"#### Visit {section.index}: {section.label}")
            if section.events:
                for event in section.events:
                    prefix = (
                        format_datetime(event.timestamp)
                        if event.timestamp
                        else "No timestamp"
                    )
                    lines.append(f"- {prefix}: {event.description}")
            else:
                lines.append("- No events recorded for this visit.")
            lines.append("")

            next_start = (
                visit_sections[idx + 1].start
                if idx + 1 < len(visit_sections)
                else None
            )
            label = (
                f"between Visit {section.index} and Visit "
                f"{visit_sections[idx + 1].index}"
                if next_start
                else f"after Visit {section.index}"
            )
            emit_extra(label, next_start)
    else:
        lines.append("## Chronological Events")
        lines.append("")
        combined = sorted(
            overflow_events, key=lambda e: e.timestamp or datetime.min
        )
        if combined:
            for event in combined:
                prefix = (
                    format_datetime(event.timestamp)
                    if event.timestamp
                    else "No timestamp"
                )
                lines.append(f"- {prefix}: {event.description}")
        else:
            lines.append("No timestamped events available.")
        lines.append("")

    lines.append("## Events without timestamp")
    if no_timestamp:
        for entry in no_timestamp:
            lines.append(f"- {entry}")
    else:
        lines.append("- None")
    lines.append("")

    return "\n".join(lines)


# ============================================================================
# Parallel Processing Worker
# ============================================================================

def _init_trajectory_worker(shared_mappings):
    """Initializer for trajectory worker processes — loads mappings once per worker."""
    global _WORKER_MAPPINGS
    _WORKER_MAPPINGS = shared_mappings


def process_patient_worker(patient_dir: Path) -> Tuple[str, bool, Optional[str]]:
    """Worker function to process a single patient (for parallel execution).
    
    Uses global _WORKER_MAPPINGS set by _init_trajectory_worker to avoid
    pickling mappings for every task submission (which would be 500K+ copies).
    """
    mappings = _WORKER_MAPPINGS
    patient_id = patient_dir.name

    try:
        # Collect events
        events, no_ts, visit_details = collect_events_for_patient(patient_dir, mappings)

        # Organize by visits
        visit_sections, overflow_events = organize_events_by_visit_details(
            events, visit_details, mappings
        )

        # Get demographics
        person_info = summarize_person(patient_dir, mappings)

        # Generate markdown
        markdown_content = render_patient_trajectory(
            patient_id,
            person_info,
            visit_sections,
            overflow_events,
            no_ts,
        )

        # Write to patient folder
        output_path = patient_dir / "patient_trajectory.md"
        output_path.write_text(markdown_content, encoding="utf-8")

        return patient_id, True, None

    except Exception as e:
        return patient_id, False, str(e)


# ============================================================================
# Main Execution
# ============================================================================

class TrajectoryGenerator:
    """Main class for generating patient trajectories."""

    def __init__(self):
        self.mappings = None
        self.stats = {
            "total_patients": 0,
            "successful": 0,
            "failed": 0,
            "errors": []
        }

    def load_concept_mappings(self):
        """Load all concept mappings."""
        logging.info("Loading concept mappings...")
        self.mappings = load_mappings(mapping_dir)
        logging.info(f"Loaded {len(self.mappings)} concept mapping tables")

    def generate_all_trajectories(self):
        """Generate trajectories for all patients using parallel processing."""
        logging.info("Finding all patient directories...")
        patient_dirs = find_all_patients()
        self.stats["total_patients"] = len(patient_dirs)

        if not patient_dirs:
            logging.error(f"No patient directories found in {patient_data_dir}")
            return

        logging.info(f"Found {len(patient_dirs)} patients")
        logging.info(f"Generating trajectories using {MAX_WORKERS} workers...")

        # Process in parallel — mappings are shared via initializer (once per worker),
        # NOT pickled per-task (which would be 500K+ copies and cause OOM)
        start_time = time.time()
        with ProcessPoolExecutor(
            max_workers=MAX_WORKERS,
            initializer=_init_trajectory_worker,
            initargs=(self.mappings,)
        ) as executor:
            futures = {executor.submit(process_patient_worker, patient_dir): patient_dir
                      for patient_dir in patient_dirs}

            # Progress bar
            with tqdm(total=len(patient_dirs), desc="Generating trajectories",
                     mininterval=PROGRESS_INTERVAL) as pbar:
                for future in as_completed(futures):
                    patient_id, success, error = future.result()

                    if success:
                        self.stats["successful"] += 1
                    else:
                        self.stats["failed"] += 1
                        self.stats["errors"].append({
                            "patient_id": patient_id,
                            "error": error
                        })
                        logging.error(f"Failed to process {patient_id}: {error}")

                    pbar.update(1)

        elapsed_time = time.time() - start_time
        logging.info(f"Trajectory generation completed in {elapsed_time:.2f} seconds")
        logging.info(f"Successful: {self.stats['successful']}, Failed: {self.stats['failed']}")

    def save_report(self):
        """Save generation statistics report."""
        report_path = output_dir / "trajectory_generation_report.txt"

        with open(report_path, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("PATIENT TRAJECTORY GENERATION REPORT\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Generated: {datetime.now().isoformat()}\n\n")

            f.write("SUMMARY\n")
            f.write("-" * 80 + "\n")
            f.write(f"Total patients: {self.stats['total_patients']}\n")
            f.write(f"Successful: {self.stats['successful']}\n")
            f.write(f"Failed: {self.stats['failed']}\n\n")

            if self.stats['errors']:
                f.write("ERRORS\n")
                f.write("-" * 80 + "\n")
                for error in self.stats['errors']:
                    f.write(f"Patient {error['patient_id']}: {error['error']}\n")

        logging.info(f"Report saved to {report_path}")


def main():
    """Main execution function."""
    # Setup logging
    log_dir = output_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"trajectory_generation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    logging.info("=" * 80)
    logging.info("PATIENT TRAJECTORY GENERATION")
    logging.info("=" * 80)
    logging.info(f"Patient data directory: {patient_data_dir}")
    logging.info(f"Mapping directory: {mapping_dir}")
    logging.info(f"Output directory: {output_dir}")
    logging.info(f"Max workers: {MAX_WORKERS}")

    # Run generation
    generator = TrajectoryGenerator()
    generator.load_concept_mappings()
    generator.generate_all_trajectories()
    generator.save_report()

    logging.info("=" * 80)
    logging.info("TRAJECTORY GENERATION COMPLETE")
    logging.info("=" * 80)


if __name__ == "__main__":
    main()
