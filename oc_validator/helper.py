# ISC License
#
# Copyright (c) 2023-2026, Elia Rizzetto, Silvio Peroni
#
# Permission to use, copy, modify, and/or distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
# INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
# LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
# OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
# PERFORMANCE OF THIS SOFTWARE.

from collections import defaultdict
from csv import field_size_limit, DictReader
from typing import Generator, Iterator, Optional
import json


class UnionFind:
    """Union-Find (Disjoint Set Union) data structure for grouping related identifiers."""

    def __init__(self) -> None:
        """
        Initialise an empty Union-Find structure.

        :rtype: None
        """
        self.parent = dict()

    def find(self, x: str) -> str:
        """
        Return the root of the component containing *x*.

        If *x* has never been seen before it is registered as its own root.
        Path compression is applied on every lookup.

        :param x: Element identifier.
        :type x: str
        :return: Root identifier of the component.
        :rtype: str
        """
        if x not in self.parent:
            self.parent[x] = x
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])  # Path compression
        return self.parent[x]

    def union(self, x: str, y: str) -> None:
        """
        Merge the components containing *x* and *y*.

        The root of *x* is made a child of the root of *y*.

        :param x: First element.
        :type x: str
        :param y: Second element.
        :type y: str
        :rtype: None
        """
        self.parent[self.find(x)] = self.find(y)

class Helper:
    """Container for utility functions used across the validation pipeline."""

    def __init__(self) -> None:
        """
        Initialise the Helper.

        :rtype: None
        """
        self.descr = 'contains helper functions'

    def group_ids(self, id_groups: list[set]) -> list[set]:
        """
        Group identifiers that co-occur in the same row into connected components.

        Uses a Union-Find algorithm so that two IDs are considered to belong to the
        same bibliographic entity if they appear together in at least one row.

        :param id_groups: List of sets, where each set contains the identifiers
            from a single row field (e.g. ``id``, ``citing_id``).
        :type id_groups: list[set]
        :return: List of sets, each grouping the IDs of the same entity.
        :rtype: list[set]
        """

        uf = UnionFind()

        # Union all IDs that appear together in a group
        for group in id_groups:
            ids = list(group)
            for i in range(1, len(ids)):
                uf.union(ids[0], ids[i])

        # Gather groups
        components = defaultdict(set)
        for group in id_groups:
            for id_ in group:
                root = uf.find(id_)
                components[root].add(id_)

        return list(components.values())

    def create_error_dict(self, validation_level: str, error_type: str, message: str, error_label: str, located_in: str,
                          table: dict, valid: bool = False) -> dict:
        """
        Create a dictionary representing a validation error or warning.

        :param validation_level: One of ``"csv_wellformedness"``, ``"external_syntax"``,
            ``"existence"``, or ``"semantic"``.
        :type validation_level: str
        :param error_type: One of ``"error"`` or ``"warning"``.
        :type error_type: str
        :param message: Human-readable error description.
        :type message: str
        :param error_label: Machine-readable label uniquely connected to one validation check.
        :type error_label: str
        :param located_in: Granularity of the error location — one of ``"row"``,
            ``"field"``, or ``"item"``.
        :type located_in: str
        :param table: Tree structure pinpointing the exact position of all elements
            involved in the error.
        :type table: dict
        :param valid: Whether the data is still considered valid despite the issue.
            Defaults to ``False``.
        :type valid: bool
        :return: Error dictionary consumable by the report writer.
        :rtype: dict
        """

        position = {
            'located_in': located_in,
            'table': table
        }

        result = {
            'validation_level': validation_level,
            'error_type': error_type,
            'error_label': error_label,
            'valid': valid,
            'message': message,
            'position': position
        }

        return result


    def create_validation_summary_stream(self, json_fp: str) -> Generator[str, None, None]:
        """
        Stream a natural-language summary of the validation error report.

        Performs two passes over the JSON-Lines file: the first counts errors
        per label and stores the explanation text, the second yields formatted
        lines grouped by error label.

        :param json_fp: Path to the JSON-Lines file containing the validation error report.
        :type json_fp: str
        :return: Generator yielding lines of the summary.
        :rtype: Generator[str, None, None]
        """

        # ---- FIRST PASS: count errors per label + store explanation ----
        error_counts = {}
        label_explanations = {}

        with JSONLStreamIO(json_fp) as jsonl_stream:
            for error in jsonl_stream:
                label = error['error_label']
                error_counts[label] = error_counts.get(label, 0) + 1

                # store explanation once
                if label not in label_explanations:
                    label_explanations[label] = error['message']

        # ---- SECOND PASS: stream output ----
        with JSONLStreamIO(json_fp) as jsonl_stream:

            current_label_seen = {label: 0 for label in error_counts}

            for error in jsonl_stream:
                label = error['error_label']

                # If first time we encounter this label → print header
                if current_label_seen[label] == 0:
                    count = error_counts[label]
                    explanation = label_explanations[label] + "\n"

                    count_summary = (
                        f"There are {count} {label} issues in the document.\n"
                        if count > 1
                        else f"There is {count} {label} issue in the document.\n"
                    )

                    yield count_summary
                    yield explanation

                # ---- build location string ----
                tree = error['position']['table']
                all_locs = []

                for row_node_pos, row_node_value in tree.items():
                    for field_node_name, field_node_value in row_node_value.items():
                        single_node_pos = (
                            f"row {row_node_pos}, field {field_node_name}, "
                            f"and items in position {field_node_value}"
                        )
                        all_locs.append(single_node_pos)

                location = "; ".join(all_locs)

                # ---- detail line ----
                current_label_seen[label] += 1
                idx = current_label_seen[label]

                if error_counts[label] > 1:
                    detail = f"- {error['error_type']} {idx} involves: {location}.\n"
                else:
                    detail = f"- The {error['error_type']} involves: {location}.\n"

                yield detail

                # spacing between groups
                if current_label_seen[label] == error_counts[label]:
                    yield "\n\n"


class CSVStreamReader:
    """
    A streamable CSV reader that yields rows one at a time, allowing for memory-efficient
    processing of large CSV files.

    Supports multiple passes by reopening the file. The delimiter is auto-detected
    from the first rows (tries comma, semicolon, and tab).
    """
    def __init__(self, csv_fp: str) -> None:
        """
        Initialise the reader and auto-detect the CSV delimiter and field names.

        :param csv_fp: Path to the CSV file to read.
        :type csv_fp: str
        :rtype: None
        """
        self.csv_fp = csv_fp
        self._delimiter: Optional[str] = None
        self._fieldnames: Optional[list[str]] = None
        self._detect_delimiter_and_fieldnames()

    def _detect_delimiter_and_fieldnames(self) -> None:
        """
        Detect the CSV delimiter and field names from the first rows.

        Tries ``','``, ``';'``, and ``'\\t'`` in order and selects the first one
        that produces a row with more than one column.

        :raises ValueError: if no valid delimiter can be determined.
        :rtype: None
        """
        field_size_limit(100000000)  # sets 100 MB as size limit for parsing larger csv fields
        for delimiter in [',', ';', '\t']:
            with open(self.csv_fp, newline='', encoding='utf-8') as f:
                reader = DictReader(f, delimiter=delimiter)
                # Read first row to check if delimiter is correct
                try:
                    first_row = next(reader)
                    if first_row and len(first_row) > 1:  # if dict has more than 1 key, we assume it's read correctly
                        self._delimiter = delimiter
                        self._fieldnames = reader.fieldnames
                        return
                except StopIteration:
                    continue  # Empty file, try next delimiter
        raise ValueError("Could not detect CSV delimiter")

    def stream(self) -> Iterator[dict]:
        """
        Stream rows from the CSV file one at a time.

        Each call reopens the file, so the generator can be consumed multiple
        times for separate validation passes.

        :return: Iterator of row dictionaries (as returned by ``csv.DictReader``).
        :rtype: Iterator[dict]
        """
        field_size_limit(100000000)
        with open(self.csv_fp, newline='', encoding='utf-8') as f:
            reader = DictReader(f, delimiter=self._delimiter)  # if fieldnames is specified, DictReader interprets the first row as data, not header!
            for row in reader:
                yield row

    def __iter__(self) -> Iterator[dict]:
        """
        Make the reader directly iterable.

        :return: Row iterator (delegates to :meth:`stream`).
        :rtype: Iterator[dict]
        """
        return self.stream()


class JSONLStreamIO:
    """
    Context manager for reading and writing JSON-Lines (JSONL) files.

    Each line in the file is a separate JSON object. Supports both read and
    write modes and can be used as an iterator for line-by-line consumption.
    """
    def __init__(self, jsonl_fp: str, mode: str = 'r') -> None:
        """
        Initialise the JSON-Lines handler.

        :param jsonl_fp: Path to the JSON-Lines file.
        :type jsonl_fp: str
        :param mode: File open mode (``'r'``, ``'w'``, or ``'a'``).
            Defaults to ``'r'``.
        :type mode: str
        :rtype: None
        """
        self.jsonl_fp = jsonl_fp
        self.mode = mode
        self._file = None

    def __enter__(self):
        """
        Open the underlying file and return this instance.

        :return: The :class:`JSONLStreamIO` instance.
        :rtype: JSONLStreamIO
        """
        self._file = open(self.jsonl_fp, self.mode, encoding='utf-8')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Close the underlying file on context exit.

        :rtype: None
        """
        if self._file:
            self._file.close()

    def is_empty(self) -> bool:
        """
        Check whether the JSON-Lines file is empty or contains only blank lines.

        :return: ``True`` if the file has no non-empty JSON lines, ``False`` otherwise.
        :rtype: bool
        """
        with open(self.jsonl_fp, 'r', encoding='utf-8') as f:
            for line in f:
                if json.loads(line.strip()):
                    return False
        return True

    def read(self) -> Iterator[dict]:
        """
        Read the JSON-Lines file, yielding one parsed JSON object per line.

        :return: Iterator of dictionaries parsed from each line.
        :rtype: Iterator[dict]
        """
        with open(self.jsonl_fp, 'r', encoding='utf-8') as f:
            for line in f:
                yield json.loads(line.strip())

    def __iter__(self) -> Iterator[dict]:
        """
        Make the handler directly iterable (delegates to :meth:`read`).

        :return: Iterator of parsed JSON objects.
        :rtype: Iterator[dict]
        """
        return self.read()

    def write(self, json_obj: dict) -> None:
        """
        Write a JSON object as a single line to the file.

        The file must already be open via the context manager in ``'w'`` or ``'a'`` mode.

        :param json_obj: JSON-serialisable object to write.
        :type json_obj: dict
        :raises ValueError: if the file has not been opened via the context manager.
        :rtype: None
        """
        if self._file is None:
            raise ValueError("File not open. Use context manager with mode='a' or 'w'.")
        self._file.write(json.dumps(json_obj) + '\n')

    def flush(self) -> None:
        """
        Flush the underlying file buffer.

        :rtype: None
        """
        if self._file:
            self._file.flush()

def read_csv(csv_fp: str) -> list[dict]:
    """
    Read an entire CSV file into memory.

    .. deprecated::
        Use :class:`CSVStreamReader` for memory-efficient streaming instead.

    :param csv_fp: Path to the CSV file.
    :type csv_fp: str
    :return: List of row dictionaries.
    :rtype: list[dict]
    :raises ValueError: if no valid delimiter can be determined.
    """
    field_size_limit(100000000)  # sets 100 MB as size limit for parsing larger csv fields
    for delimiter in [',', ';', '\t']:
        with open(csv_fp, newline='', encoding='utf-8') as f:
            reader = DictReader(f, delimiter=delimiter)
            rows = list(reader)
            if rows and len(rows[0]) > 1:  # if each dict has more than 1 key, we assume it's read correctly
                return rows
    raise ValueError("Could not detect CSV delimiter")
