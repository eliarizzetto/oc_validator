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

"""Command-line interface for oc_validator."""

from argparse import ArgumentParser
from oc_validator.main import Validator, ClosureValidator


def add_common_args(parser: ArgumentParser) -> None:
    """Add arguments shared by both subcommands."""
    parser.add_argument(
        '--use-lmdb', dest='use_lmdb', action='store_true', default=False,
        help='Enable LMDB for efficient memory usage with large files.')
    parser.add_argument(
        '--map-size', dest='map_size', type=int, default=1,
        help='LMDB map size in GiB (default: 1).')
    parser.add_argument(
        '--cache-dir', dest='cache_dir', type=str, default=None,
        help='Base directory under which all LMDB caches are created.')
    parser.add_argument(
        '-v', '--verbose', dest='verbose', action='store_true', default=False,
        help='Enable verbose logging output.')
    parser.add_argument(
        '--log-file', dest='log_file', type=str, default=None,
        help='Write logs to this file instead of the terminal.')


def cmd_validate(args) -> None:
    """Run single-table validation."""
    v = Validator(
        args.input_csv,
        args.output_dir,
        use_meta_endpoint=args.use_meta_endpoint,
        verify_id_existence=args.verify_id_existence,
        use_lmdb=args.use_lmdb,
        map_size=args.map_size * 1024 ** 3,
        cache_dir=args.cache_dir,
        verbose=args.verbose,
        log_file=args.log_file,
    )
    v.validate()


def cmd_closure(args) -> None:
    """Run closure validation on a META-CSV + CITS-CSV pair."""
    cv = ClosureValidator(
        meta_in=args.meta,
        meta_out_dir=args.meta_out,
        cits_in=args.cits,
        cits_out_dir=args.cits_out,
        strict_sequentiality=args.strict_sequentiality,
        meta_kwargs={
            'use_meta_endpoint': args.use_meta_endpoint,
            'verify_id_existence': args.verify_id_existence,
        },
        cits_kwargs={
            'use_meta_endpoint': args.use_meta_endpoint,
            'verify_id_existence': args.verify_id_existence,
        },
        use_lmdb=args.use_lmdb,
        map_size=args.map_size * 1024 ** 3,
        cache_dir=args.cache_dir,
        verbose=args.verbose,
        log_file=args.log_file,
    )
    cv.validate()


def main() -> None:
    """Entry point for the ``oc_validator`` CLI."""
    parser = ArgumentParser(
        prog='oc_validator',
        description='Validate CSV documents storing citation data and bibliographic '
                    'metadata according to the OpenCitations Data Model.')
    subparsers = parser.add_subparsers(dest='command', required=True)

    # --- validate subcommand ---
    validate_parser = subparsers.add_parser(
        'validate',
        help='Validate a single META-CSV or CITS-CSV document.')
    validate_parser.add_argument(
        '-i', '--input', dest='input_csv', required=True, type=str,
        help='Path to the CSV document to validate.')
    validate_parser.add_argument(
        '-o', '--output', dest='output_dir', required=True, type=str,
        help='Directory where the output JSON-L file will be stored.')
    validate_parser.add_argument(
        '-m', '--use-meta', dest='use_meta_endpoint', action='store_true',
        help='Use the OC Meta endpoint to check if an ID exists.')
    validate_parser.add_argument(
        '-s', '--no-id-existence', dest='verify_id_existence', action='store_false',
        help='Skip checking if IDs are registered somewhere.')
    add_common_args(validate_parser)
    validate_parser.set_defaults(func=cmd_validate)

    # --- closure subcommand ---
    closure_parser = subparsers.add_parser(
        'closure',
        help='Validate a META-CSV and CITS-CSV pair together, checking transitive closure.')
    closure_parser.add_argument(
        '--meta', required=True, type=str,
        help='Path to the META-CSV file.')
    closure_parser.add_argument(
        '--meta-out', required=True, type=str,
        help='Output directory for META-CSV validation results.')
    closure_parser.add_argument(
        '--cits', required=True, type=str,
        help='Path to the CITS-CSV file.')
    closure_parser.add_argument(
        '--cits-out', required=True, type=str,
        help='Output directory for CITS-CSV validation results.')
    closure_parser.add_argument(
        '--strict-sequentiality', dest='strict_sequentiality', action='store_true', default=False,
        help='Skip closure check if individual validations already report errors.')
    closure_parser.add_argument(
        '-m', '--use-meta', dest='use_meta_endpoint', action='store_true',
        help='Use the OC Meta endpoint to check if an ID exists.')
    closure_parser.add_argument(
        '-s', '--no-id-existence', dest='verify_id_existence', action='store_false',
        help='Skip checking if IDs are registered somewhere.')
    add_common_args(closure_parser)
    closure_parser.set_defaults(func=cmd_closure)

    args = parser.parse_args()
    args.func(args)
