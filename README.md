# oc_validator

**oc_validator** is a Python (≥3.9) library to validate CSV documents storing citation data and bibliographic metadata.
To be processed by the validator, the tables must be built as either CITS-CSV or META-CSV tables, defined in two specification documents[^1][^2].

[^1]: Massari, Arcangelo, and Ivan Heibi. 2022. ‘How to Structure Citations Data and Bibliographic Metadata in the OpenCitations Accepted Format’. https://doi.org/10.48550/arXiv.2206.03971.

[^2]: Massari, Arcangelo. 2022. ‘How to Produce Well-Formed CSV Files for OpenCitations’. https://doi.org/10.5281/zenodo.6597141.

## Installation
The library can be installed from **PyPI**:
```
pip install oc_validator
```

## Contributing / Development

This project uses [uv](https://docs.astral.sh/uv/) for dependency management and building. To set up a development environment:

```bash
# Clone the repository
git clone https://github.com/opencitations/oc_validator.git
cd oc_validator

# Create a virtual environment and install dependencies
uv sync
```

### Running tests

```bash
uv run python -m unittest discover -s tests
```

### Building

```bash
uv build
```

## Usage

The CLI provides two subcommands:

- **`validate`** — validate a single META-CSV or CITS-CSV document.
- **`closure`** — validate a META-CSV and CITS-CSV pair together, checking transitive closure.

```bash
oc_validator validate -i <input csv file path> -o <output dir path> [-m] [-s] [--use-lmdb [--map-size <GiB>] [--cache-dir <dir>]]

oc_validator closure --meta <meta csv file path> --meta-out <meta output dir> --cits <cits csv file path> --cits-out <cits output dir> [--strict-sequentiality] [-m] [-s] [--use-lmdb [--map-size <GiB>] [--cache-dir <dir>]]
```

Alternatively, the program can also be invoked as a Python module:

```bash
python -m oc_validator.main -i <input csv file path> -o <output dir path> [options]
```

### Required Parameters — `validate`

- `-i`, `--input`: The path to the CSV file to validate.
- `-o`, `--output`: The path to the directory where the output JSON-Lines file and .txt file will be stored.

### Required Parameters — `closure`

- `--meta`: The path to the META-CSV file.
- `--meta-out`: The output directory for META-CSV validation results.
- `--cits`: The path to the CITS-CSV file.
- `--cits-out`: The output directory for CITS-CSV validation results.

### Optional Parameters (both subcommands)

- `-m`, `--use-meta`: Look for an ID in OpenCitations Meta (querying Meta SPARQL endpoint) before any other call to external services: if the ID is present in Meta, the ID is considered as existing and no other requests are made; otherwise it is looked for in the applicable external database(s) via the appropriate APIs (e.g. Crossref). According to the nature of the data, this option might make the process faster (if the majority of the IDs in the table are already registered in OC Meta) or slow it down (in case most IDs are not present in OC Meta or do not exist at all).
- `-s`, `--no-id-existence`: Skips the check for ID existence altogether, ensuring that neither the Meta endpoint nor any external APIs are used during validation. This allows for a much shorter execution time, but does not make sure that all the submitted IDs actually refer to real-world entities.
- `--use-lmdb`: Enables LMDB-backed caching instead of in-memory Python objects. Recommended when validating large input files that risk saturating the available RAM. When this flag is set, all internal caches (ID lookup caches, duplicate detection caches, Union-Find structures) are temporarily written on/read from disk using LMDB environments, keeping the memory footprint bounded regardless of input size.
- `--map-size`: Specifies the maximum size of each LMDB environment in gibibytes (GiB). Defaults to 1. Increase this value if you encounter `lmdb.MapFullError` during validation of very large files. NOTE: On Windows, you must have enough free disk space for at least 4x `map_size` per each `Validator` object.
- `--cache-dir`: Specifies the base directory under which all LMDB cache directories are created. Defaults to the current working directory. The validator creates and automatically cleans up temporary directories inside this path.

### Optional Parameters — `closure` only

- `--strict-sequentiality`: Skip the transitive closure check if the individual validations of the metadata and/or citations tables already report errors. By default the closure check is always performed.

### Example Usage from CLI

To validate a single CSV file and output the results to a specified directory (with optional parameters set to default values, i.e. checking for the existence of IDs via querying external APIs):

```bash
oc_validator validate -i path/to/input.csv -o path/to/output_dir
```

To use OC Meta endpoint instead of external APIs to verify the existence of the IDs:

```bash
oc_validator validate -i path/to/input.csv -o path/to/output_dir -m
```

To skip all ID existence verification:

```bash
oc_validator validate -i path/to/input.csv -o path/to/output_dir -s
```

To validate a large CSV file using LMDB-backed caching (with a 4 GiB map size and a custom cache directory):

```bash
oc_validator validate -i path/to/large_input.csv -o path/to/output_dir --use-lmdb --map-size 4 --cache-dir /tmp/lmdb_cache
```

To validate a META-CSV and CITS-CSV pair together, checking transitive closure:

```bash
oc_validator closure --meta path/to/meta.csv --meta-out path/to/meta_output --cits path/to/cits.csv --cits-out path/to/cits_output
```

Same as above, but skipping the closure check if individual validations already report errors:

```bash
oc_validator closure --meta path/to/meta.csv --meta-out path/to/meta_output --cits path/to/cits.csv --cits-out path/to/cits_output --strict-sequentiality
```

### Programmatic Usage

An object of the `Validator` class is instantiated, passing as parameters the path to the input document to validate and the path to the directory where to store the output. By calling the `validate()` method on the instance of `Validator`, the validation process gets executed.

The process automatically detects which of the two tables has been passed as input (on condition that the input CSV document's header is formatted correctly for at least one of them). During the process, the *whole* document is always processed: if the document is invalid or contains anomalies, the errors/warnings are reported in detail in a JSON-Lines file and summarized in a .txt file, which will be automatically created in the output directory.

```python
from oc_validator.main import Validator

# Basic validation
v = Validator('path/to/table.csv', 'output/directory')
v.validate()

# Validation with Meta endpoint checking for ID existence
v = Validator('path/to/table.csv', 'output/directory', use_meta_endpoint=True)
v.validate()

# Validation skipping all ID existence checks
v = Validator('path/to/table.csv', 'output/directory', verify_id_existence=False)
v.validate()

# Validation with LMDB-backed caching (recommended for large files)
with Validator('path/to/large_table.csv', 'output/directory',
               use_lmdb=True, map_size=4*1024**3, cache_dir='/tmp/cache') as v:
    v.validate()
```

`Validator` also supports use as a context manager (`with` statement) to ensure LMDB caches and other resources are properly cleaned up, which is especially recommended when using `use_lmdb=True`.

Starting from version 0.3.3, it is possible to validate two tables at a time, one storing metadata and the other storing citations, in order to verify, besides all the other checks, that all the citations represented in a document have their metadata represented in the other document, and vice versa. This can be done by using the `ClosureValidator` class. The `ClosureValidator` class internally wraps two instances of `Validator`, one for metadata and one for citations, and requires to explicitly specify the table type for either document. Both the internal `Validator` instances can be separately customized by specifying the optional parameters for each of the two via the `meta_kwargs` and `cits_kwargs` arguments. `ClosureValidator` takes the following parameters:

- `meta_in`: Path to the input CSV table storing metadata.
- `meta_out_dir`: Directory for metadata validation results.
- `cits_in`: Path to the input CSV table storing citations.
- `cits_out_dir`: Directory for citation validation results.
- `strict_sequentiality`: \[deafaults to False\] If True, checks the transitive closure if and only if all the other checks passed without detecting errors. With the default option (False), it is always checked that all the entities involved in citations have also their metadata represented in the other table, and vice versa, *regardless* of the presence of other errors in the tables.
- `meta_kwargs`: (Optional) Dictionary of configuration options for the metadata table validator.
- `cits_kwargs`: (Optional) Dictionary of configuration options for the citation table validator.
- `use_lmdb`: \[defaults to False\] If True, both internal `Validator` instances use LMDB-backed caches instead of in-memory Python objects. Recommended for large files. This can also be set individually via `meta_kwargs` or `cits_kwargs`.
- `map_size`: \[defaults to 1 GiB\] Maximum size in bytes for each LMDB environment. Only relevant when `use_lmdb=True`. Can also be set individually via `meta_kwargs` or `cits_kwargs`.
- `cache_dir`: (Optional) Base directory under which all LMDB cache directories are created. Only relevant when `use_lmdb=True`. Can also be set individually via `meta_kwargs` or `cits_kwargs`.

A usage example of how to validate metadata and citations with `ClosureValidator` is provided as follows:

```python
from oc_validator.main import ClosureValidator

cv = ClosureValidator(
    meta_in='path/to/meta.csv',
    meta_out_dir='path/to/meta_results',
    cits_in='path/to/cits.csv',
    cits_out_dir='path/to/cits_results',
    meta_kwargs={'verify_id_existence': False},  # Skip ID existence checks for metadata
    cits_kwargs={'use_meta_endpoint': True}  # Use OC Meta before external APIs to verify the existence of PIDs
)

cv.validate() # validates the tables and saves output files in the specified (separate) directories
```

With LMDB-backed caching for large files:

```python
from oc_validator.main import ClosureValidator

with ClosureValidator(
    meta_in='path/to/large_meta.csv',
    meta_out_dir='path/to/meta_results',
    cits_in='path/to/large_cits.csv',
    cits_out_dir='path/to/cits_results',
    use_lmdb=True,
    map_size=4*1024**3,  # 4 GiB per LMDB environment
    cache_dir='/tmp/cache',
) as cv:
    cv.validate()
```

## Output visualisation

`oc_validator` has an integrated tool for the interactive visualisation of the validation results, which helps users to locate the detected errors in the document in a more intuitive way, and facilitates human understanding of the underlying problems generating them. The documentation of the visualisation tool can be found in [oc_validator/interface/README.md](oc_validator/interface/README.md).