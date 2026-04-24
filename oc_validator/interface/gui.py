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

from bs4 import BeautifulSoup, Tag
from oc_validator.table_reader import MetadataRow, CitationsRow, AgentItem, VenueInfo
from typing import Union, List
import colorsys
import json
from oc_validator.helper import CSVStreamReader
from jinja2 import Environment, FileSystemLoader
from os.path import dirname, abspath, realpath
import random
from os.path import join

def generate_error_colors(n: int) -> set[str]:
    """
    Generate *n* visually distinct random colours as hex strings.

    Uses HSV colour space with controlled saturation and value ranges to
    ensure readable colours.

    :param n: Number of distinct colours to generate.
    :type n: int
    :return: Set of hex colour strings (e.g. ``'#a3f2c1'``).
    :rtype: set[str]
    """
    colors = set()

    while len(colors) < n:
        h = random.random()
        s = random.uniform(0.5, 0.9)
        v = random.uniform(0.7, 0.95)

        r, g, b = colorsys.hsv_to_rgb(h, s, v)

        hex_color = '#{:02x}{:02x}{:02x}'.format(
            int(r * 255), int(g * 255), int(b * 255)
        )

        colors.add(hex_color)

    return colors


def model_row_default(row:Union[MetadataRow, CitationsRow], row_idx: int) -> dict:
    """
    Models a row of the input table as a dictionary with a structure suitable 
    for enrichment with error information and rendering in the HTML template. 
    Each field value is broken down into items, and each item is associated with 
    an empty list of associated issues that can be populated later.

    The resulting dictionary has the following structure: ::

        {
            "contains_issue": False,  # will be updated to True if any error is associated
            "row_idx": 4,  # index of the row in the original table
            "fields": {
                "id": [
                    {
                        "raw": "doi:10.4242/x",  # original value of the whole item (without separators)
                        "item_id": "4-id-0",  # unique identifier for the item
                        "issues": []  # error IDs that affect this item (default empty list)
                    },
                    ...
                ],
                "title": [...],
                ...
            }
        }
    
    :param row: the original row to be modelled
    :type row: Union[MetadataRow, CitationsRow]
    :param row_idx: 0-based index of the row in the original table, used to create unique item IDs and for error mapping
    :type row_idx: int
    :return: a dictionary representing the modelled row, ready for enrichment with error information and rendering in the HTML template
    :rtype: dict
    """

    default_model = {
        "contains_issue": False, 
        "row_idx": row_idx,
        "fields": {}  # <field label>:[dict]
    }

    for field_label, items_in_field in row.flat_serialise().items():
        field_value_model = []
        if not items_in_field:
            # if the field is empty an empty list (None in the report),
            #  we still want to represent it in the model, 
            # with an empty item that can be associated with errors related to the whole field
            field_value_model.append({
                "raw": "", 
                "item_id": f"{row_idx}-{field_label}-empty",  # e.g. 4-id-empty
                "issues": []
            })
        else:
            for item_idx, item in enumerate(items_in_field):

                field_value_model.append({
                    "raw": item, 
                    "item_id": f"{row_idx}-{field_label}-{item_idx}",
                    "issues": []
                })
        default_model['fields'][field_label] = field_value_model

    return default_model


def enrich_row(modeled_row: dict, error_obj: dict, err_id: str) -> dict:
    """
    Enrich the modelled row with error information, by adding the error ID to
    the ``issues`` list of each item involved in the error.

    :param modeled_row: The dictionary representing the modelled row to enrich.
    :type modeled_row: dict
    :param error_obj: The error object from the validation report.
    :type error_obj: dict
    :param err_id: Unique identifier of the error.
    :type err_id: str
    :return: The enriched modelled row (modified in place and returned).
    :rtype: dict
    """
    row_number : str = str(modeled_row['row_idx'])
    for field_label, items_indexes in error_obj['position']['table'][row_number].items():
        if items_indexes is None:
            # if None, the error is related to the whole field, 
            # so we associate it with the first (and only) virtual empty item 
            # representing the whole field value

            data_item :dict= modeled_row['fields'][field_label][0]
            data_item['issues'].append(err_id)
            break

        for item_idx in items_indexes:
            data_item :dict= modeled_row['fields'][field_label][item_idx]
            if err_id not in data_item['issues']:  # avoid error duplicates
                data_item['issues'].append(err_id)
    
    modeled_row['contains_issue'] = True
    
    return modeled_row


def map_errors_to_data(data: List[Union[MetadataRow, CitationsRow]], report: list) -> tuple[list[dict], dict]:
    """
    Map validation report errors to the corresponding data items in the original table.

    :param data: The original table data, as a list of MetadataRow or CitationsRow objects.
    :type data: List[Union[MetadataRow, CitationsRow]]
    :param report: The validation report, as a list of error dictionaries.
    :type report: list
    :return: A tuple ``(enriched_rows, mapped_errors)`` where *enriched_rows* is a
        list of row dictionaries and *mapped_errors* maps error IDs to their metadata.
    :rtype: tuple[list[dict], dict]
    """

    out_data = [model_row_default(row, idx) for idx, row in enumerate(data)]

    table_type:str = 'cits' if isinstance(data[0], CitationsRow) else 'meta'

    del data  # free memory

    colors = generate_error_colors(len(report))

    out_errors = {}
    
    for err_idx, error_obj in enumerate(report):
        err_id = f"{table_type}-{err_idx}"  # e.g. meta-0, cits-1

        # store error info
        out_errors[err_id] = {
            "message": error_obj['message'],
            "label": error_obj['error_label'],  # can be used for grouping and filtering in HTML
            "level": error_obj['error_type'],  # error|warning
            "color": colors.pop()
        }

        # enrich data with error info
        invalid_rows_indexes :dict = [int(k) for k in error_obj['position']['table'].keys()]
        for i in invalid_rows_indexes:
            out_data[i] = enrich_row(out_data[i], error_obj, err_id)
        
    return out_data, out_errors


def make_gui(csv_fp: str, report_fp: str, out_fp: str) -> None:
    """
    Generate an HTML document that visualises the validation results.

    Maps errors from the validation report to the corresponding data items
    in the original CSV table and renders them in a user-friendly HTML page.

    :param csv_fp: Path to the original CSV data file.
    :type csv_fp: str
    :param report_fp: Path to the JSON-Lines validation report.
    :type report_fp: str
    :param out_fp: Path where the generated HTML document will be saved.
    :type out_fp: str
    :rtype: None
    """

    # separators for items according to the field,
    # used in the template to reconstruct the whole field value 
    # from the list of items
    item_separators = {
        'citing_id': ' ',
        'cited_id': ' ',
        'id': ' ',
        'author': '; ',
        'publisher': '; ',
        'editor': '; '
    }

    current_dir = dirname(abspath(__file__))
    env = Environment(
        loader=FileSystemLoader(current_dir),
        trim_blocks=True,
        lstrip_blocks=True
    )

    # Read JSON-Lines file (one JSON object per line)
    report = []
    with open(report_fp, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():  # Skip empty lines
                report.append(json.loads(line))
    
    with open(join(current_dir, 'script.js'), 'r') as script_file, open(join(current_dir, 'style.css'), 'r') as style_file:
        script = script_file.read()
        style = style_file.read()

    if not report:
        # template = env.get_template('valid_page.html')
        # html_output = template.render()

        with open(out_fp, "w", encoding='utf-8') as file, open('valid_page.html', 'r') as valid_page:
            file.write(valid_page.read())

        print("No errors found: valid HTML generated.")
        return
    
    # Use streaming to read CSV file efficiently
    csv_stream = CSVStreamReader(csv_fp)
    
    # Read first row to determine table type
    first_row = None
    for row in csv_stream:
        first_row = row
        break
    
    table_type = 'meta' if len(list(first_row.keys())) > 4 else 'cits'
    parser = MetadataRow if table_type == 'meta' else CitationsRow

    # Stream and parse all rows
    structured_data = [parser(row) for row in csv_stream.stream()]

    mapped_data, mapped_errors = map_errors_to_data(structured_data, report)

    template = env.get_template('invalid_template.j2')
    
    html_output = template.render(
        error_count=len(mapped_errors),
        data=mapped_data,
        errors=mapped_errors,
        item_separators=item_separators,
        script=script,
        style=style
    )

    with open(out_fp, "w", encoding='utf-8') as file:
        file.write(html_output)

    print(f"HTML document generated successfully at {realpath(out_fp)}.")

    return None

def merge_html_files(doc1_fp: str, doc2_fp: str, merged_out_fp: str) -> None:
    """
    Merge two HTML documents into a single document.

    Combines the table containers from both documents and interleaves the
    general-info sections.

    :param doc1_fp: Path to the first HTML document.
    :type doc1_fp: str
    :param doc2_fp: Path to the second HTML document.
    :type doc2_fp: str
    :param merged_out_fp: Path for the output merged HTML document.
    :type merged_out_fp: str
    :rtype: None
    """
    with open(doc1_fp, 'r', encoding='utf-8') as fhandle1, open(doc2_fp, 'r', encoding='utf-8') as fhandle2:
        soup1 = BeautifulSoup(fhandle1, 'html.parser')
        soup2 = BeautifulSoup(fhandle2, 'html.parser')

    # general_info_1 = soup1.find('div', class_='general-info')
    general_info_2 = soup2.find('div', class_='general-info')
    table_1_container = soup1.find('div', class_='table-container')
    table_2_container = soup2.find('div', class_='table-container')
    table_1_container.insert_after(general_info_2)
    general_info_2.insert_after(table_2_container)
    
    html_out = str(soup1)
    with open(merged_out_fp, "w", encoding='utf-8') as outf:
        outf.write(html_out)
    print(f"HTML document generated successfully at {realpath(outf.name)}.")