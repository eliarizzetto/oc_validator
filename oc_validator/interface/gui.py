from bs4 import BeautifulSoup, Tag
from oc_validator.table_reader import MetadataRow, CitationsRow, AgentItem, VenueInfo
from typing import Union, List
import colorsys
import json
from oc_validator.helper import read_csv
from jinja2 import Environment, FileSystemLoader
from os.path import dirname, abspath, realpath
import random
from os.path import join

def generate_error_colors(n) -> set:
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


def enrich_row(modeled_row:dict, error_obj:dict, err_id:str):
    """
        Enriches the modelled row with the error information, by adding the error ID to the 'issues' list of each item involved in the error.
    
    :param modeled_row: the dictionary representing the modelled row to be enriched with error information
    :type modeled_row: dict
    :param error_obj: the error object (as taken from the validation report) containing information about the error to be associated with one or more pieces of data in the table
    :type error_obj: dict
    :param err_id: the unique identifier of the error
    :type err_id: str
    """

    for row_idx, field_info in error_obj['position']['table'].items():
        for field_label, items_indexes in field_info.items():
            if items_indexes is None:
                # if None, the error is related to the whole field, 
                # so we associate it with the first (and only) virtual empty item 
                # representing the whole field value
                items_indexes = [0]  
            for item_idx in items_indexes:
                data_item :dict= modeled_row['fields'][field_label][item_idx]
                if err_id not in data_item['issues']:  # avoid error duplicates
                    data_item['issues'].append(err_id)
    
    modeled_row['contains_issue'] = True
    
    return modeled_row


def map_errors_to_data(data:List[Union[MetadataRow, CitationsRow]], report:list):
    """
    Maps the errors in the validation report to the corresponding pieces of data 
    in the original table, by enriching the modelled data with error information.

    :param data: the original table data, as a list of MetadataRow or CitationsRow objects
    :type data: List[Union[MetadataRow, CitationsRow]]
    :param report: the validation report, as a list of error objects (dictionaries) 
    :type report: list
    :return: a tuple containing the enriched rows (as a list of dictionaries) and the mapped errors (as a dictionary of error information)
    :rtype: Tuple[List[dict], dict]
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


def make_gui(csv_fp:str, report_fp:str, out_fp:str):
    """
    Generates an HTML document that visualises the validation results, 
    by mapping the errors in the validation report to the corresponding pieces 
    of data in the original table and rendering the enriched data and error 
    information in a user-friendly format.
    
    :param csv_fp: the file path of the original CSV data, used to model the data and map errors to it
    :type csv_fp: str
    :param report_fp: Description of the file path of the validation report, used to extract error information and map it to the data
    :type report_fp: str
    :param out_fp: the file path where the generated HTML document will be saved
    :type out_fp: str
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

    with open(report_fp, 'r') as f:
        report = json.load(f)
    
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
    
    raw_data = read_csv(csv_fp) # as read with csv.DictReader, i.e. list of dicts
    
    table_type = 'meta' if len(list(raw_data[0].keys())) > 4 else 'cits'
    parser = MetadataRow if table_type == 'meta' else CitationsRow

    structured_data = [parser(row) for row in raw_data]

    del raw_data  # free memory

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

def merge_html_files(doc1_fp, doc2_fp, merged_out_fp):
    """
    Merges two HTML documents into a single document. 
    :param doc1_fp: the file path to the first HTML document.
    :param doc2_fp: the file path to the second HTML document.
    :param merged_out_fp: the file path to the output merged HTML document.
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