import csv
from collections import Counter
from typing import List, Dict, Tuple, Optional
from collections import defaultdict
import typer

app = typer.Typer()

def replace_spaces_in_headers(headers: List[str]) -> dict:
    """replaces spaces in headers

    Args:
        headers (List[str]): the original header list

    Returns:
        dict: the mapping dictionary to column names without spaces
    """
    return {header:header.strip().replace(" ", "_") for header in headers}

def rename_keys(row: dict, keys: dict) -> dict:
    """rename keys for an individual row

    Args:
        row (dict): The csv row as dict
        keys (dict): the key mapping from one with to one without spaces

    Returns:
        dict: the csv row as dict where keys do not have spaces
    """
    return {keys[key]:val for key,val in row.items()}

def read_csv_file(file_path: str) -> Tuple[List[str], List[dict]]:
    """
    Reads a CSV file, renames the headers to not include spaces

    Returns:
        Tuple[headers, rows]: the headers (list) and rows (list of dictionaries) of the csv
    """
   
    try:
        with open(file_path, encoding="iso8859-1") as f:
            reader = csv.DictReader(f, delimiter=';')
            headers = replace_spaces_in_headers(reader.fieldnames)
            rows = [rename_keys(row, headers) for row in reader]

            return list(headers.values()), rows
        
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        exit(1)
    except csv.Error as e:
        print(f"Error while reading '{file_path}': {e}")
        exit(1)


def summarize_column(column_values: List[str]) -> dict:
    """Summarizes a singe column value list

    Args:
        column_values (List[str]): the value list

    Returns:
        dict: the count of each unique value in the list
    """
    category_counts = dict()
    # print(column_values)
    for value in column_values:
        if value not in category_counts:
            category_counts[value] = 0
        category_counts[value] += 1

    return category_counts

def column_summary(column_name:str, rows: List[dict]) -> dict:
    return summarize_column([row[column_name] for row in rows])
   

def all_column_summary(headers:List[str], rows: List[dict] ) -> dict:
    summary = {}
    for header in headers:
        if header not in (
            "Betrag", 
            "BIC_(SWIFT-Code)", 
            "Kontonummer/IBAN", 
            "Beguenstigter/Zahlungspflichtiger", 
            "Sammlerreferenz", 
            "Kundenreferenz_(End-to-End)",
            "Mandatsreferenz",
            "Glaeubiger_ID",
            "Verwendungszweck",
            "Auftragskonto",
            "Buchungstag",
            "Valutadatum"):
            summary[header] = column_summary(header, rows)
    
    return summary


@app.command()
def summarize_csv(filename: str, column_name:Optional[str] = typer.Argument(None), print_headers:Optional[bool] = typer.Argument(False)):
    headers, rows = read_csv_file(filename)
    if print_headers:
        print(headers)

    if column_name is not None:
        if column_name in headers:
            summary = column_summary(column_name, rows)
        else:
            print(f"Error: column '{column_name}' not found in headers: {headers}.")
            exit(1) 
    else:
        summary = all_column_summary(headers, rows)
    print(summary)



if __name__ == "__main__":
    app()
