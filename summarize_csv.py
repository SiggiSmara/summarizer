import csv
from typing import List, Tuple, Optional
from datetime import datetime
import typer
import polars as pl
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

app = typer.Typer()


# def replace_spaces_in_headers(headers: List[str]) -> dict:
#     """
#     Replaces spaces in headers

#     Args:
#         headers (List[str]): the original header list

#     Returns:
#         dict: the mapping dictionary to column names without spaces
#     """
#     return {header: header.strip().replace(" ", "_") for header in headers}


# def rename_keys(row: dict, keys: dict) -> dict:
#     """
#     Rename keys for an individual row

#     Args:
#         row (dict): The csv row as dict
#         keys (dict): the key mapping from one with to one without spaces

#     Returns:
#         dict: the csv row as dict where keys do not have spaces
#     """
#     return {keys[key]: val for key, val in row.items()}


def read_csv_file(file_path: str) -> pl.DataFrame:
    """
    Reads a CSV file and returns the column headers and rows as separate Polars DataFrames.
    """
    df = pl.read_csv(file_path, separator=";", encoding="iso8859-1")

    # Remove spaces from column names
    df.columns = [header.strip().replace(" ", "_") for header in df.columns]
    df = df.with_columns(
        pl.col("Buchungstag").str.strptime(pl.Date, "%d.%m.%y"),
        pl.col("Valutadatum").str.strptime(pl.Date, "%d.%m.%y"),
        pl.col("Betrag").str.replace(",", ".").cast(pl.Float64),
    )
    # print(df.head(5))
    return df


def pd_read_csv_file(file_path: str) -> pd.DataFrame:
    """
    Reads a CSV file and returns the column headers and rows as separate Polars DataFrames.
    """
    df = pd.read_csv(file_path, sep=";", encoding="iso8859-1")

    # Remove spaces from column names
    df.columns = [header.strip().replace(" ", "_") for header in df.columns]
    df["Buchungstag"] = pd.to_datetime(df["Buchungstag"], format="%d.%m.%y")
    df["Valutadatum"] = pd.to_datetime(df["Valutadatum"], format="%d.%m.%y")
    df["Betrag"] = pd.to_numeric(df["Betrag"].str.replace(",", "."))
    
    # df = df.with_columns(
    #     pl.col("Buchungstag").str.strptime(pl.Date, "%d.%m.%y"),
    #     pl.col("Valutadatum").str.strptime(pl.Date, "%d.%m.%y"),
    #     pl.col("Betrag").str.replace(",", ".").cast(pl.Float64),
    # )
    print(df.head)
    return df

# def read_csv_file_basic(file_path: str) -> Tuple[List[str], List[dict]]:
#     """
#     Reads a CSV file, renames the headers to not include spaces

#     Returns:
#         Tuple[headers, rows]: the headers (list) and rows (list of dictionaries) of the csv
#     """

#     try:
#         with open(file_path, encoding="iso8859-1") as f:
#             reader = csv.DictReader(f, delimiter=";")
#             headers = replace_spaces_in_headers(reader.fieldnames)
#             rows = [rename_keys(row, headers) for row in reader]

#             return list(headers.values()), rows

#     except FileNotFoundError:
#         print(f"Error: File '{file_path}' not found.")
#         exit(1)
#     except csv.Error as e:
#         print(f"Error while reading '{file_path}': {e}")
#         exit(1)


# def summarize_column(column_values: List[str]) -> dict:
#     """
#     Summarizes a singe column value list

#     Args:
#         column_values (List[str]): the value list

#     Returns:
#         dict: the count of each unique value in the list
#     """
#     category_counts = dict()
#     # print(column_values)
#     for value in column_values:
#         if value not in category_counts:
#             category_counts[value] = 0
#         category_counts[value] += 1

#     return category_counts


@app.command()
def summarize_csv(
    filename: str,
    # column_name: Optional[str] = typer.Argument(None),
    print_headers: Optional[bool] = typer.Argument(False),
):
    # headers, rows = read_csv_file(filename)
    df = read_csv_file(filename)
    if print_headers:
        print(df.columns)

    only_neg = (
        df.filter(pl.col("Betrag") < 0)
        # .filter(pl.col("Betrag") > -2000)
        .filter(pl.col("Buchungstag") < datetime(2023, 4, 1))
        .sort("Buchungstag")
        .with_columns(pl.col("Betrag")*-1)
    )
    costs_summary = only_neg.groupby_dynamic("Buchungstag", every="1mo").agg(
        pl.col("Betrag").sum()
    )
    costs_summary = costs_summary.with_columns(
        pl.col("Buchungstag").dt.strftime("%Y-%m").alias("yearmonth")
    )
    print(costs_summary.head(12))

    myplot = sns.lineplot(data=costs_summary, x="yearmonth", y="Betrag")
    plt.setp(myplot.get_xticklabels(), rotation=90)
    fig = myplot.get_figure()
    fig.savefig("out.png")

    # if column_name is not None:
    #     if column_name in headers:
    #         summary = summarize_column([row[column_name] for row in rows])
    #     else:
    #         print(f"Error: column '{column_name}' not found in headers: {headers}.")
    #         exit(1)
    # else:
    #     # if some are to be excluded better to that in one go
    #     summary_headers = [
    #         header
    #         for header in headers
    #         if header
    #         not in (
    #             "Betrag",
    #             "BIC_(SWIFT-Code)",
    #             "Kontonummer/IBAN",
    #             "Beguenstigter/Zahlungspflichtiger",
    #             "Sammlerreferenz",
    #             "Kundenreferenz_(End-to-End)",
    #             "Mandatsreferenz",
    #             "Glaeubiger_ID",
    #             "Verwendungszweck",
    #             "Auftragskonto",
    #             "Buchungstag",
    #             "Valutadatum",
    #         )
    #     ]
    #     summary = {}
    #     for header in summary_headers:
    #         summary[header] = summarize_column([row[header] for row in rows])
    # print(summary)


if __name__ == "__main__":
    app()
