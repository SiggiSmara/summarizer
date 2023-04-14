from typing import List, Tuple, Optional
from datetime import datetime
import typer
import enum
import polars as pl
from data_model import (
    Transaction,
    TransactionDetail,
    TransactionDetailType,
    TransactionType,
)
from db import engine
from column_mapping import columnMapping, MappedCols
from  handle_csv import handleCSV, csvParams

app = typer.Typer()

if __name__ == "__main__":
    # app()
    my_mapping = columnMapping(
        amount_col="betrag",
        booking_date_col="buchungstag",
        value_date_col="valutadatum",
        category_col=None,
        date_format_str="%d.%m.%y",
    )

    my_csvparam = csvParams(separator=";", encoding="iso8859-1")

    # mydata = handleCSV(
    #     file_path="20230414-101179311-umsatz.CSV",
    #     csv_params=my_csvparam,
    #     column_mapping=my_mapping,
    #     db_engine=engine
    # )
    mydata = handleCSV(
        file_path="duplicates.CSV",
        csv_params=my_csvparam,
        column_mapping=my_mapping,
        db_engine=engine
    )
    print(mydata.df.head(5))
    # mydata.insert_data()
