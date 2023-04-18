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
from sqlalchemy import select, insert, and_, or_, text, func
from sqlalchemy.orm import Session
from column_mapping import columnMapping, MappedCols
from  handle_csv import handleCSV, csvParams

app = typer.Typer()

@app.command()
def summarize_transaction_detail():
    stmt = select(
        TransactionDetailType.label,
        TransactionDetail.description,
        func.count()
    ).join(
        TransactionDetail.transaction_detail_type
    ).group_by(
        TransactionDetailType.label,
        TransactionDetail.description
    )
    # print(stmt)
    with Session(engine) as conn:
        summary = {}
        detail = {}
        for row in conn.execute(stmt).all():
            if row._mapping["label"] not in summary:
                summary[row._mapping["label"]] = {"tot":0, "distinct":0}
                detail[row._mapping["label"]] = {}
            summary[row._mapping["label"]]["distinct"] += 1
            summary[row._mapping["label"]]["tot"] += row._mapping["count"]
            detail[row._mapping["label"]][row._mapping["description"]] = row._mapping["count"]
        for label, cnt in summary.items():
            # not_labels = ["kontonummer/iban", "bic_(swift-code)"]
            # not_labels = []  label not in not_labels and
            if  cnt["tot"] > 1000 and cnt["distinct"] < 400 and cnt["distinct"] > 2:
                print(f"{label}:all:{cnt}")
                # for desc, cnt2 in detail[label].items():
                #     print(f"{label}:{desc}:{cnt2}")

if __name__ == "__main__":
    app()
    # my_mapping = columnMapping(
    #     amount_col="betrag",
    #     booking_date_col="buchungstag",
    #     value_date_col="valutadatum",
    #     category_col=None,
    #     date_format_str="%d.%m.%y",
    # )

    # my_csvparam = csvParams(separator=";", encoding="iso8859-1")

    # # mydata = handleCSV(
    # #     file_path="20230414-101179311-umsatz.CSV",
    # #     csv_params=my_csvparam,
    # #     column_mapping=my_mapping,
    # #     db_engine=engine
    # # )
    # mydata = handleCSV(
    #     file_path="duplicates.CSV",
    #     csv_params=my_csvparam,
    #     column_mapping=my_mapping,
    #     db_engine=engine
    # )
    # print(mydata.df.head(5))
    # # mydata.insert_data()
