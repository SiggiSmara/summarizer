from typing import List, Tuple, Optional, Dict
from datetime import datetime
import polars as pl
from sqlalchemy import select, insert, and_

from data_model import (
    Transaction,
    TransactionDetail,
    TransactionDetailType,
    TransactionType,
)
from column_mapping import columnMapping, MappedCols


class csvParams:
    """
    Class that holds the parameters for parsing the csv.
    """

    def __init__(self, separator: str, encoding: str):
        self.separator = separator
        self.encoding = encoding


class handleCSV:
    """
    A class that handles reading the csv and writing the result to the database
    """

    def __init__(
        self,
        file_path: str,
        csv_params: csvParams,
        column_mapping: columnMapping,
        db_engine,
    ):
        """
        Initialization

        Args:
            file_path (str): the file path to the csv to be read
            csv_params (csvParams): a class that holds the parameters needed to read the csv file
            column_mapping (columnMapping): a class that holds the column mappings and takes care of the column name cleaning
            db_engine (sqlalchemy engine): the database engine used to save the data
        """
        self.file_path = file_path
        self.csv_params = csv_params
        self.column_mapping = column_mapping
        self.db_engine = db_engine
        self.df = self.read_csv_file()
        self.detail_cols = [
            header
            for header in self.df.columns
            if header not in self.column_mapping.col_list
        ]
        self.detail_mapping = self.sync_detail_types()

    def read_csv_file(self) -> pl.DataFrame:
        """
        Reads a CSV file and returns it as a Polars DataFrame.
        Maps the columns for amount, booking_date and value_date according to column_mapping.
        Cleans the column names so that they don't contain spaces and ensures that they are
        lower case.

        Returns:
            pl.DataFrame: a polars dataframe with the parsed data
        """
        df = pl.read_csv(
            self.file_path,
            separator=self.csv_params.separator,
            encoding=self.csv_params.encoding,
        )

        # Remove spaces from column names and remove case as well
        df.columns = [
            self.column_mapping.get_mapped_name(header) for header in df.columns
        ]

        # cast the mapped columns to their correct types and add the transaction_type column
        df = (
            df.with_columns(
                pl.col(MappedCols.booking_date_col.value).str.strptime(
                    pl.Date, self.column_mapping.date_format_str
                ),
                pl.col(MappedCols.value_date_col.value).str.strptime(
                    pl.Date, self.column_mapping.date_format_str
                ),
                pl.col(MappedCols.amount_col.value)
                .str.replace(",", ".")
                .cast(pl.Float64),
            )
            .with_columns(
                pl.when(pl.col(MappedCols.amount_col.value) < 0)
                .then(TransactionType.debit.value)
                .otherwise(TransactionType.credit.value)
                .alias("tr_type")
            )
            .with_columns(
                pl.when(pl.col(MappedCols.amount_col.value) < 0)
                .then(pl.col(MappedCols.amount_col.value) * (-1))
                .otherwise(pl.col(MappedCols.amount_col.value))
            )
        )

        return df

    def sync_detail_types(self) -> dict:
        """
        Looks up all column names that are not mapped to transaction itself in TransactionDetailType.
        If found stores the id (needed for inserts later), if not found inserts it and retreives the
        inserted id.

        Returns:
            dict: a dictionary with keys as the column names and the id from the data base as the value
        """

        detail_mapping = {}
        ins_stmt = insert(TransactionDetailType).returning(TransactionDetailType.id)
        for one_col in self.detail_cols:
            stmt = select(TransactionDetailType.id).where(
                TransactionDetailType.label == one_col
            )
            with self.db_engine.connect() as conn:
                found_id = conn.execute(stmt).scalar_one_or_none()
                if found_id is None:
                    found_id = conn.execute(
                        ins_stmt, [{"label": one_col, "description": one_col}]
                    ).scalar_one_or_none()
                    if found_id is not None:
                        conn.commit()
                detail_mapping[one_col] = found_id
        return detail_mapping

    def find_possible_duplicate(self, row):
        found_trans = self.find_transactions(row=row)
        return found_trans
        # if len(found_trans) > 0:
        #     found_trans_det = self.find_transaction_details(row=row, ids=found_trans)

        #     if len(found_trans_det):
        #         return list(found_trans_det.keys())
        #     else:
        #         return []

    def find_transactions(self, row: dict) -> List[int]:
        """
        Counts current transactions that contain the same data as found in the row

        Args:
            row (dict): one csv row from incoming data

        Returns:
            List[int]: the list of ids matchin the incoming data
        """
        stmt = select(Transaction.id).where(
            and_(
                Transaction.amount == row[MappedCols.amount_col.value],
                Transaction.booking_date == row[MappedCols.booking_date_col.value],
                Transaction.value_date == row[MappedCols.value_date_col.value],
                Transaction.tr_type == row[MappedCols.tr_type_col.value],
            )
        )

        with self.db_engine.connect() as conn:
            found_ids = conn.execute(stmt).scalar()
            if found_ids is not None:
                found_ids = found_ids.all()
        return found_ids

    def find_transaction_details(self, row: dict, ids: List[int]) -> dict:
        """
        Finds matching transaction details for matching transaction ids.

        Args:
            row (dict): one csv row from incoming data
            ids (List[int]): the list of transaction ids to search for


        Returns:
            dict: the dictionary of lists of transaction detail ids matching the incoming data
        """
        return {}

    def insert_data(self):
        for row in self.df.rows(named=True):
            duplicates = self.find_possible_duplicate(row)
            if duplicates is not None and len(duplicates) != 0:
                # possible duplicates found, deal with it
                print(f"error found duplicates of row: {row}")
                print(duplicates)
            else:
                # all good, lets instert some data
                self.insert_one_row(row=row)

    def insert_one_row(self, row: dict):
        """
        Insert one row

        Args:
            row (dict): one row from the csv
        """
        transaction_id = self.insert_transaction(
            booking_date=row[MappedCols.booking_date_col.value],
            value_date=row[MappedCols.value_date_col.value],
            amount=row[MappedCols.amount_col.value],
            tr_type=row[MappedCols.tr_type_col.value],
        )
        for detail, db_id in self.detail_mapping.items():
            if row[detail]:
                self.insert_trans_detail(
                    transaction_id=transaction_id,
                    transaction_detail_type_id=db_id,
                    description=row[detail],
                )

    def insert_transaction(
        self,
        booking_date: datetime,
        value_date: datetime,
        amount: float,
        tr_type: TransactionType,
    ) -> int:
        """
        Insert one transation

        Args:
            booking_date (datetime): booking_date
            value_date (datetime): value_date
            amount (float): amount
            tr_type (TransactionType): tr_type

        Returns:
            int: the new transaction id
        """
        stmt = insert(Transaction).returning(Transaction.id)
        with self.db_engine.connect() as conn:
            trans_id:int = conn.execute(
                stmt,
                [
                    {
                        "booking_date": booking_date,
                        "value_date": value_date,
                        "amount": amount,
                        "tr_type": tr_type,
                    }
                ],
            ).scalar_one()
            conn.commit()
        return trans_id

    def insert_trans_detail(
        self, transaction_id: int, transaction_detail_type_id: int, description: str
    ) -> int:
        """
        Insert one transaction detail

        Args:
            transaction_id (int): transaction_id
            transaction_detail_type_id (int): transaction_detail_type_id
            description (str): description

        Returns:
            int: the transaction detail id
        """
        stmt = insert(TransactionDetail).returning(TransactionDetail.id)
        with self.db_engine.connect() as conn:
            trans_detail_id = conn.execute(
                stmt,
                [
                    {
                        "transaction_id": transaction_id,
                        "transaction_detail_type_id": transaction_detail_type_id,
                        "description": description,
                    }
                ],
            ).scalar_one()
            conn.commit()
        return trans_detail_id


if __name__ == "__main__":
    from db import engine

    my_mapping = columnMapping(
        amount_col="betrag",
        booking_date_col="buchungstag",
        value_date_col="valutadatum",
        category_col=None,
        date_format_str="%d.%m.%y",
    )

    my_csvparam = csvParams(separator=";", encoding="iso8859-1")

    mydata = handleCSV(
        file_path="20230414-101179311-umsatz.CSV",
        csv_params=my_csvparam,
        column_mapping=my_mapping,
        db_engine=engine,
    )
    print(mydata.df.head(5))
