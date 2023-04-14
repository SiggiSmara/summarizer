import enum

class MappedCols(enum.Enum):
    amount_col =  "amount"
    booking_date_col =  "booking_date"
    value_date_col = "value_date"
    category_col = "category"
    tr_type_col = "tr_type"

class columnMapping():
    """
    Class that holds the parameters for generating the transaction
    object and related info.
    """

    def __init__(
        self,
        amount_col: str,
        booking_date_col: str,
        value_date_col: str,
        category_col: str,
        date_format_str: str,
    ):
        self.amount_col = amount_col
        self.booking_date_col = booking_date_col
        self.value_date_col = value_date_col
        self.category_col = category_col
        self.date_format_str = date_format_str
        self.col_list = [self.amount_col, self.booking_date_col, self.value_date_col]

    def clean_name(self, col_name: str) -> str:
        """
        Forces the name to lower case and replaces any space character with '_'

        Args:
            col_name (str): the original name

        Returns:
            str: the cleaned name
        """
        return col_name.strip().lower().replace(" ", "_")

    def get_mapped_name(self, col_name: str) -> str:
        """
        Calls self.clean_name and after that checks for any of the mapped names.
        If mapped names are found then return the mapped name instead.

        Args:
            col_name (str): the original name

        Returns:
            str: the cleaned and possibly mapped name
        """
        new_name = self.clean_name(col_name=col_name)
        if new_name == self.amount_col:
            new_name = MappedCols.amount_col.value
        elif new_name == self.booking_date_col:
            new_name = MappedCols.booking_date_col.value
        elif new_name == self.value_date_col:
            new_name = MappedCols.value_date_col.value
        elif new_name == self.category_col:
            new_name = MappedCols.category_col.value
        return new_name
