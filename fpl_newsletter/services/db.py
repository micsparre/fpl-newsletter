import pandas as pd
from .sql import get_df_from_table


class Table:
    def __init__(self) -> None:
        pass

    def get_df(self) -> pd.DataFrame:
        df = get_df_from_table(self.table_name)
        return df

    def get_df_as_list(self) -> list:
        df = self.get_df()
        df_list = df.to_dict("records")
        return df_list


class Players(Table):

    def __init__(self) -> None:
        self.table_name = "players"


class Newsletter(Table):

    def __init__(self) -> None:
        self.table_name = "newsletter"


class Subcribers(Table):

    def __init__(self) -> None:
        self.table_name = "subscribers"


class Owners(Table):

    def __init__(self) -> None:
        self.table_name = "owners"
