import pandas as pd
from sql import get_df_from_table

class Table:
    def __init__(self) -> None:
        pass
    
    def get_df(self) -> pd.DataFrame:
        df = get_df_from_table(self.table_name)
        return df
    
class Players(Table):
    
    def __init__(self) -> None:
        self.table_name = "players"
        
    
        