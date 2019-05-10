select_fields_from_table = """SELECT {0} FROM {1} TABLESAMPLE SYSTEM({2});"""

select_fields_from_table_where = """SELECT {0} FROM {1} TABLESAMPLE SYSTEM ({4}) WHERE {2}='{3}';"""

select_distinct_values = """SELECT DISTINCT {0} FROM {1};"""

select_distinct_values_where = """SELECT DISTINCT {0} FROM {1} WHERE {2}='{3}';"""
