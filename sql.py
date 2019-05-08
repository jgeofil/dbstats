select_fields_from_table = """SELECT {0} FROM {1};"""

select_distinct_values = """SELECT DISTINCT {0} FROM {1};"""

count_concept_occurrences = """SELECT COUNT({0}), {0} AS some_alias FROM {1} WHERE concept_cd='{2}' GROUP BY {0};"""

get_concept_stats = """SELECT MIN({0}), MAX({0}), stddev({0}) AS some_alias FROM {1} WHERE concept_cd='{2}';"""