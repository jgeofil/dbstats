from typing import List
import psycopg2

def output_list(file_path: str, list: List):
	with open(file_path, "w+") as fout:
		for val in list:
			fout.write("{0}\n".format(val))

def output_rows(file_path: str, cursor: psycopg2._psycopg.cursor):
	with open(file_path, "w+") as fout:
		for row in cursor:
			fout.write('\t'.join([str(x) for x in row])+'\n')