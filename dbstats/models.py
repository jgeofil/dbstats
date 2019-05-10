import sparse, psycopg2, os
from typing import List
from tqdm import tqdm
import numpy as np
from .sql import select_fields_from_table, select_fields_from_table_where, select_distinct_values, select_distinct_values_where


class Model:

	count_ = 0

	def __init__(self, out_path: str, connection: psycopg2._psycopg.connection, table_name: str, fields: List[str], out_folder: str):

		print('***********************************************')
		print('Model {0}'.format(out_folder))

		self.fields_ = fields
		self.table_name_ = table_name
		self.ignore_keys_ = []

		self.connection_ = connection

		self.folder_name_ = out_folder
		self.output_path_ = os.path.join(out_path, out_folder, table_name)
		self.makedir(self.output_path_)
		self.log_path_ = os.path.join(self.output_path_, 'error.log')

		print('Table: {0}'.format(self.table_name_))
		print('Fields: {0}'.format('\t'.join(self.fields_)))

	@staticmethod
	def makedir(path: str):
		if not os.path.exists(path):
			os.makedirs(path)

	@staticmethod
	def output_list(file_path: str, list: List):
		with open(file_path, "w+") as fout:
			for val in list:
				fout.write("{0}\n".format(val))

	def output_rows(self, file_path: str, cursor: psycopg2._psycopg.cursor):
		with open(file_path, "w+") as fout:
			for row in cursor:
				fout.write('\t'.join(self.stringify(row)) + '\n')

	@staticmethod
	def stringify(row: List):
		return [str(x) for x in row]

	def log(self, low_row):
		with open(self.log_path_, 'a+') as flog:
			flog.write(low_row+'\n')

	def get_named_cursor(self):
		Model.count_ += 1
		return self.connection_.cursor(self.folder_name_ + str(Model.count_))


class ReferenceMatrix(Model):

	def __init__(self, out_path: str, connection: psycopg2._psycopg.connection, table_name: str, fields: List[str], group_by=None):

		Model.__init__(self, out_path, connection, table_name, fields, 'reference_matrix')

		self.cube = sparse.DOK(1)
		self.dimension_keys = {}
		self.output_path_ = os.path.join(self.output_path_, '-'.join(self.fields_))
		self.makedir(self.output_path_)
		self.group_by_ = group_by

	def ignore_key(self, key: str):
		self.ignore_keys_.append(key)

	@staticmethod
	def rows_to_index(rows: List):
		return {str(val): i for i, val in enumerate(rows)}

	def _get_shape(self, out_path: str, where=None):
		shape = []
		for field in self.fields_:

			if where and self.group_by_:
				query = select_distinct_values_where.format(field, self.table_name_, self.group_by_, where)
			else:
				query = select_distinct_values.format(field, self.table_name_)

			cursor = self.get_named_cursor()
			cursor.execute(query)
			result = [self.stringify(row)[0] for row in cursor]

			if field not in self.ignore_keys_:
				self.output_list(os.path.join(out_path, field), result)

			shape.append(len(result))
			self.dimension_keys[field] = self.rows_to_index(result)

		return tuple(shape)

	def _get_indices(self, row: List):
		return [self.dimension_keys[field][val] for field, val in zip(self.fields_, row)]

	def _populate(self, cursor: psycopg2._psycopg.cursor, out_path: str, desc=None, ratio=1.0):

		with tqdm(total=cursor.rowcount, desc=desc) as pbar:
			for observation in cursor:
				try:
					observation = self.stringify(observation)
					if len(observation) == 2:
						i, j = self._get_indices(observation)
						self.cube[i, j] += 1
					elif len(observation) == 3:
						i, j, k = self._get_indices(observation)
						self.cube[i, j, k] += 1
					elif len(observation) == 4:
						i, j, k, l = self._get_indices(observation)
						self.cube[i, j, k, l] += 1
					elif len(observation) == 5:
						i, j, k, l, m = self._get_indices(observation)
						self.cube[i, j, k, l, m] += 1
				except:
					with open(os.path.join(self.output_path_, out_path, 'error.log'), 'a') as log:
						log.write("\t".join(observation) + "\n")
				pbar.update(1)

		self.cube = self.cube.to_coo()
		sparse.save_npz(os.path.join(out_path, 'dimension_matrix_'+str(ratio).replace('.', '')), self.cube)

	def run(self, ratio=1.0):
		try:
			if self.group_by_:
				query = select_distinct_values.format(self.group_by_, self.table_name_)
				cursor1 = self.get_named_cursor()
				cursor1.execute(query)

				for value in cursor1:
					try:
						value = str(value[0])

						subfolder = os.path.join(self.output_path_, value)
						self.makedir(subfolder)

						self.cube = sparse.DOK(self._get_shape(subfolder, where=value), dtype=np.uint8)

						query = select_fields_from_table_where.format(', '.join(self.fields_), self.table_name_, self.group_by_, value, ratio)
						cursor2 = self.get_named_cursor()
						cursor2.execute(query)

						self._populate(cursor2, subfolder, desc=value, ratio=ratio)
					except:
						self.log('Failed for group {0}'.format(value))

			else:
				self.cube = sparse.DOK(self._get_shape(self.output_path_), dtype=np.uint8)

				query = select_fields_from_table.format(', '.join(self.fields_), self.table_name_, ratio)
				cursor = self.get_named_cursor()
				cursor.execute(query)

				self._populate(cursor, self.output_path_, desc=self.table_name_, ratio=ratio)
		except:
			self.log('Could not run on table {0} for fields {1} and ratio {2}'.format(self.table_name_, ', '.join(self.fields_), ratio))


class DumpRows(Model):

	def __init__(self, out_path: str, connection: psycopg2._psycopg.connection, table_name: str, fields: List[str]):

		Model.__init__(self, out_path, connection, table_name, fields, 'dump_rows')

	def run(self):
		try:
			query = select_fields_from_table.format(', '.join(self.fields_), self.table_name_, 1.0)
			cursor = self.get_named_cursor()
			cursor.execute(query)
			self.output_rows(os.path.join(self.output_path_, self.table_name_), cursor)
		except:
			self.log('Could not run on table {0} for fields {1}'.format(self.table_name_, ', '.join(self.fields_)))

