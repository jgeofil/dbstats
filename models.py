import sparse
import psycopg2
from typing import List
from util import output_list, output_rows
import os
import numpy as np
from tqdm import tqdm

from sql import select_fields_from_table, select_distinct_values


class Model:

	count_ = 0

	def __init__(self, out_path: str, connection: psycopg2._psycopg.connection, table_name: str, fields: List[str], out_folder: str):
		print('***********************************************')
		print('Initializing model {0}..'.format(out_folder))
		self.fields_ = fields
		self.table_name_ = table_name
		self.ignore_keys_ = []

		self.cursor_ = connection.cursor()
		self.count_ += 1
		self.named_cursor_ = connection.cursor(out_folder+str(self.count_))

		self.output_path_ = os.path.join(out_path, out_folder, table_name)
		if not os.path.exists(self.output_path_):
			os.makedirs(self.output_path_)

		print('Table: {0}'.format(self.table_name_))
		print('Fields: {0}'.format('\t'.join(self.fields_)))


class ReferenceMatrix(Model):

	def __init__(self, out_path: str, connection: psycopg2._psycopg.connection, table_name: str, fields: List[str]):

		Model.__init__(self, out_path, connection, table_name, fields, 'reference_matrix')

		self.cube = sparse.DOK(1)
		self.dimension_keys = {}

	def ignore_key(self, key: str):
		self.ignore_keys_.append(key)

	@staticmethod
	def rows_to_index(rows: List):
		return {str(val): i for i, val in enumerate(rows)}

	@staticmethod
	def stringify(row: List):
		return [str(x) for x in row]

	def _get_shape(self):
		print('Getting dimensions for {0} fields..'.format(len(self.fields_)))
		shape = []
		for field in self.fields_:
			query = select_distinct_values.format(field, self.table_name_)
			self.cursor_.execute(query)
			result = [self.stringify(row)[0] for row in self.cursor_]
			if not field in self.ignore_keys_:
				output_list(os.path.join(self.output_path_, field), result)
			shape.append(len(result))
			self.dimension_keys[field] = self.rows_to_index(result)
		print('Dimensions: {0}'.format(shape))
		return tuple(shape)

	def _get_indices(self, row: List):
		return [self.dimension_keys[field][val] for field, val in zip(self.fields_, row)]

	def run(self):
		self.cube = sparse.DOK(self._get_shape(), dtype=np.uint8)

		query = select_fields_from_table.format(', '.join(self.fields_), self.table_name_)
		self.named_cursor_.execute(query)

		with tqdm(total=self.named_cursor_.rowcount) as pbar:
			for observation in self.named_cursor_:
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
					with open(os.path.join(self.output_path_, 'error.log'), 'a') as log:
						log.write("\t".join(observation)+"\n")
				pbar.update(1)

		self.cube = self.cube.to_coo()
		sparse.save_npz(os.path.join(self.output_path_, 'dimension_matrix'), self.cube)

class DumpRows(Model):

	OUT_FOLDER = 'dump_rows'

	def __init__(self, out_path: str, connection: psycopg2._psycopg.connection, table_name: str, fields: List[str]):

		Model.__init__(self, out_path, connection, table_name, fields, 'dump_rows')

	def run(self):
		query = select_fields_from_table.format(', '.join(self.fields_), self.table_name_)
		self.named_cursor_.execute(query)
		output_rows(os.path.join(self.output_path_, self.table_name_), self.named_cursor_)