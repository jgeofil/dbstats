import psycopg2
import yaml
import os
import csv
from models import ReferenceMatrix, DumpRows

selectFrom = """SELECT {0} FROM {1};"""
countDistinct = """SELECT COUNT({0}), {0} AS some_alias FROM {1} WHERE concept_cd='{2}' GROUP BY {0};"""
minMaxDev = """SELECT MIN({0}), MAX({0}), stddev({0}) AS some_alias FROM {1} WHERE concept_cd='{2}';"""

with open('config.yaml') as fin:
	config = yaml.load(fin)

OUT_PATH = config['outPath']

if not os.path.exists(OUT_PATH):
	os.makedirs(OUT_PATH)

OBS_PATH = os.path.join(OUT_PATH, 'obs')
if not os.path.exists(OBS_PATH):
	os.makedirs(OBS_PATH)

conn = psycopg2.connect(dbname=config['dbname'],
						user=config['username'],
						password=config['password'],
						host=config['host'])


def outputTuples(tableName, tuples):
	with open(tableName, 'w+') as fout:
		writer = csv.writer(fout, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)
		for t in tuples:
			writer.writerow(t)

def outputTuple(tableName, tuple):
	with open(tableName, 'w+') as fout:
		for val in tuple:
			fout.write("{0}\n".format(val))


def allRows(tableName, fields, cursor, output=False):
	comm = selectFrom.format(', '.join(fields), tableName)
	cursor.execute(comm)
	data = cursor.fetchall()
	if output:
		outputTuples(os.path.join(OUT_PATH, tableName+'.tsv'), data)
	return data

def distinctCount(tableName, fieldName, conceptCd, cursor):
	comm = countDistinct.format(fieldName, tableName, conceptCd)
	cursor.execute(comm)
	data = cursor.fetchall()
	if len(data) > 1:
		outputTuples(os.path.join(OBS_PATH, conceptCd+'-'+fieldName), data)


cur = conn.cursor()

allRows('i2b2demodata.relation_type', ('id', 'label'), cur, True)
allRows('i2b2demodata.study', ('study_num', 'study_id', 'study_blob'), cur, True)

patients = allRows('i2b2demodata.patient_dimension', ('patient_num', 'sex_cd'), cur, True)
patientIds = {str(val[0]): i for i, val in enumerate(patients)}

concepts = allRows('i2b2demodata.concept_dimension', ('concept_cd', 'concept_path', 'name_char'), cur, True)
conceptIds = {str(val[0]): i for i, val in enumerate(concepts)}

modifiers = allRows('i2b2demodata.modifier_dimension', ('modifier_cd', 'modifier_path', 'name_char'), cur, True)
modifierIds = {str(val[0]): i for i, val in enumerate(modifiers)}
modifierIds["@"] = len(modifierIds)

visits = allRows('i2b2demodata.trial_visit_dimension', ('trial_visit_num', 'study_num', 'rel_time_label'), cur, True)
visitIds = {str(val[0]): i for i, val in enumerate(visits)}
visitIds["@"] = len(visitIds)


DumpRows(
	OUT_PATH,
	conn,
	'i2b2demodata.relation_type',
	['id', 'label']
).run()

ReferenceMatrix(
	OUT_PATH, conn,
	'i2b2demodata.observation_fact',
	['patient_num', 'concept_cd', 'modifier_cd', 'trial_visit_num']
).run()

# Get concept distributions
for conceptCode in conceptIds:
	distinctCount('i2b2demodata.observation_fact', 'tval_char', conceptCode, cur)
	distinctCount('i2b2demodata.observation_fact', 'nval_num', conceptCode, cur)
