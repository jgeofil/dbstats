import psycopg2
import yaml
import os
from models import ReferenceMatrix, DumpRows

with open('config.yaml') as fin:
	config = yaml.load(fin)

OUT_PATH = config['outPath']

if not os.path.exists(OUT_PATH):
	os.makedirs(OUT_PATH)


conn = psycopg2.connect(dbname=config['dbname'],
						user=config['username'],
						password=config['password'],
						host=config['host'])


DumpRows(OUT_PATH, conn, 'i2b2demodata.relation_type', ['id', 'label']).run()

DumpRows(OUT_PATH, conn, 'i2b2demodata.study', ['study_num', 'study_id', 'study_blob']).run()

DumpRows(OUT_PATH, conn, 'i2b2demodata.patient_dimension', ['sex_cd']).run()

DumpRows(OUT_PATH, conn, 'i2b2demodata.concept_dimension', ['concept_cd', 'concept_path', 'name_char']).run()

DumpRows(OUT_PATH, conn, 'i2b2demodata.modifier_dimension', ['modifier_cd', 'modifier_path', 'name_char']).run()

DumpRows(OUT_PATH, conn, 'i2b2demodata.trial_visit_dimension', ['trial_visit_num', 'study_num', 'rel_time_label']).run()

model = ReferenceMatrix(OUT_PATH, conn, 'i2b2demodata.observation_fact',
	['patient_num', 'concept_cd', 'modifier_cd', 'trial_visit_num'])
model.ignore_key('patient_num')
model.run(0.1)
model.run(0.25)
model.run(0.5)
model.run(0.75)

ReferenceMatrix(OUT_PATH, conn, 'i2b2demodata.observation_fact',
	['tval_char', 'nval_num'], group_by='concept_cd').run()

model = ReferenceMatrix(OUT_PATH, conn, 'i2b2demodata.relation',
	['left_subject_id', 'relation_type_id', 'right_subject_id'])
model.ignore_key('left_subject_id')
model.ignore_key('right_subject_id')
model.run()

ReferenceMatrix(OUT_PATH, conn, 'i2b2demodata.relation',
	['share_household', 'biological'], group_by='relation_type_id').run()

model = ReferenceMatrix(OUT_PATH, conn, 'i2b2demodata.observation_fact',
	['patient_num', 'concept_cd', 'start_date'])
model.ignore_key('patient_num')
model.run()

