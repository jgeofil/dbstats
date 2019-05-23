#!/usr/bin/env python

import psycopg2, os, argparse
from dbstats.models import ReferenceMatrix, DumpRows

parser = argparse.ArgumentParser(description='DBstats')
parser.add_argument('--db', help='Database name.', default='transmart')
parser.add_argument('--user', help='Username.', default='postgres')
parser.add_argument('--pswd', help='Password.', default='postgres')
parser.add_argument('--host', help='Database host.', default='localhost')
parser.add_argument('--out', help='Output folder.', default=os.path.join(os.getcwd(), 'out'))
args = parser.parse_args()

OUT_PATH = args.out

if not os.path.exists(OUT_PATH):
	os.makedirs(OUT_PATH)

conn = psycopg2.connect(dbname=args.db,
						user=args.user,
						password=args.pswd,
						host=args.host)

DumpRows(OUT_PATH, conn, 'i2b2demodata.relation_type', ['id', 'label']).run()
DumpRows(OUT_PATH, conn, 'i2b2demodata.study', ['study_num', 'study_id', 'study_blob']).run()
DumpRows(OUT_PATH, conn, 'i2b2demodata.patient_dimension', ['sex_cd']).run()
DumpRows(OUT_PATH, conn, 'i2b2demodata.concept_dimension', ['concept_cd', 'concept_path', 'name_char']).run()
DumpRows(OUT_PATH, conn, 'i2b2demodata.modifier_dimension', ['modifier_cd', 'modifier_path', 'name_char']).run()
DumpRows(OUT_PATH, conn, 'i2b2demodata.trial_visit_dimension', ['trial_visit_num', 'study_num', 'rel_time_label']).run()

model = ReferenceMatrix(OUT_PATH, conn, 'i2b2demodata.relation',
	['left_subject_id', 'relation_type_id', 'right_subject_id'])
model.ignore_key('left_subject_id')
model.ignore_key('right_subject_id')
model.run()

ReferenceMatrix(OUT_PATH, conn, 'i2b2demodata.relation',
	['share_household', 'biological'], group_by='relation_type_id').run()

model1 = ReferenceMatrix(OUT_PATH, conn, 'i2b2demodata.observation_fact',
	['patient_num', 'concept_cd', 'modifier_cd', 'trial_visit_num'])
model1.ignore_key('patient_num')
model1.run(0.1)
model1.run(0.25)

model2 = ReferenceMatrix(OUT_PATH, conn, 'i2b2demodata.observation_fact',
	['patient_num', 'concept_cd', 'start_date'])
model2.ignore_key('patient_num')
model2.run(0.1)
model2.run(0.25)

ReferenceMatrix(OUT_PATH, conn, 'i2b2demodata.observation_fact',
	['tval_char', 'nval_num'], group_by='concept_cd').run()

model1.run(0.5)
model1.run(0.75)
model1.run(1.0)
model2.run(0.5)
model2.run(0.75)
model2.run(1.0)

print('***** Done')