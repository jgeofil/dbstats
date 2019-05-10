import numpy as np
import sparse


mat = sparse.load_npz('/home/jeremy/repos/benchmark/dbsynth/out/reference_matrix/i2b2demodata.observation_fact/patient_num-concept_cd-start_date/dimension_matrix.npz')
shape = mat.shape

mat = mat.sum(0)
mat = mat.sum(0)

mat = [x for x in mat]

print(mat)