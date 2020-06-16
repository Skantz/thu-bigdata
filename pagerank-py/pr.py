import sys
import numpy as np
#import scipy
from scipy.sparse import csr_matrix, lil_matrix
import random
from random import randint
import pickle
import time
import matplotlib.pyplot as pyplot
from math import log2

DAMP = 0.85
DANGLING_RANDOM_NBORS = 20
EPS = 0.0001

inp_dict = {}
n_nbors  = {}

all_start = time.time()
with open(sys.argv[1]) as f:
    for line in f:
        line = line.split()
        assert(len(line) == 2)
        s, t = int(line[0]), int(line[1])
        try:
            inp_dict[s].append(t)
        except KeyError:
            inp_dict[s] = [t]

ITER = int(sys.argv[2])

keys = list(inp_dict.keys())
keys.sort()
#assert(keys[0] == 0)
n = max([k for k in keys] + [e for lst_ in [lst for lst in inp_dict.values()] for e in lst_]) + 1

missing = []

for i in range(n):
    if i not in inp_dict:
        inp_dict[i] = []
        missing += [i]
        n_nbors[i] = 0
    n_nbors[i] = len(inp_dict[i])

print("create empty matrix")

matrix = lil_matrix((n, n))
print("m shape", matrix.shape)

print("fill matrix")
for u in inp_dict:
    nbors = inp_dict[u]
    #for v in nb:
    #n_nbors = len(nbors)
    for v in nbors:
        matrix[v, u] = 1. / n_nbors[u]
        #if u in inp_dict[v]:
        #matrix[u, v] = 1. / sum([1 for w in nbors ])#inp_dict[v]])
        #if inp_dict[v] == []:
        #    missing += [v]

non_dangling = [i for i in range(n) if i not in missing]
#print(non_dangling)
#print(random.choices(non_dangling, k=4))
for i in missing:
    for j in [random.sample(non_dangling, k=min(n, DANGLING_RANDOM_NBORS))]:
        #print(i, j)
        #print(matrix[i, :], "matrix i, :")
        #print(matrix[j ,:], "matrix j, :")
        #assert( matrix[j, i] == 0 )
        matrix[j, i] = 1/(min(DANGLING_RANDOM_NBORS, n))


if n < 1000:
    print(matrix)

matrix = csr_matrix(matrix)

vec = np.matrix([1./n for _ in range(n)]).T
vec_saved = vec
print("start solving")
b = time.time()
for i in range(ITER):
    vec = matrix * vec * DAMP + ((1 - DAMP) / n)
    vec_saved = vec
    a = time.time()
    print("finish iter i", i+1, "in", a - b)
    b = a


print("total time", time.time() - all_start)
print("iterations", i)
if n <= 100:
    print("output", vec)
print("this should be 1", sum(vec))

with open("prvec.npy", "wb") as f:
    np.save(f, vec)

#vec = [e[0] for e in vec]
#r = min(100, len(vec))
#pyplot.plot(range(r), [log2(e) for e in sorted(vec, reverse=True)[:r]])
