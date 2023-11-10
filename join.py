import json
import re
import itertools

with open('data.txt') as f:
    lines = f.readlines()

A_lines = lines[:112]
B_lines = lines[112:]

A = []
B = []

p = re.compile(r'\d+: \((\d+), (\d+)\)')
for line in A_lines:
    if m := p.match(line):
        A.append((int(m.group(1)), int(m.group(2))))

for line in B_lines:
    if m := p.match(line):
        B.append((int(m.group(1)), int(m.group(2))))

# print(len(A), len(B))

# join
C = []
for a, b in itertools.product(A, B):
    if a[0] == b[0]:
        C.append((*a, *b))

C.sort()

# print(len(C))

for i, c in enumerate(C):
    print(f'{i+1}:', *c, sep='\t')
