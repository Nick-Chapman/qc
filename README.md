# qc

Explore query compilation via staged interpretation.

## links
- [functional pearl](https://www.cs.purdue.edu/homes/rompf/papers/rompf-icfp15.pdf)
- [longer form paper](https://namin.seas.harvard.edu/files/namin/files/sql2c_jfp.pdf)
- [talk at ICFP 2015](https://www.youtube.com/watch?v=kGuVlTfoZIY)

## done
- represent tables, schema and records
- query plan: scan file, project-as, filter
- parse CVS files (including quoted fields)
- example data: uk mps
- simple recursive evaluation

## todo
- push-based evaluation
- query plan operators: join, group-by, hash-join
- simple parser for SQL -> query plan
- staged interpreter; generated code, C?
- types: string and ints
