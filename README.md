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
- slow (quadratic) join
- hash-join (kinda - uses log-maps, but avoids being quadratic which is what is most important)
- group-by (separate aggregate & expand)
- push-based evaluation (step to compile)
- materialize (for quad join & push-based evaluation) -- removed
- introduce Interaction type as result of runAction
- regain lazy gen/print for slow quadratic join
- make Action be a first order type
- unique name gen when compiling to Action
- types for record/value/boolean expressions, and record ids. +pp
- evalAction in env mapping rid -> record(value)

## todo
- represent Records as Map ColName Value
- move remaining example to push-based-eval/compile
- simple parser for SQL -> query plan
- staged interpreter; generated code, C?
- types: string and ints
