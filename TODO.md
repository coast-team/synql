
## Optimization

- To reduce writes:
  - Upon insert/update pre-determinate some timestamps based on column id, fk_id
    - a single clock update after insert/update

- Allow foreign key of foreign key
  - resolve during merge

- swap peer and ts everywhere (in particular in PK)
  - use ORDER BY instead of GROUP BY in views

- Remove clock and only use the context
  - reduce redundancy and errors :)
  - compute (max of ts) + 1 and max between this and the current unix ts
  - create an index for immediate computing of max?
    thanks to https://www.sqlite.org/optoverview.html#minmax
  - This allows to remove a trigger
