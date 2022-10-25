from sqlschm import sql

ANY = sql.Type(name="any")
INTEGER = sql.Type(name="integer")

ROWID_COL = sql.Column(name="rowid", type=INTEGER, constraints=tuple())


"""In SQLite ROWID is the primary key when no primary key is declared"""
ROWID_PRIMARY_KEY = sql.Uniqueness(
    indexed=(sql.Indexed(column="rowid"),), is_primary=True
)


def is_temp(tbl: sql.Table) -> bool:
    return tbl.temporary or tbl.name[1:2] == tuple(["temp"])


def primary_key(tbl: sql.Table) -> sql.Uniqueness:
    pk = tbl.primary_key()
    if pk is None:
        pk = ROWID_PRIMARY_KEY
    return pk


def uniqueness(tbl: sql.Table) -> list[sql.Uniqueness]:
    pk = primary_key(tbl)
    result = list(tbl.uniqueness())
    if pk not in result:
        result = [pk] + result
    return result


def foreign_column_names(tbl: sql.Table) -> frozenset[str]:
    return frozenset(col for fk in tbl.foreign_keys() for col in fk.columns)


def is_generated(col: sql.Column) -> bool:
    return any(
        x
        for x in col.constraints
        if isinstance(x, sql.Generated)
        or (
            isinstance(x, sql.Uniqueness)
            and (x.autoincrement or is_rowid_alias(col, x))
        )
    )


def rowid_aliases(tbl: sql.Table) -> list[str]:
    pk = primary_key(tbl)
    return list(
        ({"rowid", "_rowid_", "oid"} - {col.name for col in tbl.columns}).union(
            {col.name for col in tbl.columns if is_rowid_alias(col, pk)}
        )
    )


def has_rowid_alias(tbl: sql.Table) -> bool:
    pk = primary_key(tbl)
    return not tbl.options.without_rowid and any(
        is_rowid_alias(col, pk) for col in tbl.columns
    )


def is_rowid_alias(col: sql.Column, pk: sql.Uniqueness) -> bool:
    # INTEGER PRIMARY KEY are aliases of rowid
    return (
        col.type.name.lower() == "integer"
        and len(col.type.params) == 0
        and len(pk.indexed) == 1
        and pk.indexed[0].column == col.name
        # Edge case: INTEGER PRIMARY KEY DESC is not an alias of rowid
        # See https://www.sqlite.org/lang_createtable.html#rowids_and_the_integer_primary_key
        and (pk.is_table_constraint or pk.indexed[0].sorting is not sql.Sorting.DESC)
    )


def replicated_columns(tbl: sql.Table) -> list[sql.Column]:
    foreign_col_names = foreign_column_names(tbl)
    return [
        col
        for col in tbl.columns
        if not is_generated(col) and col.name not in foreign_col_names
    ]


def ids(
    symbols: sql.Symbols,
) -> dict[sql.Table | tuple[sql.Table, sql.Column | sql.TableConstraint], int]:
    result: dict[
        sql.Table | tuple[sql.Table, sql.Column | sql.TableConstraint], int
    ] = {}
    id = 0
    for tbl in symbols.values():
        result[tbl] = id
        id += 1
        for col in tbl.columns:
            result[(tbl, col)] = id
            id += 1
        for cst in tbl.all_constraints():
            result[(tbl, cst)] = id
            id += 1
    return result


def cols(
    tbl: sql.Table,
) -> dict[str, sql.Column]:
    return {col.name: col for col in tbl.columns}
