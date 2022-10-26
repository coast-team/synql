from dataclasses import dataclass
from sqlschm import sql
import typing


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
    return list(
        ({"rowid", "_rowid_", "oid"} - {col.name for col in tbl.columns}).union(
            {col.name for col in tbl.columns if is_rowid_alias(col, tbl.primary_key())}
        )
    )


def has_rowid_alias(tbl: sql.Table) -> bool:
    return not tbl.options.without_rowid and any(
        is_rowid_alias(col, tbl.primary_key()) for col in tbl.columns
    )


def is_rowid_alias(col: sql.Column, pk: sql.Uniqueness | None) -> bool:
    # INTEGER PRIMARY KEY are aliases of rowid
    return (
        col.type.name.lower() == "integer"
        and len(col.type.params) == 0
        and pk is not None
        and pk.is_primary
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


def referred_columns(fk: sql.ForeignKey, tables: sql.Symbols) -> tuple[str, ...]:
    f_table = tables[fk.foreign_table.name[0]]
    referred_columns = fk.referred_columns
    if referred_columns is None:
        f_pk = f_table.primary_key()
        assert f_pk is not None
        return tuple(f_pk.columns())
    else:
        return referred_columns


@dataclass(frozen=True, kw_only=True, slots=True)
class FkResolution:
    foreign_key: sql.ForeignKey
    referred: typing.Union["FkResolution", str]


def fk_col_resolution(
    fk: sql.ForeignKey,
    col: str,
    tables: sql.Symbols,
) -> FkResolution:
    assert col in fk.columns
    f_tbl = tables[fk.foreign_table.name[0]]
    f_cols = referred_columns(fk, tables)
    assert len(fk.columns) == len(f_cols)
    f_col = f_cols[fk.columns.index(col)]
    for f_fk in f_tbl.foreign_keys():
        if f_col in f_fk.columns:
            return FkResolution(
                foreign_key=fk, referred=fk_col_resolution(f_fk, f_col, tables)
            )
    return FkResolution(foreign_key=fk, referred=f_col)


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
