# Copyright (c) 2022 Victorien Elvinger
# Licensed under the MIT License (https://mit-license.org)

from dataclasses import dataclass
from sqlschm import sql
import typing


def foreign_column_names(tbl: sql.Table) -> frozenset[str]:
    return frozenset(col for fk in tbl.foreign_keys() for col in fk.columns)


def rowid_aliases(tbl: sql.Table) -> tuple[str, ...]:
    return tuple(
        ({"rowid", "_rowid_", "oid"} - {col.name for col in tbl.columns}).union(
            {col.name for col in tbl.columns if is_rowid_alias(col, tbl.primary_key())}
        )
    )


def has_rowid_alias(tbl: sql.Table) -> bool:
    pk = tbl.primary_key()
    return not tbl.options.without_rowid and any(
        is_rowid_alias(col, pk) for col in tbl.columns
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


def replicated_columns(tbl: sql.Table) -> typing.Iterable[sql.Column]:
    foreign_col_names = foreign_column_names(tbl)
    pk = tbl.primary_key()
    return (
        col
        for col in tbl.non_generated_columns()
        if not is_rowid_alias(col, pk) and col.name not in foreign_col_names
    )


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
