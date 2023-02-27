# Copyright (c) 2022 Inria, Victorien Elvinger
# Licensed under the MIT License (https://mit-license.org/)

"""Utilities for sqlschm."""

import typing
from sqlschm import sql


def foreign_column_names(tbl: sql.Table, /) -> frozenset[str]:
    """Set of the columns that are covered by a foreign key."""
    return frozenset(col for fk in tbl.foreign_keys() for col in fk.columns)


def rowid_aliases(tbl: sql.Table, /) -> tuple[str, ...]:
    """Returns the aliases of SQLite `rowid` removing the names used by `table`."""
    primary_key = tbl.primary_key()
    custom_aliases = {
        col.name for col in tbl.columns if is_rowid_alias(col, primary_key)
    }
    return tuple(
        ({"rowid", "_rowid_", "oid"} - {col.name for col in tbl.columns}).union(
            custom_aliases
        )
    )


def has_rowid_alias(tbl: sql.Table, /) -> bool:
    """Has `tbl` a column that is an alias of SQLite `rowid`?"""
    primary_key = tbl.primary_key()
    return not tbl.options.without_rowid and any(
        is_rowid_alias(col, primary_key) for col in tbl.columns
    )


def is_rowid_alias(col: sql.Column, key: sql.Uniqueness | None, /) -> bool:
    """Is `col` an alias of SQLite `rowid`?

    To answer this question the primary key of the table that includes `col` must be passed.
    """
    assert key is None or key.is_primary, "`key` must be a primary key"
    # INTEGER PRIMARY KEY are aliases of rowid
    return (
        key is not None
        and col.type.name.lower() == "integer"
        and len(col.type.params) == 0
        and len(key.indexed) == 1
        and key.indexed[0].column == col.name
        # Edge Case: INTEGER PRIMARY KEY DESC is not an alias of rowid
        # See https://www.sqlite.org/lang_createtable.html#rowids_and_the_integer_primary_key
        and (key.is_table_constraint or key.indexed[0].sorting is not sql.Sorting.DESC)
    )


def replicated_columns(tbl: sql.Table, /) -> typing.Iterable[sql.Column]:
    """Columns that must be replicated by Synql.

    This corresponds to all columns except the generated columns and the aliases of SQLite `rowid`.
    """
    foreign_col_names = foreign_column_names(tbl)
    primary_key = tbl.primary_key()
    return (
        col
        for col in tbl.non_generated_columns()
        if not is_rowid_alias(col, primary_key) and col.name not in foreign_col_names
    )


def ids(
    symbols: sql.Symbols, /
) -> dict[sql.Table | tuple[sql.Table, sql.Column | sql.TableConstraint], int]:
    """Returns a dict that associates a unique natural number to every table, column, constraint"""
    result: dict[
        sql.Table | tuple[sql.Table, sql.Column | sql.TableConstraint], int
    ] = {}
    nth = 0
    for tbl in symbols.values():
        result[tbl] = nth
        nth += 1
        for col in tbl.columns:
            result[(tbl, col)] = nth
            nth += 1
        for cst in tbl.all_constraints():
            result[(tbl, cst)] = nth
            nth += 1
    return result
