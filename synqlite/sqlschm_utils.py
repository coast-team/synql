from sqlschm import sql
from typing import Iterable


Symbols = dict[str, sql.Table]


def symbols(schema: sql.Schema) -> Symbols:
    return dict((tbl.name[0], tbl) for tbl in schema.tables)


"""In SQLite ROWID is the primary key when no primary key is declared"""
DEFAULT_PRIMARY_KEY = sql.Uniqueness(columns=tuple(["rowid"]), is_primary=True)


def primary_key(tbl: sql.Table) -> sql.Uniqueness:
    pks = [
        pk for pk in tbl.constraints if isinstance(pk, sql.Uniqueness) and pk.is_primary
    ]
    if len(pks) != 0:
        return pks[0]
    else:
        return DEFAULT_PRIMARY_KEY


def foreign_keys(tbl: sql.Table) -> Iterable[sql.ForeignKey]:
    return (fk for fk in tbl.constraints if isinstance(fk, sql.ForeignKey))


def foreign_column_names(tbl: sql.Table) -> frozenset[str]:
    return frozenset(col for fk in foreign_keys(tbl) for col in fk.columns)


def is_generated(col: sql.Column) -> bool:
    return col.autoincrement or col.generated


def replicated_columns(tbl: sql.Table) -> list[sql.Column]:
    foreign_col_names = foreign_column_names(tbl)
    return [
        col
        for col in tbl.columns
        if not is_generated(col) and col.name not in foreign_col_names
    ]
