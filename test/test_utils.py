# Copyright (c) 2022 Inria, Victorien Elvinger
# Licensed under the MIT License (https://mit-license.org/)

"""Utilities to write concise and expressive tests."""

from contextlib import closing
import typing
from dataclasses import dataclass
import pysqlite3 as sqlite3


Ts = tuple[int | None, int | None]  # (ts, peer)


@dataclass(frozen=True, kw_only=True, slots=True)
class Val:
    """Represents a log entry in the database. This can be a foreign key or an regular column."""

    ts: Ts
    row: Ts
    name: str | int
    val: typing.Any


@dataclass(frozen=True, kw_only=True, slots=True)
class Undo:
    """Represents an undo entry in the database."""

    ts: Ts
    obj: Ts
    ul: int


@dataclass(frozen=True, kw_only=True, slots=True)
class Crr:
    """Represents a database state."""

    tbls: dict[str, set[typing.Any]]
    ctx: dict[int, int]
    log: set[Val | Undo]


def execute(db: sqlite3.Connection, sql_command: str, /) -> None:
    """Execute `sql_command` on `db`"""
    with closing(db.cursor()) as cursor:
        cursor.execute(sql_command)
    db.commit()


def fetch(db: sqlite3.Connection, sql_query: str, /) -> list[typing.Any]:
    """Execute `sql_query` on `db` and returns the resulting rows."""
    with closing(db.cursor()) as cursor:
        cursor.execute(sql_query)
        return cursor.fetchall()


_SELECT_USER_TABLE_NAME = """--sql
SELECT name FROM sqlite_master WHERE type = 'table' AND
    name NOT LIKE 'sqlite_%' AND name NOT LIKE '_synql_%';
"""


def crr_from(db: sqlite3.Connection, /) -> Crr:
    """Returns a simplified view of the database state."""
    tbl_name_rows = fetch(db, _SELECT_USER_TABLE_NAME)
    tbls = {}
    for tbl_name_row in tbl_name_rows:
        tbl_name = tbl_name_row[0]
        tbls[tbl_name] = set(
            r[:-2] + ((r[-2], r[-1]),)
            for r in fetch(
                db,
                f"""SELECT tbl.*, id.row_ts, id.row_peer
                FROM "{tbl_name}" tbl JOIN "_synql_id_{tbl_name}" AS id ON tbl.rowid = id.rowid""",
            )
        )
    log = (
        {
            Val(ts=(ts, peer), row=(row_ts, row_peer), name=name, val=val)
            for ts, peer, row_ts, row_peer, name, val in fetch(
                db,
                """
                SELECT ts, peer, row_ts, row_peer, ifnull(name, field), val
                FROM _synql_log_extra LEFT JOIN _synql_names
                    ON field = id
                """,
            )
        }
        .union(
            {
                Val(
                    ts=(ts, peer),
                    row=(row_ts, row_peer),
                    name=name,
                    val=(frow_ts, frow_peer),
                )
                for ts, peer, row_ts, row_peer, name, frow_ts, frow_peer in fetch(
                    db,
                    """
                    SELECT ts, peer, row_ts, row_peer, ifnull(name, field),
                        foreign_row_ts, foreign_row_peer 
                    FROM _synql_fklog_extra LEFT JOIN _synql_names
                        ON field = id
                    """,
                )
            }
        )
        .union(
            {
                Undo(ts=(ts, peer), obj=(obj_ts, obj_peer), ul=ul)
                for ts, peer, obj_ts, obj_peer, ul in fetch(
                    db,
                    """
                    SELECT ts, peer, obj_ts, obj_peer, ul FROM _synql_undolog
                    UNION
                    SELECT ts, peer, row_ts, row_peer, ul FROM _synql_id_undo
                    """,
                )
            }
        )
    )
    ctx = dict(fetch(db, "SELECT peer, ts FROM _synql_context"))
    return Crr(tbls=tbls, ctx=ctx, log=log)
