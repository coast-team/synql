from contextlib import closing
import typing
from dataclasses import dataclass, field
from sqlschm import sql
from synqlite import crr
import pysqlite3 as sqlite3


Ts = tuple[int | None, int | None]  # (ts, peer)


@dataclass(frozen=True, kw_only=True, slots=True)
class Col:
    ts: Ts
    row: Ts
    col: int
    val: typing.Any


@dataclass(frozen=True, kw_only=True, slots=True)
class Ref:
    ts: Ts
    row: Ts
    fk: int
    target: Ts


@dataclass(frozen=True, kw_only=True, slots=True)
class Undo:
    ts: Ts
    obj: Ts
    ul: int


@dataclass(frozen=True, kw_only=True, slots=True)
class Crr:
    tbls: dict[str, set[typing.Any]]
    ctx: dict[int, int]
    log: set[Col | Ref | Undo]


def exec(db: sqlite3.Connection, q: str) -> None:
    with closing(db.cursor()) as cursor:
        cursor.execute(q)
    db.commit()


def fetch(db: sqlite3.Connection, q: str) -> list[typing.Any]:
    with closing(db.cursor()) as cursor:
        cursor.execute(q)
        return cursor.fetchall()


_SELECT_AR_TABLE_NAME = """--sql
SELECT name FROM sqlite_master WHERE type = 'table' AND
    name NOT LIKE 'sqlite_%' AND name NOT LIKE '_synq_%';
"""


def crr_from(db: sqlite3.Connection) -> Crr:
    tbl_names = fetch(db, _SELECT_AR_TABLE_NAME)
    tbls = {}
    for t in tbl_names:
        tbl_name = t[0]
        tbls[tbl_name] = set(
            r[:-2] + ((r[-2], r[-1]),)
            for r in fetch(
                db,
                f'SELECT tbl.*, id.row_ts, id.row_peer FROM "{tbl_name}" tbl JOIN "_synq_id_{tbl_name}" AS id ON tbl.rowid = id.rowid',
            )
        )
    log = (
        {
            Col(ts=(ts, peer), row=(row_ts, row_peer), col=col, val=val)
            for ts, peer, row_ts, row_peer, col, val in fetch(
                db, "SELECT ts, peer, row_ts, row_peer, col, val FROM _synq_log"
            )
        }
        .union(
            {
                Ref(
                    ts=(ts, peer),
                    row=(row_ts, row_peer),
                    fk=fk_id,
                    target=(frow_ts, frow_peer),
                )
                for ts, peer, row_ts, row_peer, fk_id, frow_ts, frow_peer in fetch(
                    db,
                    "SELECT ts, peer, row_ts, row_peer, fk_id, foreign_row_ts, foreign_row_peer FROM _synq_fklog",
                )
            }
        )
        .union(
            {
                Undo(ts=(ts, peer), obj=(obj_ts, obj_peer), ul=ul)
                for ts, peer, obj_ts, obj_peer, ul in fetch(
                    db, "SELECT ts, peer, obj_ts, obj_peer, ul FROM _synq_undolog"
                )
            }
        )
    )
    ctx = {k: v for k, v in fetch(db, "SELECT peer, ts FROM _synq_context")}
    return Crr(tbls=tbls, ctx=ctx, log=log)
