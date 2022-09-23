import sqlite3
import typing
from contextlib import closing
from synqlite import crr
import pathlib


def exec(db: sqlite3.Connection, q: str) -> None:
    with closing(db.cursor()) as cursor:
        cursor.execute(q)


def fetch(db: sqlite3.Connection, q: str) -> list[typing.Any]:
    with closing(db.cursor()) as cursor:
        cursor.execute(q)
        return cursor.fetchall()


def test_t(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        exec(a, "CREATE TABLE X(rowid integer PRIMARY KEY);")
        crr.init(a)

        assert len(fetch(a, "SELECT * FROM _synq_local")) == 1


def test_insert_aliased_rowid(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        exec(a, "CREATE TABLE X(x integer PRIMARY KEY)")
        crr.init(a)
        exec(a, "INSERT INTO X VALUES(1)")

        ids = fetch(a, "SELECT row_ts, row_peer FROM _synq_id")
        assert fetch(a, "SELECT rowid FROM X") == [(1,)]
        assert fetch(a, "SELECT rowid FROM _synq_id_X") == [(1,)]
        assert fetch(a, "SELECT row_ts, row_peer FROM _synq_id_X") == ids
        assert fetch(a, "SELECT 1 FROM _synq_log") == []
        assert fetch(a, "SELECT 1 FROM _synq_fklog") == []


def test_insert_repl_col(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        exec(a, "CREATE TABLE X(v any)")
        crr.init(a)
        exec(a, "INSERT INTO X VALUES('v1')")

        ids = fetch(a, "SELECT row_ts, row_peer FROM _synq_id")
        assert fetch(a, "SELECT row_ts, row_peer FROM _synq_id_X") == ids
        assert fetch(a, "SELECT row_ts, row_peer FROM _synq_log") == ids
        assert fetch(a, "SELECT val, tbl_index FROM _synq_log") == [("v1", None)]
        assert fetch(a, "SELECT 1 FROM _synq_fklog") == []


def test_clone_to(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b:
        exec(a, "CREATE TABLE X(rowid integer PRIMARY KEY);")
        crr.init(a)
        crr.clone_to(a, b)

        assert fetch(a, "SELECT peer FROM _synq_local") != fetch(
            b, "SELECT peer FROM _synq_local"
        )
