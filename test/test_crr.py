import pysqlite3 as sqlite3
import typing
from contextlib import closing
from synqlite import crr
import pathlib
from sqlschm import sql


def exec(db: sqlite3.Connection, q: str) -> None:
    with closing(db.cursor()) as cursor:
        cursor.execute(q)
    db.commit()


def fetch(db: sqlite3.Connection, q: str) -> list[typing.Any]:
    with closing(db.cursor()) as cursor:
        cursor.execute(q)
        return cursor.fetchall()


def test_crr_init(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        crr.init(a, id=1, ts=False)

        assert fetch(a, "SELECT peer, ts FROM _synq_local") == [(1, 0)]


def test_ins_aliased_rowid(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        exec(a, "CREATE TABLE X(x integer PRIMARY KEY)")
        crr.init(a, id=1, ts=False)
        exec(a, "INSERT INTO X VALUES(1)")

        assert fetch(a, "SELECT rowid FROM X") == [(1,)]
        assert fetch(a, "SELECT rowid, row_ts, row_peer FROM _synq_id_X") == [(1, 1, 1)]
        assert fetch(a, "SELECT row_ts, row_peer FROM _synq_id") == [(1, 1)]
        assert fetch(a, "SELECT peer, ts FROM _synq_context") == [(1, 1)]
        assert fetch(a, "SELECT 1 FROM _synq_log") == []
        assert fetch(a, "SELECT 1 FROM _synq_fklog") == []
        assert fetch(a, "SELECT 1 FROM _synq_undolog") == []


def test_del_aliased_rowid(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        exec(a, "CREATE TABLE X(x integer PRIMARY KEY)")
        crr.init(a, id=1, ts=False)
        exec(a, "INSERT INTO X VALUES(1)")
        exec(a, "DELETE FROM X")

        assert fetch(a, "SELECT * FROM X") == []
        assert fetch(a, "SELECT * FROM _synq_id_X") == []
        assert fetch(a, "SELECT row_ts, row_peer FROM _synq_id") == [(1, 1)]
        assert fetch(a, "SELECT peer, ts FROM _synq_context") == [(1, 2)]
        assert fetch(a, "SELECT ts, peer, obj_ts, obj_peer, ul FROM _synq_undolog") == [
            (2, 1, 1, 1, 1)
        ]
        assert fetch(a, "SELECT * FROM _synq_log") == []
        assert fetch(a, "SELECT * FROM _synq_fklog") == []
        assert fetch(a, "SELECT ts, peer, obj_ts, obj_peer FROM _synq_undolog") == [
            (2, 1, 1, 1)
        ]


def test_up_aliased_rowid(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        exec(a, "CREATE TABLE X(x integer PRIMARY KEY)")
        crr.init(a, id=1, ts=False)
        exec(a, "INSERT INTO X VALUES(1)")
        exec(a, "UPDATE X SET x = 2")

        assert fetch(a, "SELECT rowid FROM X") == [(2,)]
        assert fetch(a, "SELECT rowid, row_ts, row_peer FROM _synq_id_X") == [(2, 1, 1)]
        assert fetch(a, "SELECT row_ts, row_peer FROM _synq_id") == [(1, 1)]
        assert fetch(a, "SELECT peer, ts FROM _synq_context") == [(1, 1)]
        assert fetch(a, "SELECT 1 FROM _synq_log") == []
        assert fetch(a, "SELECT 1 FROM _synq_fklog") == []
        assert fetch(a, "SELECT 1 FROM _synq_undolog") == []


def test_ins_repl_col(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        exec(a, "CREATE TABLE X(v any)")
        crr.init(a, id=1, ts=False)
        exec(a, "INSERT INTO X VALUES('v1')")

        assert fetch(a, "SELECT rowid, v FROM X") == [(1, "v1")]
        assert fetch(a, "SELECT rowid, row_ts, row_peer FROM _synq_id_X") == [(1, 1, 1)]
        assert fetch(a, "SELECT peer, ts FROM _synq_context") == [(1, 2)]
        assert fetch(
            a, "SELECT ts, peer, row_ts, row_peer, val, tbl_index FROM _synq_log"
        ) == [(2, 1, 1, 1, "v1", None)]
        assert fetch(a, "SELECT 1 FROM _synq_fklog") == []
        assert fetch(a, "SELECT 1 FROM _synq_undolog") == []


def test_up_repl_col(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        exec(a, "CREATE TABLE X(v any)")
        crr.init(a, id=1, ts=False)
        exec(a, "INSERT INTO X VALUES('v1')")
        exec(a, "UPDATE X SET v = 'v2'")

        assert fetch(a, "SELECT rowid, v FROM X") == [(1, "v2")]
        assert fetch(a, "SELECT rowid, row_ts, row_peer FROM _synq_id_X") == [(1, 1, 1)]
        assert fetch(a, "SELECT peer, ts FROM _synq_context") == [(1, 3)]
        assert fetch(
            a, "SELECT ts, peer, row_ts, row_peer, val, tbl_index FROM _synq_log"
        ) == [(2, 1, 1, 1, "v1", None), (3, 1, 1, 1, "v2", None)]
        assert fetch(a, "SELECT 1 FROM _synq_fklog") == []
        assert fetch(a, "SELECT 1 FROM _synq_undolog") == []


def test_ins_fk_aliased_rowid(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        exec(a, "CREATE TABLE X(x integer PRIMARY KEY)")
        exec(a, "CREATE TABLE Y(y integer PRIMARY KEY, x integer REFERENCES X(x))")
        crr.init(a, id=1, ts=False)
        exec(a, "INSERT INTO X VALUES(1)")
        exec(a, "INSERT INTO Y VALUES(1, 1)")

        assert fetch(a, "SELECT x FROM X") == [(1,)]
        assert fetch(a, "SELECT y, x FROM Y") == [(1, 1)]
        assert fetch(a, "SELECT rowid, row_ts, row_peer FROM _synq_id_X") == [(1, 1, 1)]
        assert fetch(a, "SELECT rowid, row_ts, row_peer FROM _synq_id_Y") == [(1, 2, 1)]
        assert fetch(a, "SELECT row_ts, row_peer, tbl FROM _synq_id") == [
            (1, 1, "X"),
            (2, 1, "Y"),
        ]
        assert fetch(a, "SELECT 1 FROM _synq_log") == []
        assert fetch(
            a,
            "SELECT ts, peer, row_ts, row_peer, fk_id, on_delete, on_update, foreign_row_ts, foreign_row_peer, foreign_index FROM _synq_fklog",
        ) == [
            (
                3,
                1,
                2,
                1,
                0,
                crr.FK_ACTION[sql.OnUpdateDelete.NO_ACTION],
                crr.FK_ACTION[sql.OnUpdateDelete.NO_ACTION],
                1,
                1,
                0,
            )
        ]
        assert fetch(a, "SELECT 1 FROM _synq_undolog") == []
        assert fetch(a, "SELECT peer, ts FROM _synq_context") == [(1, 3)]


def test_up_fk_aliased_rowid(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        exec(a, "CREATE TABLE X(x integer PRIMARY KEY)")
        exec(a, "CREATE TABLE Y(y integer PRIMARY KEY, x integer REFERENCES X(x))")
        crr.init(a, id=1, ts=False)
        exec(a, "INSERT INTO X VALUES(1)")
        exec(a, "INSERT INTO Y VALUES(1, 1)")
        exec(a, "INSERT INTO X VALUES(2)")
        exec(a, "UPDATE Y SET x = 2")

        assert fetch(a, "SELECT x FROM X") == [(1,), (2,)]
        assert fetch(a, "SELECT y, x FROM Y") == [(1, 2)]
        assert fetch(a, "SELECT rowid, row_ts, row_peer FROM _synq_id_X") == [
            (1, 1, 1),
            (2, 4, 1),
        ]
        assert fetch(a, "SELECT rowid, row_ts, row_peer FROM _synq_id_Y") == [(1, 2, 1)]
        assert fetch(a, "SELECT row_ts, row_peer, tbl FROM _synq_id") == [
            (1, 1, "X"),
            (2, 1, "Y"),
            (4, 1, "X"),
        ]
        assert fetch(a, "SELECT 1 FROM _synq_log") == []
        assert fetch(
            a,
            "SELECT ts, peer, row_ts, row_peer, fk_id, on_delete, on_update, foreign_row_ts, foreign_row_peer, foreign_index FROM _synq_fklog",
        ) == [
            (
                3,
                1,
                2,
                1,
                0,
                crr.FK_ACTION[sql.OnUpdateDelete.NO_ACTION],
                crr.FK_ACTION[sql.OnUpdateDelete.NO_ACTION],
                1,
                1,
                0,
            ),
            (
                5,
                1,
                2,
                1,
                0,
                crr.FK_ACTION[sql.OnUpdateDelete.NO_ACTION],
                crr.FK_ACTION[sql.OnUpdateDelete.NO_ACTION],
                4,
                1,
                0,
            ),
        ]
        assert fetch(a, "SELECT 1 FROM _synq_undolog") == []
        assert fetch(a, "SELECT peer, ts FROM _synq_context") == [(1, 5)]


def test_ins_fk_repl_col(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        exec(a, "CREATE TABLE X(x any PRIMARY KEY)")
        exec(a, "CREATE TABLE Y(y integer PRIMARY KEY, x integer REFERENCES X(x))")
        crr.init(a, id=1, ts=False)
        exec(a, "INSERT INTO X VALUES(1)")
        exec(a, "INSERT INTO Y VALUES(1, 1)")

        assert fetch(a, "SELECT x FROM X") == [(1,)]
        assert fetch(a, "SELECT y, x FROM Y") == [(1, 1)]
        assert fetch(a, "SELECT rowid, row_ts, row_peer FROM _synq_id_X") == [(1, 1, 1)]
        assert fetch(a, "SELECT rowid, row_ts, row_peer FROM _synq_id_Y") == [(1, 3, 1)]
        assert fetch(a, "SELECT row_ts, row_peer, tbl FROM _synq_id") == [
            (1, 1, "X"),
            (3, 1, "Y"),
        ]
        assert fetch(
            a, "SELECT ts, peer, row_ts, row_peer, val, tbl_index FROM _synq_log"
        ) == [(2, 1, 1, 1, 1, 0)]
        assert fetch(
            a,
            "SELECT ts, peer, row_ts, row_peer, fk_id, on_delete, on_update, foreign_row_ts, foreign_row_peer, foreign_index FROM _synq_fklog",
        ) == [
            (
                4,
                1,
                3,
                1,
                0,
                crr.FK_ACTION[sql.OnUpdateDelete.NO_ACTION],
                crr.FK_ACTION[sql.OnUpdateDelete.NO_ACTION],
                1,
                1,
                0,
            )
        ]
        assert fetch(a, "SELECT 1 FROM _synq_undolog") == []
        assert fetch(a, "SELECT peer, ts FROM _synq_context") == [(1, 4)]


def test_up_fk_repl_col(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        exec(a, "CREATE TABLE X(x any PRIMARY KEY)")
        exec(a, "CREATE TABLE Y(y integer PRIMARY KEY, x integer REFERENCES X(x))")
        crr.init(a, id=1, ts=False)
        exec(a, "INSERT INTO X VALUES(1)")
        exec(a, "INSERT INTO Y VALUES(1, 1)")
        exec(a, "INSERT INTO X VALUES(2)")
        exec(a, "UPDATE Y SET x = 2")

        assert fetch(a, "SELECT x FROM X") == [(1,), (2,)]
        assert fetch(a, "SELECT y, x FROM Y") == [(1, 2)]
        assert fetch(a, "SELECT row_ts, row_peer FROM _synq_id_X") == [
            (1, 1),
            (5, 1),
        ]
        assert fetch(a, "SELECT row_ts, row_peer FROM _synq_id_Y") == [(3, 1)]
        assert fetch(a, "SELECT row_ts, row_peer, tbl FROM _synq_id") == [
            (1, 1, "X"),
            (3, 1, "Y"),
            (5, 1, "X"),
        ]
        assert fetch(
            a, "SELECT ts, peer, row_ts, row_peer, val, tbl_index FROM _synq_log"
        ) == [(2, 1, 1, 1, 1, 0), (6, 1, 5, 1, 2, 0)]
        assert fetch(
            a,
            "SELECT ts, peer, row_ts, row_peer, fk_id, on_delete, on_update, foreign_row_ts, foreign_row_peer, foreign_index FROM _synq_fklog",
        ) == [
            (
                4,
                1,
                3,
                1,
                0,
                crr.FK_ACTION[sql.OnUpdateDelete.NO_ACTION],
                crr.FK_ACTION[sql.OnUpdateDelete.NO_ACTION],
                1,
                1,
                0,
            ),
            (
                7,
                1,
                3,
                1,
                0,
                crr.FK_ACTION[sql.OnUpdateDelete.NO_ACTION],
                crr.FK_ACTION[sql.OnUpdateDelete.NO_ACTION],
                5,
                1,
                0,
            ),
        ]
        assert fetch(a, "SELECT 1 FROM _synq_undolog") == []
        assert fetch(a, "SELECT peer, ts FROM _synq_context") == [(1, 7)]


def test_ins_fk_repl_multi_col(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        exec(
            a,
            "CREATE TABLE X(x integer PRIMARY KEY, x1 integer, x2 integer, UNIQUE(x1,x2))",
        )
        exec(
            a,
            "CREATE TABLE Y(y integer PRIMARY KEY, x1 integer, x2 integer, FOREIGN KEY(x1,x2) REFERENCES X(x1, x2))",
        )
        crr.init(a, id=1, ts=False)
        exec(a, "INSERT INTO X VALUES(1, 2, 3)")
        exec(a, "INSERT INTO Y VALUES(1, 2, 3)")

        assert fetch(a, "SELECT x, x1, x2 FROM X") == [(1, 2, 3)]
        assert fetch(a, "SELECT y, x1, x2 FROM Y") == [(1, 2, 3)]
        assert fetch(a, "SELECT row_ts, row_peer FROM _synq_id_X") == [(1, 1)]
        assert fetch(a, "SELECT row_ts, row_peer FROM _synq_id_Y") == [(4, 1)]
        assert fetch(a, "SELECT row_ts, row_peer, tbl FROM _synq_id") == [
            (1, 1, "X"),
            (4, 1, "Y"),
        ]
        assert fetch(
            a, "SELECT ts, peer, row_ts, row_peer, val, tbl_index FROM _synq_log"
        ) == [(2, 1, 1, 1, 2, 1), (3, 1, 1, 1, 3, 1)]
        assert fetch(
            a,
            "SELECT ts, peer, row_ts, row_peer, fk_id, on_delete, on_update, foreign_row_ts, foreign_row_peer, foreign_index FROM _synq_fklog",
        ) == [
            (
                5,
                1,
                4,
                1,
                0,
                crr.FK_ACTION[sql.OnUpdateDelete.NO_ACTION],
                crr.FK_ACTION[sql.OnUpdateDelete.NO_ACTION],
                1,
                1,
                1,
            )
        ]
        assert fetch(a, "SELECT 1 FROM _synq_undolog") == []
        assert fetch(a, "SELECT peer, ts FROM _synq_context") == [(1, 5)]


def test_up_fk_repl_multi_col(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        exec(
            a,
            "CREATE TABLE X(x integer PRIMARY KEY, x1 integer, x2 integer, UNIQUE(x1,x2))",
        )
        exec(
            a,
            "CREATE TABLE Y(y integer PRIMARY KEY, x1 integer, x2 integer, FOREIGN KEY(x1,x2) REFERENCES X(x1, x2))",
        )
        crr.init(a, id=1, ts=False)
        exec(a, "INSERT INTO X VALUES(1, 2, 3)")
        exec(a, "INSERT INTO Y VALUES(1, 2, 3)")
        exec(a, "INSERT INTO X VALUES(2, 3, 4)")
        exec(a, "UPDATE Y SET x1 = 3, x2 = 4")

        assert fetch(a, "SELECT x, x1, x2 FROM X") == [(1, 2, 3), (2, 3, 4)]
        assert fetch(a, "SELECT y, x1, x2 FROM Y") == [(1, 3, 4)]
        assert fetch(a, "SELECT row_ts, row_peer FROM _synq_id_X") == [
            (1, 1),
            (6, 1),
        ]
        assert fetch(a, "SELECT row_ts, row_peer FROM _synq_id_Y") == [(4, 1)]
        assert fetch(a, "SELECT row_ts, row_peer, tbl FROM _synq_id") == [
            (1, 1, "X"),
            (4, 1, "Y"),
            (6, 1, "X"),
        ]
        assert fetch(
            a, "SELECT ts, peer, row_ts, row_peer, val, tbl_index FROM _synq_log"
        ) == [
            (2, 1, 1, 1, 2, 1),
            (3, 1, 1, 1, 3, 1),
            (7, 1, 6, 1, 3, 1),
            (8, 1, 6, 1, 4, 1),
        ]
        assert fetch(
            a,
            "SELECT ts, peer, row_ts, row_peer, fk_id, on_delete, on_update, foreign_row_ts, foreign_row_peer, foreign_index FROM _synq_fklog",
        ) == [
            (
                5,
                1,
                4,
                1,
                0,
                crr.FK_ACTION[sql.OnUpdateDelete.NO_ACTION],
                crr.FK_ACTION[sql.OnUpdateDelete.NO_ACTION],
                1,
                1,
                1,
            ),
            (
                9,
                1,
                4,
                1,
                0,
                crr.FK_ACTION[sql.OnUpdateDelete.NO_ACTION],
                crr.FK_ACTION[sql.OnUpdateDelete.NO_ACTION],
                6,
                1,
                1,
            ),
        ]
        assert fetch(a, "SELECT 1 FROM _synq_undolog") == []
        assert fetch(a, "SELECT peer, ts FROM _synq_context") == [(1, 9)]


def test_clone_to(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b:
        exec(a, "CREATE TABLE X(rowid integer PRIMARY KEY);")
        crr.init(a, id=1, ts=False)
        crr.clone_to(a, b, id=2)

        assert fetch(a, "SELECT peer FROM _synq_local") == [(1,)]
        assert fetch(b, "SELECT peer FROM _synq_local") == [(2,)]


def test_pull_ins_aliased_rowid(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b:
        exec(a, "CREATE TABLE X(rowid integer PRIMARY KEY);")
        crr.init(a, id=1, ts=False)
        crr.clone_to(a, b, id=2)
        exec(a, "INSERT INTO X VALUES(1)")

        crr.pull_from(b, tmp_path / "a.db")
        assert fetch(b, "SELECT rowid FROM X") == [(1,)]
        assert fetch(b, "SELECT row_ts, row_peer FROM _synq_id") == [(1, 1)]
        assert fetch(b, "SELECT peer, ts FROM _synq_context") == [(1, 1), (2, 0)]


def test_pull_del_aliased_rowid(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b:
        exec(a, "CREATE TABLE X(rowid integer PRIMARY KEY);")
        crr.init(a, id=1, ts=False)
        exec(a, "INSERT INTO X VALUES(1)")
        crr.clone_to(a, b, id=2)
        exec(a, "DELETE FROM X")

        crr.pull_from(b, tmp_path / "a.db")
        assert fetch(b, "SELECT * FROM X") == []
        assert fetch(b, "SELECT * FROM _synq_id_X") == []
        assert fetch(b, "SELECT row_ts, row_peer FROM _synq_id") == [(1, 1)]
        assert fetch(b, "SELECT peer, ts FROM _synq_context") == [(1, 2), (2, 0)]
        assert fetch(b, "SELECT ts, peer, obj_ts, obj_peer, ul FROM _synq_undolog") == [
            (2, 1, 1, 1, 1)
        ]


def test_pull_ins_repl_col(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b:
        exec(a, "CREATE TABLE X(v any)")
        crr.init(a, id=1, ts=False)
        crr.clone_to(a, b, id=2)
        exec(a, "INSERT INTO X VALUES('v1')")

        crr.pull_from(b, tmp_path / "a.db")
        assert fetch(b, "SELECT v FROM X") == [("v1",)]
        assert fetch(b, "SELECT row_ts, row_peer FROM _synq_id") == [(1, 1)]
        assert fetch(b, "SELECT peer, ts FROM _synq_context") == [(1, 2), (2, 0)]


def test_pull_del_repl_col(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b:
        exec(a, "CREATE TABLE X(v any)")
        crr.init(a, id=1, ts=False)
        exec(a, "INSERT INTO X VALUES('v1')")
        crr.clone_to(a, b, id=2)
        exec(a, "DELETE FROM X")

        crr.pull_from(b, tmp_path / "a.db")
        assert fetch(b, "SELECT * FROM X") == []
        assert fetch(b, "SELECT * FROM _synq_id_X") == []
        assert fetch(b, "SELECT row_ts, row_peer FROM _synq_id") == [(1, 1)]
        assert fetch(b, "SELECT peer, ts FROM _synq_context") == [(1, 3), (2, 0)]
        assert fetch(b, "SELECT ts, peer, obj_ts, obj_peer, ul FROM _synq_undolog") == [
            (3, 1, 1, 1, 1)
        ]


def test_pull_up_repl_col(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b:
        exec(a, "CREATE TABLE X(v any)")
        crr.init(a, id=1, ts=False)
        crr.clone_to(a, b, id=2)
        exec(a, "INSERT INTO X VALUES('v1')")
        exec(a, "UPDATE X SET v = 'v2'")

        crr.pull_from(b, tmp_path / "a.db")
        assert fetch(a, "SELECT rowid, v FROM X") == [(1, "v2")]
        assert fetch(a, "SELECT rowid, row_ts, row_peer FROM _synq_id_X") == [(1, 1, 1)]
        assert fetch(a, "SELECT ts, peer FROM _synq_context") == [(3, 1)]
        assert fetch(
            a, "SELECT ts, peer, row_ts, row_peer, val, tbl_index FROM _synq_log"
        ) == [(2, 1, 1, 1, "v1", None), (3, 1, 1, 1, "v2", None)]
        assert fetch(a, "SELECT 1 FROM _synq_fklog") == []
        assert fetch(a, "SELECT 1 FROM _synq_undolog") == []


def test_pull_fk_aliased_rowid(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b:
        exec(a, "CREATE TABLE X(x integer PRIMARY KEY)")
        exec(a, "CREATE TABLE Y(y integer PRIMARY KEY, x integer REFERENCES X(x))")
        crr.init(a, id=1, ts=False)
        crr.clone_to(a, b, id=2)
        exec(a, "INSERT INTO X VALUES(1)")
        exec(a, "INSERT INTO Y VALUES(1, 1)")

        crr.pull_from(b, tmp_path / "a.db")
        assert fetch(b, "SELECT x FROM X") == [(1,)]
        assert fetch(b, "SELECT y, x FROM Y") == [(1, 1)]
        assert fetch(b, "SELECT rowid, row_ts, row_peer FROM _synq_id_X") == [(1, 1, 1)]
        assert fetch(b, "SELECT rowid, row_ts, row_peer FROM _synq_id_Y") == [(1, 2, 1)]
        assert fetch(b, "SELECT row_ts, row_peer, tbl FROM _synq_id") == [
            (1, 1, "X"),
            (2, 1, "Y"),
        ]
        assert fetch(b, "SELECT peer, ts FROM _synq_context") == [(1, 3), (2, 0)]
        assert fetch(b, "SELECT 1 FROM _synq_log") == []
        assert fetch(
            b,
            "SELECT ts, peer, row_ts, row_peer, fk_id, on_delete, on_update, foreign_row_ts, foreign_row_peer, foreign_index FROM _synq_fklog",
        ) == [
            (
                3,
                1,
                2,
                1,
                0,
                crr.FK_ACTION[sql.OnUpdateDelete.NO_ACTION],
                crr.FK_ACTION[sql.OnUpdateDelete.NO_ACTION],
                1,
                1,
                0,
            )
        ]
        assert fetch(b, "SELECT 1 FROM _synq_undolog") == []


def test_concur_ins_aliased_rowid(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b:
        exec(a, "CREATE TABLE X(rowid integer PRIMARY KEY);")
        crr.init(a, id=1, ts=False)
        crr.clone_to(a, b, id=2)
        exec(a, "INSERT INTO X VALUES(1)")
        exec(b, "INSERT INTO X VALUES(1)")

        crr.pull_from(a, tmp_path / "b.db")
        assert fetch(a, "SELECT rowid FROM X") == [(1,), (2,)]
        assert fetch(a, "SELECT rowid, row_ts, row_peer FROM _synq_id_X") == [
            (1, 1, 1),
            (2, 1, 2),
        ]
        assert fetch(a, "SELECT row_ts, row_peer FROM _synq_id") == [(1, 1), (1, 2)]
        assert fetch(a, "SELECT peer, ts FROM _synq_context") == [(1, 1), (2, 1)]

        crr.pull_from(b, tmp_path / "a.db")
        assert fetch(b, "SELECT rowid FROM X") == [(1,), (2,)]
        assert fetch(b, "SELECT rowid, row_ts, row_peer FROM _synq_id_X") == [
            (1, 1, 2),
            (2, 1, 1),
        ]
        assert fetch(b, "SELECT row_ts, row_peer FROM _synq_id") == [(1, 1), (1, 2)]
        assert fetch(b, "SELECT peer, ts FROM _synq_context") == [(1, 1), (2, 1)]


def test_concur_ins_repl_col(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b:
        exec(a, "CREATE TABLE X(v any);")
        crr.init(a, id=1, ts=False)
        crr.clone_to(a, b, id=2)
        exec(a, "INSERT INTO X VALUES('v1')")
        exec(b, "INSERT INTO X VALUES('v2')")

        crr.pull_from(b, tmp_path / "a.db")
        assert fetch(b, "SELECT rowid, v FROM X") == [(1, "v2"), (2, "v1")]
        assert fetch(b, "SELECT rowid, row_ts, row_peer FROM _synq_id_X") == [
            (1, 1, 2),
            (2, 1, 1),
        ]
        assert fetch(b, "SELECT row_ts, row_peer FROM _synq_id") == [(1, 1), (1, 2)]
        assert fetch(b, "SELECT peer, ts FROM _synq_context") == [(1, 2), (2, 2)]

        crr.pull_from(a, tmp_path / "b.db")
        assert fetch(a, "SELECT rowid, v FROM X") == [(1, "v1"), (2, "v2")]
        assert fetch(a, "SELECT rowid, row_ts, row_peer FROM _synq_id_X") == [
            (1, 1, 1),
            (2, 1, 2),
        ]
        assert fetch(a, "SELECT row_ts, row_peer FROM _synq_id") == [(1, 1), (1, 2)]
        assert fetch(a, "SELECT peer, ts FROM _synq_context") == [(1, 2), (2, 2)]


def test_concur_up_repl_col(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b:
        exec(a, "CREATE TABLE X(v any);")
        crr.init(a, id=1, ts=False)
        exec(a, "INSERT INTO X VALUES('v1')")
        crr.clone_to(a, b, id=2)
        exec(a, "UPDATE X SET v = 'v2'")
        exec(b, "UPDATE X SET v = 'v3'")

        crr.pull_from(b, tmp_path / "a.db")
        assert fetch(b, "SELECT rowid, v FROM X") == [(1, "v3")]
        assert fetch(b, "SELECT rowid, row_ts, row_peer FROM _synq_id_X") == [
            (1, 1, 1),
        ]
        assert fetch(b, "SELECT row_ts, row_peer FROM _synq_id") == [(1, 1)]
        assert fetch(b, "SELECT peer, ts FROM _synq_context") == [(1, 3), (2, 3)]
        assert fetch(
            b, "SELECT ts, peer, row_ts, row_peer, val, tbl_index FROM _synq_log"
        ) == [
            (2, 1, 1, 1, "v1", None),
            (3, 2, 1, 1, "v3", None),
            (3, 1, 1, 1, "v2", None),
        ]

        crr.pull_from(a, tmp_path / "b.db")
        assert fetch(a, "SELECT rowid, v FROM X") == [(1, "v3")]
        assert fetch(a, "SELECT rowid, row_ts, row_peer FROM _synq_id_X") == [
            (1, 1, 1),
        ]
        assert fetch(a, "SELECT row_ts, row_peer FROM _synq_id") == [(1, 1)]
        assert fetch(a, "SELECT peer, ts FROM _synq_context") == [(1, 3), (2, 3)]
        assert fetch(
            a, "SELECT ts, peer, row_ts, row_peer, val, tbl_index FROM _synq_log"
        ) == [
            (2, 1, 1, 1, "v1", None),
            (3, 1, 1, 1, "v2", None),
            (3, 2, 1, 1, "v3", None),
        ]


def test_conflicting_keys(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "b.bak.db") as b_bakk:
        exec(a, "CREATE TABLE X(v any PRIMARY KEY);")
        crr.init(a, id=1, ts=False)
        crr.clone_to(a, b, id=2)
        exec(a, "INSERT INTO X VALUES('v1')")
        assert fetch(a, "SELECT peer, ts FROM _synq_context") == [(1, 2)]
        exec(b, "INSERT INTO X VALUES('v1')")
        b.backup(b_bakk)

        crr.pull_from(b, tmp_path / "a.db")
        assert fetch(b, "SELECT rowid, v FROM X") == [(1, "v1")]
        assert fetch(b, "SELECT rowid, row_ts, row_peer FROM _synq_id_X") == [
            (1, 1, 1),
        ]
        assert fetch(b, "SELECT row_ts, row_peer FROM _synq_id") == [(1, 1), (1, 2)]
        assert fetch(b, "SELECT peer, ts FROM _synq_context") == [(1, 2), (2, 3)]
        assert fetch(b, "SELECT ts, peer, obj_ts, obj_peer, ul FROM _synq_undolog") == [
            (3, 2, 1, 2, 1)
        ]

        crr.pull_from(a, tmp_path / "b.bak.db")
        assert fetch(a, "SELECT rowid, v FROM X") == [(1, "v1")]
        assert fetch(a, "SELECT rowid, row_ts, row_peer FROM _synq_id_X") == [
            (1, 1, 1),
        ]
        assert fetch(a, "SELECT row_ts, row_peer FROM _synq_id") == [(1, 1), (1, 2)]
        assert fetch(a, "SELECT peer, ts FROM _synq_context") == [(1, 3), (2, 2)]
        assert fetch(a, "SELECT ts, peer, obj_ts, obj_peer, ul FROM _synq_undolog") == [
            (3, 1, 1, 2, 1)
        ]


def test_conflicting_3keys(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "b.bak.db") as b_bak:
        exec(a, "CREATE TABLE X(u any PRIMARY KEY, v any UNIQUE);")
        crr.init(a, id=1, ts=False)
        crr.clone_to(a, b, id=2)
        exec(a, "INSERT INTO X VALUES('u1', 'v1')")
        exec(a, "INSERT INTO X VALUES('u2', 'v2')")
        exec(b, "INSERT INTO X VALUES('u1', 'v2')")
        b.backup(b_bak)

        crr.pull_from(b, tmp_path / "a.db")
        assert fetch(b, "SELECT rowid, u, v FROM X") == [
            (1, "u1", "v1"),
        ]
        assert fetch(b, "SELECT rowid, row_ts, row_peer FROM _synq_id_X") == [
            (1, 1, 1),
        ]
        assert fetch(b, "SELECT row_ts, row_peer FROM _synq_id") == [
            (1, 1),
            (1, 2),
            (4, 1),
        ]
        assert fetch(b, "SELECT peer, ts FROM _synq_context") == [(1, 6), (2, 8)]
        assert fetch(b, "SELECT ts, peer, obj_ts, obj_peer, ul FROM _synq_undolog") == [
            (7, 2, 1, 2, 1),
            (8, 2, 4, 1, 1),
        ]

        crr.pull_from(a, tmp_path / "b.bak.db")
        assert fetch(a, "SELECT rowid, u, v FROM X") == [
            (1, "u1", "v1"),
        ]
        assert fetch(a, "SELECT rowid, row_ts, row_peer FROM _synq_id_X") == [
            (1, 1, 1),
        ]
        assert fetch(a, "SELECT row_ts, row_peer FROM _synq_id") == [
            (1, 1),
            (1, 2),
            (4, 1),
        ]
        assert fetch(a, "SELECT peer, ts FROM _synq_context") == [(1, 8), (2, 3)]
        assert fetch(a, "SELECT ts, peer, obj_ts, obj_peer, ul FROM _synq_undolog") == [
            (7, 1, 1, 2, 1),
            (8, 1, 4, 1, 1),
        ]
