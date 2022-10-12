import pysqlite3 as sqlite3
import typing
from contextlib import closing
from synqlite import crr
import pathlib
from sqlschm import sql
from dataclasses import dataclass
from test_utils import exec, fetch, crr_from, Col, Ref, Undo, Crr


_DEFAULT_CONF = crr.Config(physical_clock=False)


def test_crr_init(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        assert crr_from(a) == Crr(
            tbls={},
            ctx={1: 0},
            log=set(),
        )


def test_aliased_rowid(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x integer PRIMARY KEY)")
        crr.init(a, id=1, conf=_DEFAULT_CONF)

        exec(a, "INSERT INTO X VALUES(1)")
        assert crr_from(a) == Crr(
            tbls={"X": {(1, (1, 1))}},
            ctx={1: 1},
            log=set(),
        )

        exec(a, "UPDATE X SET x = 2")
        assert crr_from(a) == Crr(
            tbls={"X": {(2, (1, 1))}},
            ctx={1: 1},
            log=set(),
        )

        exec(a, "DELETE FROM X")
        assert crr_from(a) == Crr(
            tbls={"X": set()},
            ctx={1: 2},
            log={Undo(ts=(2, 1), obj=(1, 1), ul=1)},
        )


def test_repl_col(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(v any)")
        crr.init(a, id=1, conf=_DEFAULT_CONF)

        exec(a, "INSERT INTO X VALUES('v1')")
        assert crr_from(a) == Crr(
            tbls={"X": {("v1", (1, 1))}},
            ctx={1: 2},
            log={Col(ts=(2, 1), row=(1, 1), col=0, val="v1")},
        )

        exec(a, "UPDATE X SET v = 'v2'")
        assert crr_from(a) == Crr(
            tbls={"X": {("v2", (1, 1))}},
            ctx={1: 3},
            log={
                Col(ts=(3, 1), row=(1, 1), col=0, val="v2"),
                Col(ts=(2, 1), row=(1, 1), col=0, val="v1"),
            },
        )


def test_fk_aliased_rowid(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x integer PRIMARY KEY)")
        exec(a, "CREATE TABLE Y(y integer PRIMARY KEY, x integer REFERENCES X(x))")
        crr.init(a, id=1, conf=_DEFAULT_CONF)

        exec(a, "INSERT INTO X VALUES(1)")
        exec(a, "INSERT INTO Y VALUES(1, 1)")
        assert crr_from(a) == Crr(
            tbls={"X": {(1, (1, 1))}, "Y": {(1, 1, (2, 1))}},
            ctx={1: 3},
            log={
                Ref(ts=(3, 1), row=(2, 1), fk=0, target=(1, 1)),
            },
        )

        exec(a, "INSERT INTO X VALUES(2)")
        exec(a, "UPDATE Y SET x = 2")

        assert crr_from(a) == Crr(
            tbls={"X": {(1, (1, 1)), (2, (4, 1))}, "Y": {(1, 2, (2, 1))}},
            ctx={1: 5},
            log={
                Ref(ts=(3, 1), row=(2, 1), fk=0, target=(1, 1)),
                Ref(ts=(5, 1), row=(2, 1), fk=0, target=(4, 1)),
            },
        )


def test_fk_repl_col(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x any PRIMARY KEY)")
        exec(a, "CREATE TABLE Y(y integer PRIMARY KEY, x integer REFERENCES X(x))")
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        exec(a, "INSERT INTO X VALUES(1)")
        exec(a, "INSERT INTO Y VALUES(1, 1)")

        assert crr_from(a) == Crr(
            tbls={
                "X": {(1, (1, 1))},
                "Y": {(1, 1, (3, 1))},
            },
            ctx={1: 4},
            log={
                Col(ts=(2, 1), row=(1, 1), col=0, val=1),
                Ref(ts=(4, 1), row=(3, 1), fk=0, target=(1, 1)),
            },
        )

        exec(a, "INSERT INTO X VALUES(2)")
        exec(a, "UPDATE Y SET x = 2")

        assert crr_from(a) == Crr(
            tbls={
                "X": {(1, (1, 1)), (2, (5, 1))},
                "Y": {(1, 2, (3, 1))},
            },
            ctx={1: 7},
            log={
                Col(ts=(2, 1), row=(1, 1), col=0, val=1),
                Ref(ts=(4, 1), row=(3, 1), fk=0, target=(1, 1)),
                Col(ts=(6, 1), row=(5, 1), col=0, val=2),
                Ref(ts=(7, 1), row=(3, 1), fk=0, target=(5, 1)),
            },
        )


def test_fk_repl_multi_col(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(
            a,
            "CREATE TABLE X(x integer PRIMARY KEY, x1 integer, x2 integer, UNIQUE(x1,x2))",
        )
        exec(
            a,
            "CREATE TABLE Y(y integer PRIMARY KEY, x1 integer, x2 integer, FOREIGN KEY(x1,x2) REFERENCES X(x1, x2))",
        )
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        exec(a, "INSERT INTO X VALUES(1, 2, 3)")
        exec(a, "INSERT INTO Y VALUES(1, 2, 3)")

        assert crr_from(a) == Crr(
            tbls={
                "X": {
                    (1, 2, 3, (1, 1)),
                },
                "Y": {
                    (1, 2, 3, (4, 1)),
                },
            },
            ctx={1: 5},
            log={
                Col(ts=(2, 1), row=(1, 1), col=0, val=2),
                Col(ts=(3, 1), row=(1, 1), col=1, val=3),
                Ref(ts=(5, 1), row=(4, 1), fk=0, target=(1, 1)),
            },
        )

        exec(a, "INSERT INTO X VALUES(2, 3, 4)")
        exec(a, "UPDATE Y SET x1 = 3, x2 = 4")

        assert crr_from(a) == Crr(
            tbls={
                "X": {
                    (1, 2, 3, (1, 1)),
                    (2, 3, 4, (6, 1)),
                },
                "Y": {
                    (1, 3, 4, (4, 1)),
                },
            },
            ctx={1: 9},
            log={
                Col(ts=(2, 1), row=(1, 1), col=0, val=2),
                Col(ts=(3, 1), row=(1, 1), col=1, val=3),
                Ref(ts=(5, 1), row=(4, 1), fk=0, target=(1, 1)),
                Col(ts=(7, 1), row=(6, 1), col=0, val=3),
                Col(ts=(8, 1), row=(6, 1), col=1, val=4),
                Ref(ts=(9, 1), row=(4, 1), fk=0, target=(6, 1)),
            },
        )


def test_fk_up_cascade(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x any PRIMARY KEY)")
        exec(
            a,
            "CREATE TABLE Y(y integer PRIMARY KEY, x integer REFERENCES X(x) ON UPDATE CASCADE)",
        )
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        exec(a, "INSERT INTO X VALUES(1)")
        exec(a, "INSERT INTO Y VALUES(1, 1)")
        exec(a, "UPDATE X SET x=2")

        assert crr_from(a) == Crr(
            tbls={"X": {(2, (1, 1))}, "Y": {(1, 2, (3, 1))}},
            ctx={1: 6},
            log={
                Col(ts=(2, 1), row=(1, 1), col=0, val=1),
                Ref(ts=(4, 1), row=(3, 1), fk=0, target=(1, 1)),
                Ref(ts=(5, 1), row=(3, 1), fk=0, target=(1, 1)),
                Col(ts=(6, 1), row=(1, 1), col=0, val=2),
            },
        )


def test_fk_up_set_null(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x any PRIMARY KEY)")
        exec(
            a,
            "CREATE TABLE Y(y integer PRIMARY KEY, x integer REFERENCES X(x) ON UPDATE SET NULL)",
        )
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        exec(a, "INSERT INTO X VALUES(1)")
        exec(a, "INSERT INTO Y VALUES(1, 1)")
        exec(a, "UPDATE X SET x=2")

        assert crr_from(a) == Crr(
            tbls={"X": {(2, (1, 1))}, "Y": {(1, None, (3, 1))}},
            ctx={1: 6},
            log={
                Col(ts=(2, 1), row=(1, 1), col=0, val=1),
                Ref(ts=(4, 1), row=(3, 1), fk=0, target=(1, 1)),
                Ref(ts=(5, 1), row=(3, 1), fk=0, target=(None, None)),
                Col(ts=(6, 1), row=(1, 1), col=0, val=2),
            },
        )


def test_clone_to(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b:
        exec(a, "PRAGMA foreign_keys=ON")
        crr.init(a, id=1, conf=_DEFAULT_CONF)

        crr.clone_to(a, b, id=2)
        assert crr_from(b) == Crr(tbls={}, ctx={1: 0, 2: 0}, log=set())


def test_pull_from(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b:
        exec(a, "PRAGMA foreign_keys=ON")
        crr.init(a, id=1, conf=_DEFAULT_CONF)

        crr.clone_to(a, b, id=2)
        crr.pull_from(b, tmp_path / "a.db")
        assert crr_from(b) == Crr(tbls={}, ctx={1: 0, 2: 0}, log=set())


def test_pull_aliased_rowid(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x integer PRIMARY KEY);")
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        crr.clone_to(a, b, id=2)

        exec(a, "INSERT INTO X VALUES(1)")
        crr.pull_from(b, tmp_path / "a.db")
        assert crr_from(b) == Crr(
            tbls={"X": {(1, (1, 1))}},
            ctx={1: 1, 2: 0},
            log=set(),
        )

        exec(a, "UPDATE X SET x = 2")
        crr.pull_from(b, tmp_path / "a.db")
        assert crr_from(b) == Crr(
            tbls={"X": {(1, (1, 1))}},  # x does not change on b
            ctx={1: 1, 2: 0},
            log=set(),
        )

        exec(a, "DELETE FROM X")
        crr.pull_from(b, tmp_path / "a.db")
        assert crr_from(b) == Crr(
            tbls={"X": set()},
            ctx={1: 2, 2: 0},
            log={Undo(ts=(2, 1), obj=(1, 1), ul=1)},
        )


def test_pull_repl_col(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(v any)")
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        crr.clone_to(a, b, id=2)

        exec(a, "INSERT INTO X VALUES('v1')")
        crr.pull_from(b, tmp_path / "a.db")
        assert crr_from(b) == Crr(
            tbls={"X": {("v1", (1, 1))}},
            ctx={1: 2, 2: 0},
            log={Col(ts=(2, 1), row=(1, 1), col=0, val="v1")},
        )

        exec(a, "UPDATE X SET v = 'v2'")
        crr.pull_from(b, tmp_path / "a.db")
        assert crr_from(b) == Crr(
            tbls={"X": {("v2", (1, 1))}},
            ctx={1: 3, 2: 0},
            log={
                Col(ts=(3, 1), row=(1, 1), col=0, val="v2"),
                Col(ts=(2, 1), row=(1, 1), col=0, val="v1"),
            },
        )


def test_pull_fk_aliased_rowid(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x integer PRIMARY KEY)")
        exec(a, "CREATE TABLE Y(y integer PRIMARY KEY, x integer REFERENCES X(x))")
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        crr.clone_to(a, b, id=2)

        exec(a, "INSERT INTO X VALUES(1)")
        exec(a, "INSERT INTO Y VALUES(1, 1)")
        crr.pull_from(b, tmp_path / "a.db")
        assert crr_from(b) == Crr(
            tbls={"X": {(1, (1, 1))}, "Y": {(1, 1, (2, 1))}},
            ctx={1: 3, 2: 0},
            log={
                Ref(ts=(3, 1), row=(2, 1), fk=0, target=(1, 1)),
            },
        )

        exec(a, "INSERT INTO X VALUES(2)")
        exec(a, "UPDATE Y SET x = 2")
        crr.pull_from(b, tmp_path / "a.db")
        assert crr_from(b) == Crr(
            tbls={
                "X": {
                    (1, (1, 1)),
                    (2, (4, 1)),
                },
                "Y": {
                    (1, 2, (2, 1)),
                },
            },
            ctx={1: 5, 2: 0},
            log={
                Ref(ts=(3, 1), row=(2, 1), fk=0, target=(1, 1)),
                Ref(ts=(5, 1), row=(2, 1), fk=0, target=(4, 1)),
            },
        )


def test_concur_ins_aliased_rowid(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(rowid integer PRIMARY KEY);")
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        crr.clone_to(a, b, id=2)
        exec(a, "INSERT INTO X VALUES(1)")
        exec(b, "INSERT INTO X VALUES(1)")

        crr.pull_from(a, tmp_path / "b.db")
        assert crr_from(a) == Crr(
            tbls={
                "X": {
                    (1, (1, 1)),
                    (2, (1, 2)),
                }
            },
            ctx={1: 1, 2: 1},
            log=set(),
        )

        crr.pull_from(b, tmp_path / "a.db")
        assert crr_from(b) == Crr(
            tbls={
                "X": {
                    (1, (1, 2)),
                    (2, (1, 1)),
                }
            },
            ctx={1: 1, 2: 1},
            log=set(),
        )


def test_concur_ins_repl_col(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(v any);")
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        crr.clone_to(a, b, id=2)
        exec(a, "INSERT INTO X VALUES('a1')")
        exec(b, "INSERT INTO X VALUES('b1')")

        crr.pull_from(a, tmp_path / "b.db")
        assert crr_from(a) == Crr(
            tbls={
                "X": {
                    ("a1", (1, 1)),
                    ("b1", (1, 2)),
                },
            },
            ctx={1: 2, 2: 2},
            log={
                Col(ts=(2, 1), row=(1, 1), col=0, val="a1"),
                Col(ts=(2, 2), row=(1, 2), col=0, val="b1"),
            },
        )

        crr.pull_from(b, tmp_path / "a.db")
        exec(a, "UPDATE X SET v = 'a2' WHERE v = 'a1'")
        exec(b, "UPDATE X SET v = 'b2' WHERE v = 'a1'")
        crr.pull_from(a, tmp_path / "b.db")
        assert crr_from(a) == Crr(
            tbls={
                "X": {
                    ("b2", (1, 1)),
                    ("b1", (1, 2)),
                },
            },
            ctx={1: 3, 2: 3},
            log={
                Col(ts=(2, 1), row=(1, 1), col=0, val="a1"),
                Col(ts=(2, 2), row=(1, 2), col=0, val="b1"),
                Col(ts=(3, 1), row=(1, 1), col=0, val="a2"),
                Col(ts=(3, 2), row=(1, 1), col=0, val="b2"),
            },
        )


def test_conflicting_keys(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "b.bak.db") as b_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(v any PRIMARY KEY);")
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        crr.clone_to(a, b, id=2)
        exec(a, "INSERT INTO X VALUES('v1')")
        assert fetch(a, "SELECT peer, ts FROM _synq_context") == [(1, 2)]
        exec(b, "INSERT INTO X VALUES('v1')")
        b.backup(b_bak)

        crr.pull_from(b, tmp_path / "a.db")
        assert crr_from(b) == Crr(
            tbls={"X": {("v1", (1, 1))}},
            ctx={1: 2, 2: 3},
            log={
                Col(ts=(2, 1), row=(1, 1), col=0, val="v1"),
                Col(ts=(2, 2), row=(1, 2), col=0, val="v1"),
                Undo(ts=(3, 2), obj=(1, 2), ul=1),
            },
        )

        crr.pull_from(a, tmp_path / "b.bak.db")
        assert crr_from(a) == Crr(
            tbls={"X": {("v1", (1, 1))}},
            ctx={1: 3, 2: 2},
            log={
                Col(ts=(2, 1), row=(1, 1), col=0, val="v1"),
                Col(ts=(2, 2), row=(1, 2), col=0, val="v1"),
                Undo(ts=(3, 1), obj=(1, 2), ul=1),
            },
        )


def test_conflicting_3keys(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "b.bak.db") as b_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(u any PRIMARY KEY, v any UNIQUE);")
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        crr.clone_to(a, b, id=2)
        exec(a, "INSERT INTO X VALUES('u1', 'v1')")
        exec(a, "INSERT INTO X VALUES('u2', 'v2')")
        exec(b, "INSERT INTO X VALUES('u1', 'v2')")
        b.backup(b_bak)

        crr.pull_from(b, tmp_path / "a.db")
        assert crr_from(b) == Crr(
            tbls={"X": {("u1", "v1", (1, 1))}},
            ctx={1: 6, 2: 8},
            log={
                Col(ts=(2, 1), row=(1, 1), col=0, val="u1"),
                Col(ts=(3, 1), row=(1, 1), col=1, val="v1"),
                Col(ts=(5, 1), row=(4, 1), col=0, val="u2"),
                Col(ts=(6, 1), row=(4, 1), col=1, val="v2"),
                Col(ts=(2, 2), row=(1, 2), col=0, val="u1"),
                Col(ts=(3, 2), row=(1, 2), col=1, val="v2"),
                Undo(ts=(7, 2), obj=(1, 2), ul=1),
                Undo(ts=(8, 2), obj=(4, 1), ul=1),
            },
        )

        crr.pull_from(a, tmp_path / "b.bak.db")
        assert crr_from(a) == Crr(
            tbls={"X": {("u1", "v1", (1, 1))}},
            ctx={1: 8, 2: 3},
            log={
                Col(ts=(2, 1), row=(1, 1), col=0, val="u1"),
                Col(ts=(3, 1), row=(1, 1), col=1, val="v1"),
                Col(ts=(5, 1), row=(4, 1), col=0, val="u2"),
                Col(ts=(6, 1), row=(4, 1), col=1, val="v2"),
                Col(ts=(2, 2), row=(1, 2), col=0, val="u1"),
                Col(ts=(3, 2), row=(1, 2), col=1, val="v2"),
                Undo(ts=(7, 1), obj=(1, 2), ul=1),
                Undo(ts=(8, 1), obj=(4, 1), ul=1),
            },
        )


def test_concur_del_fk_restrict_aliased_rowid(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "a.bak.db") as a_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x integer PRIMARY KEY)")
        exec(
            a,
            "CREATE TABLE Y(y integer PRIMARY KEY, x integer REFERENCES X(x) ON DELETE RESTRICT)",
        )
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        exec(a, "INSERT INTO X VALUES(1)")
        crr.clone_to(a, b, id=2)
        exec(a, "DELETE FROM X")
        a.backup(a_bak)
        exec(b, "INSERT INTO Y VALUES(1, 1)")

        crr.pull_from(a, tmp_path / "b.db")
        assert crr_from(a) == Crr(
            tbls={"X": {(1, (1, 1))}, "Y": {(1, 1, (2, 2))}},
            ctx={1: 4, 2: 3},
            log={
                Undo(ts=(2, 1), obj=(1, 1), ul=1),
                Ref(ts=(3, 2), row=(2, 2), fk=0, target=(1, 1)),
                Undo(ts=(4, 1), obj=(1, 1), ul=2),
            },
        )

        crr.pull_from(b, tmp_path / "a.bak.db")
        assert crr_from(b) == Crr(
            tbls={"X": {(1, (1, 1))}, "Y": {(1, 1, (2, 2))}},
            ctx={1: 2, 2: 4},
            log={
                Undo(ts=(2, 1), obj=(1, 1), ul=1),
                Ref(ts=(3, 2), row=(2, 2), fk=0, target=(1, 1)),
                Undo(ts=(4, 2), obj=(1, 1), ul=2),
            },
        )


def test_concur_del_fk_restrict_repl_pk(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "a.bak.db") as a_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x any PRIMARY KEY)")
        exec(
            a,
            "CREATE TABLE Y(y integer PRIMARY KEY, x integer REFERENCES X(x) ON DELETE RESTRICT)",
        )
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        exec(a, "INSERT INTO X VALUES(1)")
        crr.clone_to(a, b, id=2)
        exec(a, "DELETE FROM X")
        a.backup(a_bak)
        exec(b, "INSERT INTO Y VALUES(1, 1)")

        crr.pull_from(a, tmp_path / "b.db")
        assert crr_from(a) == Crr(
            tbls={"X": {(1, (1, 1))}, "Y": {(1, 1, (3, 2))}},
            ctx={1: 5, 2: 4},
            log={
                Col(ts=(2, 1), row=(1, 1), col=0, val=1),
                Undo(ts=(3, 1), obj=(1, 1), ul=1),
                Ref(ts=(4, 2), row=(3, 2), fk=0, target=(1, 1)),
                Undo(ts=(5, 1), obj=(1, 1), ul=2),
            },
        )

        crr.pull_from(b, tmp_path / "a.bak.db")
        assert crr_from(b) == Crr(
            tbls={"X": {(1, (1, 1))}, "Y": {(1, 1, (3, 2))}},
            ctx={1: 3, 2: 5},
            log={
                Col(ts=(2, 1), row=(1, 1), col=0, val=1),
                Undo(ts=(3, 1), obj=(1, 1), ul=1),
                Ref(ts=(4, 2), row=(3, 2), fk=0, target=(1, 1)),
                Undo(ts=(5, 2), obj=(1, 1), ul=2),
            },
        )


def test_concur_del_fk_cascade(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "a.bak.db") as a_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x integer PRIMARY KEY)")
        exec(
            a,
            "CREATE TABLE Y(y integer PRIMARY KEY, x integer REFERENCES X(x) ON DELETE CASCADE)",
        )
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        exec(a, "INSERT INTO X VALUES(1)")
        crr.clone_to(a, b, id=2)
        exec(a, "DELETE FROM X")
        a.backup(a_bak)
        exec(b, "INSERT INTO Y VALUES(1, 1)")

        crr.pull_from(a, tmp_path / "b.db")
        assert crr_from(a) == Crr(
            tbls={"X": set(), "Y": set()},
            ctx={1: 4, 2: 3},
            log={
                Undo(ts=(2, 1), obj=(1, 1), ul=1),
                Ref(ts=(3, 2), row=(2, 2), fk=0, target=(1, 1)),
                Undo(ts=(4, 1), obj=(2, 2), ul=1),
            },
        )

        crr.pull_from(b, tmp_path / "a.bak.db")
        assert crr_from(b) == Crr(
            tbls={"X": set(), "Y": set()},
            ctx={1: 2, 2: 4},
            log={
                Undo(ts=(2, 1), obj=(1, 1), ul=1),
                Ref(ts=(3, 2), row=(2, 2), fk=0, target=(1, 1)),
                Undo(ts=(4, 2), obj=(2, 2), ul=1),
            },
        )


def test_concur_del_fk_set_null(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "a.bak.db") as a_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x integer PRIMARY KEY)")
        exec(
            a,
            "CREATE TABLE Y(y integer PRIMARY KEY, x integer REFERENCES X(x) ON DELETE SET NULL)",
        )
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        exec(a, "INSERT INTO X VALUES(1)")
        crr.clone_to(a, b, id=2)
        exec(a, "DELETE FROM X")
        a.backup(a_bak)
        exec(b, "INSERT INTO Y VALUES(1, 1)")

        crr.pull_from(a, tmp_path / "b.db")
        assert crr_from(a) == Crr(
            tbls={"X": set(), "Y": {(1, None, (2, 2))}},
            ctx={1: 4, 2: 3},
            log={
                Undo(ts=(2, 1), obj=(1, 1), ul=1),
                Ref(ts=(3, 2), row=(2, 2), fk=0, target=(1, 1)),
                Ref(ts=(4, 1), row=(2, 2), fk=0, target=(None, None)),
            },
        )

        crr.pull_from(b, tmp_path / "a.bak.db")
        assert crr_from(b) == Crr(
            tbls={"X": set(), "Y": {(1, None, (2, 2))}},
            ctx={1: 2, 2: 4},
            log={
                Undo(ts=(2, 1), obj=(1, 1), ul=1),
                Ref(ts=(3, 2), row=(2, 2), fk=0, target=(1, 1)),
                Ref(ts=(4, 2), row=(2, 2), fk=0, target=(None, None)),
            },
        )


def test_concur_up_fk_restrict(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "a.bak.db") as a_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x any PRIMARY KEY)")
        exec(
            a,
            "CREATE TABLE Y(y integer PRIMARY KEY, x integer REFERENCES X(x) ON UPDATE RESTRICT)",
        )
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        exec(a, "INSERT INTO X VALUES(1)")
        crr.clone_to(a, b, id=2)
        exec(a, "UPDATE X SET x=2")
        a.backup(a_bak)
        exec(b, "INSERT INTO Y VALUES(1, 1)")

        crr.pull_from(a, tmp_path / "b.db")
        assert crr_from(a) == Crr(
            tbls={"X": {(1, (1, 1))}, "Y": {(1, 1, (3, 2))}},
            ctx={1: 5, 2: 4},
            log={
                Col(ts=(2, 1), row=(1, 1), col=0, val=1),
                Col(ts=(3, 1), row=(1, 1), col=0, val=2),
                Ref(ts=(4, 2), row=(3, 2), fk=0, target=(1, 1)),
                Undo(ts=(5, 1), obj=(3, 1), ul=1),
            },
        )

        crr.pull_from(b, tmp_path / "a.bak.db")
        assert crr_from(b) == Crr(
            tbls={"X": {(1, (1, 1))}, "Y": {(1, 1, (3, 2))}},
            ctx={1: 3, 2: 5},
            log={
                Col(ts=(2, 1), row=(1, 1), col=0, val=1),
                Col(ts=(3, 1), row=(1, 1), col=00, val=2),
                Ref(ts=(4, 2), row=(3, 2), fk=0, target=(1, 1)),
                Undo(ts=(5, 2), obj=(3, 1), ul=1),
            },
        )


def test_concur_up_fk_cascade(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "a.bak.db") as a_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x any PRIMARY KEY)")
        exec(
            a,
            "CREATE TABLE Y(y integer PRIMARY KEY, x integer REFERENCES X(x) ON UPDATE CASCADE)",
        )
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        exec(a, "INSERT INTO X VALUES(1)")
        crr.clone_to(a, b, id=2)
        exec(a, "UPDATE X SET x=2")
        a.backup(a_bak)
        exec(b, "INSERT INTO Y VALUES(1, 1)")

        crr.pull_from(a, tmp_path / "b.db")
        assert crr_from(a) == Crr(
            tbls={"X": {(2, (1, 1))}, "Y": {(1, 2, (3, 2))}},
            ctx={1: 5, 2: 4},
            log={
                Col(ts=(2, 1), row=(1, 1), col=0, val=1),
                Col(ts=(3, 1), row=(1, 1), col=0, val=2),
                Ref(ts=(4, 2), row=(3, 2), fk=0, target=(1, 1)),
                Ref(ts=(5, 1), row=(3, 2), fk=0, target=(1, 1)),
            },
        )

        crr.pull_from(b, tmp_path / "a.bak.db")
        assert crr_from(b) == Crr(
            tbls={"X": {(2, (1, 1))}, "Y": {(1, 2, (3, 2))}},
            ctx={1: 3, 2: 5},
            log={
                Col(ts=(2, 1), row=(1, 1), col=0, val=1),
                Col(ts=(3, 1), row=(1, 1), col=0, val=2),
                Ref(ts=(4, 2), row=(3, 2), fk=0, target=(1, 1)),
                Ref(ts=(5, 2), row=(3, 2), fk=0, target=(1, 1)),
            },
        )


def test_concur_up_fk_set_null(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "a.bak.db") as a_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x any PRIMARY KEY)")
        exec(
            a,
            "CREATE TABLE Y(y integer PRIMARY KEY, x integer REFERENCES X(x) ON UPDATE SET NULL)",
        )
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        exec(a, "INSERT INTO X VALUES(1)")
        crr.clone_to(a, b, id=2)
        exec(a, "UPDATE X SET x=2")
        a.backup(a_bak)
        exec(b, "INSERT INTO Y VALUES(1, 1)")

        crr.pull_from(a, tmp_path / "b.db")
        assert crr_from(a) == Crr(
            tbls={"X": {(2, (1, 1))}, "Y": {(1, None, (3, 2))}},
            ctx={1: 5, 2: 4},
            log={
                Col(ts=(2, 1), row=(1, 1), col=0, val=1),
                Col(ts=(3, 1), row=(1, 1), col=0, val=2),
                Ref(ts=(4, 2), row=(3, 2), fk=0, target=(1, 1)),
                Ref(ts=(5, 1), row=(3, 2), fk=0, target=(None, None)),
            },
        )

        crr.pull_from(b, tmp_path / "a.bak.db")
        assert crr_from(b) == Crr(
            tbls={"X": {(2, (1, 1))}, "Y": {(1, None, (3, 2))}},
            ctx={1: 3, 2: 5},
            log={
                Col(ts=(2, 1), row=(1, 1), col=0, val=1),
                Col(ts=(3, 1), row=(1, 1), col=0, val=2),
                Ref(ts=(4, 2), row=(3, 2), fk=0, target=(1, 1)),
                Ref(ts=(5, 2), row=(3, 2), fk=0, target=(None, None)),
            },
        )
