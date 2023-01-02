# Copyright (c) 2022 Victorien Elvinger
# Licensed under the MIT License (https://mit-license.org)

import pysqlite3 as sqlite3
import typing
from contextlib import closing
from synql import crr
import pathlib
from sqlschm import sql
from dataclasses import dataclass
from test_utils import exec, fetch, crr_from, Val, Undo, Crr


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

        # Test rowid aliases (rowid, _rowid_, oid)
        exec(a, "UPDATE X SET rowid = 3")
        assert crr_from(a) == Crr(
            tbls={"X": {(3, (1, 1))}},
            ctx={1: 1},
            log=set(),
        )

        exec(a, "UPDATE X SET _rowid_ = 4")
        assert crr_from(a) == Crr(
            tbls={"X": {(4, (1, 1))}},
            ctx={1: 1},
            log=set(),
        )

        exec(a, "UPDATE X SET oid = 5")
        assert crr_from(a) == Crr(
            tbls={"X": {(5, (1, 1))}},
            ctx={1: 1},
            log=set(),
        )

        exec(a, "DELETE FROM X")
        assert crr_from(a) == Crr(
            tbls={"X": set()},
            ctx={1: 2},
            log={Undo(ts=(2, 1), obj=(1, 1), ul=1)},
        )

        exec(a, "INSERT INTO X VALUES(1)")
        exec(a, "INSERT OR REPLACE INTO X VALUES(1)")
        assert crr_from(a) == Crr(
            tbls={"X": {(1, (5, 1))}},
            ctx={1: 5},
            log={Undo(ts=(4, 1), obj=(3, 1), ul=1), Undo(ts=(2, 1), obj=(1, 1), ul=1)},
        )

        exec(a, "INSERT INTO X VALUES(1) ON CONFLICT(x) DO UPDATE SET x = 2")
        assert crr_from(a) == Crr(
            tbls={"X": {(2, (5, 1))}},
            ctx={1: 5},
            log={Undo(ts=(4, 1), obj=(3, 1), ul=1), Undo(ts=(2, 1), obj=(1, 1), ul=1)},
        )
        exec(a, "PRAGMA integrity_check")


def test_repl_col(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x integer PRIMARY KEY AUTOINCREMENT, v text)")
        crr.init(a, id=1, conf=_DEFAULT_CONF)

        exec(a, "INSERT INTO X(v) VALUES('v1')")
        assert crr_from(a) == Crr(
            tbls={"X": {(1, "v1", (1, 1))}},
            ctx={1: 1},
            log={Val(ts=(1, 1), row=(1, 1), name="v", val="v1")},
        )

        exec(a, "UPDATE X SET v = 'v2'")
        assert crr_from(a) == Crr(
            tbls={"X": {(1, "v2", (1, 1))}},
            ctx={1: 2},
            log={
                Val(ts=(1, 1), row=(1, 1), name="v", val="v1"),
                Val(ts=(2, 1), row=(1, 1), name="v", val="v2"),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_repl_pk(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x int PRIMARY KEY)")
        crr.init(a, id=1, conf=_DEFAULT_CONF)

        exec(a, "INSERT INTO X VALUES(1)")
        assert crr_from(a) == Crr(
            tbls={"X": {(1, (1, 1))}},
            ctx={1: 1},
            log={Val(ts=(1, 1), row=(1, 1), name="x", val=1)},
        )

        exec(a, "INSERT INTO X VALUES(1) ON CONFLICT(x) DO UPDATE SET x = 2")
        assert crr_from(a) == Crr(
            tbls={"X": {(2, (1, 1))}},
            ctx={1: 2},
            log={
                Val(ts=(1, 1), row=(1, 1), name="x", val=1),
                Val(ts=(2, 1), row=(1, 1), name="x", val=2),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_fk_aliased_rowid(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x integer PRIMARY KEY)")
        exec(
            a,
            "CREATE TABLE Y(y integer PRIMARY KEY, x integer CONSTRAINT fk REFERENCES X(x))",
        )
        crr.init(a, id=1, conf=_DEFAULT_CONF)

        exec(a, "INSERT INTO X VALUES(1)")
        exec(a, "INSERT INTO Y VALUES(1, 1)")
        assert crr_from(a) == Crr(
            tbls={"X": {(1, (1, 1))}, "Y": {(1, 1, (2, 1))}},
            ctx={1: 2},
            log={
                Val(ts=(2, 1), row=(2, 1), name="fk", val=(1, 1)),
            },
        )

        exec(a, "INSERT INTO X VALUES(2)")
        exec(a, "UPDATE Y SET x = 2")

        assert crr_from(a) == Crr(
            tbls={"X": {(1, (1, 1)), (2, (3, 1))}, "Y": {(1, 2, (2, 1))}},
            ctx={1: 4},
            log={
                Val(ts=(2, 1), row=(2, 1), name="fk", val=(1, 1)),
                Val(ts=(4, 1), row=(2, 1), name="fk", val=(3, 1)),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_fk_repl_col(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x int PRIMARY KEY)")
        exec(
            a,
            "CREATE TABLE Y(y integer PRIMARY KEY, x integer CONSTRAINT fk REFERENCES X(x))",
        )
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        exec(a, "INSERT INTO X VALUES(1)")
        exec(a, "INSERT INTO Y VALUES(1, 1)")

        assert crr_from(a) == Crr(
            tbls={
                "X": {(1, (1, 1))},
                "Y": {(1, 1, (2, 1))},
            },
            ctx={1: 2},
            log={
                Val(ts=(1, 1), row=(1, 1), name="x", val=1),
                Val(ts=(2, 1), row=(2, 1), name="fk", val=(1, 1)),
            },
        )

        exec(a, "INSERT INTO X VALUES(2)")
        exec(a, "UPDATE Y SET x = 2")

        assert crr_from(a) == Crr(
            tbls={
                "X": {(1, (1, 1)), (2, (3, 1))},
                "Y": {(1, 2, (2, 1))},
            },
            ctx={1: 4},
            log={
                Val(ts=(1, 1), row=(1, 1), name="x", val=1),
                Val(ts=(2, 1), row=(2, 1), name="fk", val=(1, 1)),
                Val(ts=(3, 1), row=(3, 1), name="x", val=2),
                Val(ts=(4, 1), row=(2, 1), name="fk", val=(3, 1)),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_fk_repl_multi_col(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(
            a,
            "CREATE TABLE X(x integer PRIMARY KEY, x1 integer, x2 integer, UNIQUE(x1,x2))",
        )
        exec(
            a,
            "CREATE TABLE Y(y integer PRIMARY KEY, x1 integer, x2 integer, CONSTRAINT fk FOREIGN KEY(x1,x2) REFERENCES X(x1, x2))",
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
                    (1, 2, 3, (2, 1)),
                },
            },
            ctx={1: 2},
            log={
                Val(ts=(1, 1), row=(1, 1), name="x1", val=2),
                Val(ts=(1, 1), row=(1, 1), name="x2", val=3),
                Val(ts=(2, 1), row=(2, 1), name="fk", val=(1, 1)),
            },
        )

        exec(a, "INSERT INTO X VALUES(2, 3, 4)")
        exec(a, "UPDATE Y SET x1 = 3, x2 = 4")

        assert crr_from(a) == Crr(
            tbls={
                "X": {
                    (1, 2, 3, (1, 1)),
                    (2, 3, 4, (3, 1)),
                },
                "Y": {
                    (1, 3, 4, (2, 1)),
                },
            },
            ctx={1: 4},
            log={
                Val(ts=(1, 1), row=(1, 1), name="x1", val=2),
                Val(ts=(1, 1), row=(1, 1), name="x2", val=3),
                Val(ts=(2, 1), row=(2, 1), name="fk", val=(1, 1)),
                Val(ts=(3, 1), row=(3, 1), name="x1", val=3),
                Val(ts=(3, 1), row=(3, 1), name="x2", val=4),
                Val(ts=(4, 1), row=(2, 1), name="fk", val=(3, 1)),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_fk_up_cascade(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x int PRIMARY KEY)")
        exec(
            a,
            "CREATE TABLE Y(y integer PRIMARY KEY, x integer CONSTRAINT fk REFERENCES X(x) ON UPDATE CASCADE)",
        )
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        exec(a, "INSERT INTO X VALUES(1)")
        exec(a, "INSERT INTO Y VALUES(1, 1)")
        exec(a, "UPDATE X SET x=2")

        assert crr_from(a) == Crr(
            tbls={"X": {(2, (1, 1))}, "Y": {(1, 2, (2, 1))}},
            ctx={1: 4},
            log={
                Val(ts=(1, 1), row=(1, 1), name="x", val=1),
                Val(ts=(2, 1), row=(2, 1), name="fk", val=(1, 1)),
                Val(ts=(4, 1), row=(1, 1), name="x", val=2),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_fk_up_set_null(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x int PRIMARY KEY)")
        exec(
            a,
            "CREATE TABLE Y(y integer PRIMARY KEY, x integer CONSTRAINT fk REFERENCES X(x) ON UPDATE SET NULL)",
        )
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        exec(a, "INSERT INTO X VALUES(1)")
        exec(a, "INSERT INTO Y VALUES(1, 1)")
        exec(a, "UPDATE X SET x=2")

        assert crr_from(a) == Crr(
            tbls={"X": {(2, (1, 1))}, "Y": {(1, None, (2, 1))}},
            ctx={1: 4},
            log={
                Val(ts=(1, 1), row=(1, 1), name="x", val=1),
                Val(ts=(2, 1), row=(2, 1), name="fk", val=(1, 1)),
                Val(ts=(4, 1), row=(1, 1), name="x", val=2),
            },
        )
        exec(a, "PRAGMA integrity_check")


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
        exec(a, "PRAGMA integrity_check")


def test_pull_repl_col(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x integer PRIMARY KEY AUTOINCREMENT, v text)")
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        crr.clone_to(a, b, id=2)

        exec(a, "INSERT INTO X(v) VALUES('v1')")
        crr.pull_from(b, tmp_path / "a.db")
        assert crr_from(b) == Crr(
            tbls={"X": {(1, "v1", (1, 1))}},
            ctx={1: 1, 2: 0},
            log={Val(ts=(1, 1), row=(1, 1), name="v", val="v1")},
        )

        exec(a, "UPDATE X SET v = 'v2'")
        crr.pull_from(b, tmp_path / "a.db")
        assert crr_from(b) == Crr(
            tbls={"X": {(1, "v2", (1, 1))}},
            ctx={1: 2, 2: 0},
            log={
                Val(ts=(1, 1), row=(1, 1), name="v", val="v1"),
                Val(ts=(2, 1), row=(1, 1), name="v", val="v2"),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_pull_fk_aliased_rowid(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x integer PRIMARY KEY)")
        exec(
            a,
            "CREATE TABLE Y(y integer PRIMARY KEY, x integer CONSTRAINT fk REFERENCES X(x))",
        )
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        crr.clone_to(a, b, id=2)

        exec(a, "INSERT INTO X VALUES(1)")
        exec(a, "INSERT INTO Y VALUES(1, 1)")
        crr.pull_from(b, tmp_path / "a.db")
        assert crr_from(b) == Crr(
            tbls={"X": {(1, (1, 1))}, "Y": {(1, 1, (2, 1))}},
            ctx={1: 2, 2: 0},
            log={
                Val(ts=(2, 1), row=(2, 1), name="fk", val=(1, 1)),
            },
        )

        exec(a, "INSERT INTO X VALUES(2)")
        exec(a, "UPDATE Y SET x = 2")
        crr.pull_from(b, tmp_path / "a.db")
        assert crr_from(b) == Crr(
            tbls={
                "X": {
                    (1, (1, 1)),
                    (2, (3, 1)),
                },
                "Y": {
                    (1, 2, (2, 1)),
                },
            },
            ctx={1: 4, 2: 0},
            log={
                Val(ts=(2, 1), row=(2, 1), name="fk", val=(1, 1)),
                Val(ts=(4, 1), row=(2, 1), name="fk", val=(3, 1)),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_pull_fk_fk(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x int PRIMARY KEY)")
        exec(a, "CREATE TABLE Y(y integer PRIMARY KEY CONSTRAINT fk1 REFERENCES X)")
        exec(a, "CREATE TABLE Z(z integer PRIMARY KEY CONSTRAINT fk2 REFERENCES Y)")
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        crr.clone_to(a, b, id=2)

        exec(a, "INSERT INTO X VALUES(1)")
        exec(a, "INSERT INTO Y VALUES(1)")
        exec(a, "INSERT INTO Z VALUES(1)")
        crr.pull_from(b, tmp_path / "a.db")
        assert crr_from(b) == Crr(
            tbls={
                "X": {
                    (1, (1, 1)),
                },
                "Y": {
                    (1, (2, 1)),
                },
                "Z": {
                    (1, (3, 1)),
                },
            },
            ctx={1: 3, 2: 0},
            log={
                Val(ts=(1, 1), row=(1, 1), name="x", val=1),
                Val(ts=(2, 1), row=(2, 1), name="fk1", val=(1, 1)),
                Val(ts=(3, 1), row=(3, 1), name="fk2", val=(2, 1)),
            },
        )
        exec(a, "PRAGMA integrity_check")


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
        exec(a, "PRAGMA integrity_check")


def test_concur_ins_repl_col(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(v text PRIMARY KEY);")
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
            ctx={1: 1, 2: 1},
            log={
                Val(ts=(1, 1), row=(1, 1), name="v", val="a1"),
                Val(ts=(1, 2), row=(1, 2), name="v", val="b1"),
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
                Val(ts=(1, 1), row=(1, 1), name="v", val="a1"),
                Val(ts=(1, 2), row=(1, 2), name="v", val="b1"),
                Val(ts=(3, 1), row=(1, 1), name="v", val="a2"),
                Val(ts=(3, 2), row=(1, 1), name="v", val="b2"),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_conflicting_keys(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "b.bak.db") as b_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(v text PRIMARY KEY);")
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        crr.clone_to(a, b, id=2)
        exec(a, "INSERT INTO X VALUES('v1')")
        exec(b, "INSERT INTO X VALUES('v1')")
        b.backup(b_bak)

        crr.pull_from(b, tmp_path / "a.db")
        assert crr_from(b) == Crr(
            tbls={"X": {("v1", (1, 1))}},
            ctx={1: 1, 2: 2},
            log={
                Val(ts=(1, 1), row=(1, 1), name="v", val="v1"),
                Val(ts=(1, 2), row=(1, 2), name="v", val="v1"),
                Undo(ts=(2, 2), obj=(1, 2), ul=1),
            },
        )

        crr.pull_from(a, tmp_path / "b.bak.db")
        assert crr_from(a) == Crr(
            tbls={"X": {("v1", (1, 1))}},
            ctx={1: 2, 2: 1},
            log={
                Val(ts=(1, 1), row=(1, 1), name="v", val="v1"),
                Val(ts=(1, 2), row=(1, 2), name="v", val="v1"),
                Undo(ts=(2, 1), obj=(1, 2), ul=1),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_unique_nulls(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "b.bak.db") as b_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x integer PRIMARY KEY AUTOINCREMENT, v int UNIQUE);")
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        crr.clone_to(a, b, id=2)
        exec(a, "INSERT INTO X(v) VALUES(NULL)")
        exec(b, "INSERT INTO X(v) VALUES(NULL)")
        b.backup(b_bak)

        crr.pull_from(b, tmp_path / "a.db")
        assert crr_from(b) == Crr(
            tbls={"X": {(2, None, (1, 1)), (1, None, (1, 2))}},
            ctx={1: 1, 2: 1},
            log={
                Val(ts=(1, 1), row=(1, 1), name="v", val=None),
                Val(ts=(1, 2), row=(1, 2), name="v", val=None),
            },
        )

        crr.pull_from(a, tmp_path / "b.bak.db")
        assert crr_from(a) == Crr(
            tbls={"X": {(1, None, (1, 1)), (2, None, (1, 2))}},
            ctx={1: 1, 2: 1},
            log={
                Val(ts=(1, 1), row=(1, 1), name="v", val=None),
                Val(ts=(1, 2), row=(1, 2), name="v", val=None),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_multi_col_conflicting_keys(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "b.bak.db") as b_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(a integer, b integer, PRIMARY KEY(a, b));")
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        crr.clone_to(a, b, id=2)
        exec(a, "INSERT INTO X VALUES(1, 2)")
        exec(a, "INSERT INTO X VALUES(1, 3)")
        exec(b, "INSERT INTO X VALUES(1, 2)")
        exec(b, "INSERT INTO X VALUES(1, 4)")
        b.backup(b_bak)

        crr.pull_from(b, tmp_path / "a.db")
        assert crr_from(b) == Crr(
            tbls={"X": {(1, 2, (1, 1)), (1, 3, (2, 1)), (1, 4, (2, 2))}},
            ctx={1: 2, 2: 3},
            log={
                Val(ts=(1, 1), row=(1, 1), name="a", val=1),
                Val(ts=(1, 1), row=(1, 1), name="b", val=2),
                Val(ts=(2, 1), row=(2, 1), name="a", val=1),
                Val(ts=(2, 1), row=(2, 1), name="b", val=3),
                Val(ts=(1, 2), row=(1, 2), name="a", val=1),
                Val(ts=(1, 2), row=(1, 2), name="b", val=2),
                Val(ts=(2, 2), row=(2, 2), name="a", val=1),
                Val(ts=(2, 2), row=(2, 2), name="b", val=4),
                Undo(ts=(3, 2), obj=(1, 2), ul=1),
            },
        )

        crr.pull_from(a, tmp_path / "b.bak.db")
        assert crr_from(a) == Crr(
            tbls={"X": {(1, 2, (1, 1)), (1, 3, (2, 1)), (1, 4, (2, 2))}},
            ctx={1: 3, 2: 2},
            log={
                Val(ts=(1, 1), row=(1, 1), name="a", val=1),
                Val(ts=(1, 1), row=(1, 1), name="b", val=2),
                Val(ts=(2, 1), row=(2, 1), name="a", val=1),
                Val(ts=(2, 1), row=(2, 1), name="b", val=3),
                Val(ts=(1, 2), row=(1, 2), name="a", val=1),
                Val(ts=(1, 2), row=(1, 2), name="b", val=2),
                Val(ts=(2, 2), row=(2, 2), name="a", val=1),
                Val(ts=(2, 2), row=(2, 2), name="b", val=4),
                Undo(ts=(3, 1), obj=(1, 2), ul=1),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_multi_col_multi_covering_unique(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "b.bak.db") as b_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(a int, b int, c int, PRIMARY KEY(a,b), UNIQUE(b,c));")
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        crr.clone_to(a, b, id=2)
        exec(a, "INSERT INTO X VALUES(1, 2, 3)")
        exec(b, "INSERT INTO X VALUES(1, 4, 3)")
        b.backup(b_bak)

        crr.pull_from(b, tmp_path / "a.db")
        assert crr_from(b) == Crr(
            tbls={"X": {(1, 2, 3, (1, 1)), (1, 4, 3, (1, 2))}},
            ctx={1: 1, 2: 1},
            log={
                Val(ts=(1, 1), row=(1, 1), name="a", val=1),
                Val(ts=(1, 1), row=(1, 1), name="b", val=2),
                Val(ts=(1, 1), row=(1, 1), name="c", val=3),
                Val(ts=(1, 2), row=(1, 2), name="a", val=1),
                Val(ts=(1, 2), row=(1, 2), name="b", val=4),
                Val(ts=(1, 2), row=(1, 2), name="c", val=3),
            },
        )

        crr.pull_from(a, tmp_path / "b.bak.db")
        assert crr_from(a) == Crr(
            tbls={"X": {(1, 2, 3, (1, 1)), (1, 4, 3, (1, 2))}},
            ctx={1: 1, 2: 1},
            log={
                Val(ts=(1, 1), row=(1, 1), name="a", val=1),
                Val(ts=(1, 1), row=(1, 1), name="b", val=2),
                Val(ts=(1, 1), row=(1, 1), name="c", val=3),
                Val(ts=(1, 2), row=(1, 2), name="a", val=1),
                Val(ts=(1, 2), row=(1, 2), name="b", val=4),
                Val(ts=(1, 2), row=(1, 2), name="c", val=3),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_conflicting_unique_fk(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "b.bak.db") as b_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x int PRIMARY KEY);")
        exec(a, "CREATE TABLE Y(x int CONSTRAINT fk REFERENCES X(x) PRIMARY KEY);")
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        exec(a, "INSERT INTO X VALUES(1)")
        crr.clone_to(a, b, id=2)
        exec(a, "INSERT INTO Y VALUES(1)")
        exec(b, "INSERT INTO Y VALUES(1)")
        b.backup(b_bak)

        crr.pull_from(b, tmp_path / "a.db")
        assert crr_from(b) == Crr(
            tbls={"X": {(1, (1, 1))}, "Y": {(1, (2, 1))}},
            ctx={1: 2, 2: 3},
            log={
                Val(ts=(1, 1), row=(1, 1), name="x", val=1),
                Val(ts=(2, 1), row=(2, 1), name="fk", val=(1, 1)),
                Val(ts=(2, 2), row=(2, 2), name="fk", val=(1, 1)),
                Undo(ts=(3, 2), obj=(2, 2), ul=1),
            },
        )

        crr.pull_from(a, tmp_path / "b.bak.db")
        assert crr_from(a) == Crr(
            tbls={"X": {(1, (1, 1))}, "Y": {(1, (2, 1))}},
            ctx={1: 3, 2: 2},
            log={
                Val(ts=(1, 1), row=(1, 1), name="x", val=1),
                Val(ts=(2, 1), row=(2, 1), name="fk", val=(1, 1)),
                Val(ts=(2, 2), row=(2, 2), name="fk", val=(1, 1)),
                Undo(ts=(3, 1), obj=(2, 2), ul=1),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_multi_col_fk_multi_covering_unique(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "b.bak.db") as b_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x int PRIMARY KEY);")
        exec(
            a,
            "CREATE TABLE Y(y int PRIMARY KEY, x int CONSTRAINT fk REFERENCES X(x), UNIQUE(y, x));",
        )
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        exec(a, "INSERT INTO X VALUES(1)")
        crr.clone_to(a, b, id=2)
        exec(a, "INSERT INTO Y VALUES(1, 1)")
        exec(b, "INSERT INTO Y VALUES(1, 1)")
        exec(b, "INSERT INTO Y VALUES(2, 1)")
        b.backup(b_bak)

        crr.pull_from(b, tmp_path / "a.db")
        assert crr_from(b) == Crr(
            tbls={"X": {(1, (1, 1))}, "Y": {(1, 1, (2, 1)), (2, 1, (3, 2))}},
            ctx={1: 2, 2: 4},
            log={
                Val(ts=(1, 1), row=(1, 1), name="x", val=1),
                Val(ts=(2, 1), row=(2, 1), name="y", val=1),
                Val(ts=(2, 1), row=(2, 1), name="fk", val=(1, 1)),
                Val(ts=(2, 2), row=(2, 2), name="y", val=1),
                Val(ts=(2, 2), row=(2, 2), name="fk", val=(1, 1)),
                Val(ts=(3, 2), row=(3, 2), name="y", val=2),
                Val(ts=(3, 2), row=(3, 2), name="fk", val=(1, 1)),
                Undo(ts=(4, 2), obj=(2, 2), ul=1),
            },
        )

        crr.pull_from(a, tmp_path / "b.bak.db")
        assert crr_from(a) == Crr(
            tbls={"X": {(1, (1, 1))}, "Y": {(1, 1, (2, 1)), (2, 1, (3, 2))}},
            ctx={1: 4, 2: 3},
            log={
                Val(ts=(1, 1), row=(1, 1), name="x", val=1),
                Val(ts=(2, 1), row=(2, 1), name="y", val=1),
                Val(ts=(2, 1), row=(2, 1), name="fk", val=(1, 1)),
                Val(ts=(2, 2), row=(2, 2), name="y", val=1),
                Val(ts=(2, 2), row=(2, 2), name="fk", val=(1, 1)),
                Val(ts=(3, 2), row=(3, 2), name="y", val=2),
                Val(ts=(3, 2), row=(3, 2), name="fk", val=(1, 1)),
                Undo(ts=(4, 1), obj=(2, 2), ul=1),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_past_conflicting_keys(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "b.bak.db") as b_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(v text PRIMARY KEY);")
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        crr.clone_to(a, b, id=2)
        exec(a, "INSERT INTO X VALUES('v1')")
        exec(a, "UPDATE X SET v = 'v2'")
        exec(b, "INSERT INTO X VALUES('v1')")
        b.backup(b_bak)

        crr.pull_from(b, tmp_path / "a.db")
        assert crr_from(b) == Crr(
            tbls={"X": {("v2", (1, 1)), ("v1", (1, 2))}},
            ctx={1: 2, 2: 1},
            log={
                Val(ts=(1, 1), row=(1, 1), name="v", val="v1"),
                Val(ts=(2, 1), row=(1, 1), name="v", val="v2"),
                Val(ts=(1, 2), row=(1, 2), name="v", val="v1"),
            },
        )

        crr.pull_from(a, tmp_path / "b.bak.db")
        assert crr_from(a) == Crr(
            tbls={"X": {("v2", (1, 1)), ("v1", (1, 2))}},
            ctx={1: 2, 2: 1},
            log={
                Val(ts=(1, 1), row=(1, 1), name="v", val="v1"),
                Val(ts=(2, 1), row=(1, 1), name="v", val="v2"),
                Val(ts=(1, 2), row=(1, 2), name="v", val="v1"),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_conflicting_3keys(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "b.bak.db") as b_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(u text PRIMARY KEY, v text UNIQUE);")
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        crr.clone_to(a, b, id=2)
        exec(a, "INSERT INTO X VALUES('u1', 'v1')")
        exec(a, "INSERT INTO X VALUES('u2', 'v2')")
        exec(b, "INSERT INTO X VALUES('u1', 'v2')")
        b.backup(b_bak)

        crr.pull_from(b, tmp_path / "a.db")
        assert crr_from(b) == Crr(
            tbls={"X": {("u1", "v1", (1, 1))}},
            ctx={1: 2, 2: 3},
            log={
                Val(ts=(1, 1), row=(1, 1), name="u", val="u1"),
                Val(ts=(1, 1), row=(1, 1), name="v", val="v1"),
                Val(ts=(2, 1), row=(2, 1), name="u", val="u2"),
                Val(ts=(2, 1), row=(2, 1), name="v", val="v2"),
                Val(ts=(1, 2), row=(1, 2), name="u", val="u1"),
                Val(ts=(1, 2), row=(1, 2), name="v", val="v2"),
                Undo(ts=(3, 2), obj=(1, 2), ul=1),
                Undo(ts=(3, 2), obj=(2, 1), ul=1),
            },
        )

        crr.pull_from(a, tmp_path / "b.bak.db")
        assert crr_from(a) == Crr(
            tbls={"X": {("u1", "v1", (1, 1))}},
            ctx={1: 3, 2: 1},
            log={
                Val(ts=(1, 1), row=(1, 1), name="u", val="u1"),
                Val(ts=(1, 1), row=(1, 1), name="v", val="v1"),
                Val(ts=(2, 1), row=(2, 1), name="u", val="u2"),
                Val(ts=(2, 1), row=(2, 1), name="v", val="v2"),
                Val(ts=(1, 2), row=(1, 2), name="u", val="u1"),
                Val(ts=(1, 2), row=(1, 2), name="v", val="v2"),
                Undo(ts=(3, 1), obj=(1, 2), ul=1),
                Undo(ts=(3, 1), obj=(2, 1), ul=1),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_concur_del_fk_restrict_aliased_rowid(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "a.bak.db") as a_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x integer PRIMARY KEY)")
        exec(
            a,
            "CREATE TABLE Y(y integer PRIMARY KEY, x integer CONSTRAINT fk REFERENCES X(x) ON DELETE RESTRICT)",
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
            ctx={1: 3, 2: 2},
            log={
                Val(ts=(2, 2), row=(2, 2), name="fk", val=(1, 1)),
                Undo(ts=(3, 1), obj=(1, 1), ul=2),
            },
        )

        crr.pull_from(b, tmp_path / "a.bak.db")
        assert crr_from(b) == Crr(
            tbls={"X": {(1, (1, 1))}, "Y": {(1, 1, (2, 2))}},
            ctx={1: 2, 2: 3},
            log={
                Val(ts=(2, 2), row=(2, 2), name="fk", val=(1, 1)),
                Undo(ts=(3, 2), obj=(1, 1), ul=2),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_concur_past_del_fk_restrict(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "a.bak.db") as a_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x integer PRIMARY KEY)")
        exec(
            a,
            "CREATE TABLE Y(y integer PRIMARY KEY, x integer CONSTRAINT fk REFERENCES X(x) ON DELETE RESTRICT)",
        )
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        exec(a, "INSERT INTO X VALUES(1)")
        crr.clone_to(a, b, id=2)
        exec(a, "DELETE FROM X")
        a.backup(a_bak)
        exec(b, "INSERT INTO Y VALUES(1, 1)")
        exec(b, "INSERT INTO X VALUES(2)")
        exec(b, "UPDATE Y SET x = 2")

        crr.pull_from(a, tmp_path / "b.db")
        assert crr_from(a) == Crr(
            tbls={"X": {(1, (3, 2))}, "Y": {(1, 1, (2, 2))}},
            ctx={1: 2, 2: 4},
            log={
                Undo(ts=(2, 1), obj=(1, 1), ul=1),
                Val(ts=(2, 2), row=(2, 2), name="fk", val=(1, 1)),
                Val(ts=(4, 2), row=(2, 2), name="fk", val=(3, 2)),
            },
        )

        crr.pull_from(b, tmp_path / "a.bak.db")
        assert crr_from(b) == Crr(
            tbls={"X": {(2, (3, 2))}, "Y": {(1, 2, (2, 2))}},
            ctx={1: 2, 2: 4},
            log={
                Undo(ts=(2, 1), obj=(1, 1), ul=1),
                Val(ts=(2, 2), row=(2, 2), name="fk", val=(1, 1)),
                Val(ts=(4, 2), row=(2, 2), name="fk", val=(3, 2)),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_concur_del_fk_restrict_repl_pk(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "a.bak.db") as a_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x int PRIMARY KEY)")
        exec(
            a,
            "CREATE TABLE Y(y integer PRIMARY KEY, x integer CONSTRAINT fk REFERENCES X(x) ON DELETE RESTRICT)",
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
            ctx={1: 3, 2: 2},
            log={
                Val(ts=(1, 1), row=(1, 1), name="x", val=1),
                Val(ts=(2, 2), row=(2, 2), name="fk", val=(1, 1)),
                Undo(ts=(3, 1), obj=(1, 1), ul=2),
            },
        )

        crr.pull_from(b, tmp_path / "a.bak.db")
        assert crr_from(b) == Crr(
            tbls={"X": {(1, (1, 1))}, "Y": {(1, 1, (2, 2))}},
            ctx={1: 2, 2: 3},
            log={
                Val(ts=(1, 1), row=(1, 1), name="x", val=1),
                Val(ts=(2, 2), row=(2, 2), name="fk", val=(1, 1)),
                Undo(ts=(3, 2), obj=(1, 1), ul=2),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_concur_del_fk_restrict_rec(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "a.bak.db") as a_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x integer PRIMARY KEY)")
        exec(
            a,
            "CREATE TABLE Y(y integer PRIMARY KEY, x integer CONSTRAINT fk1 REFERENCES X(x) ON DELETE CASCADE)",
        )
        exec(
            a,
            "CREATE TABLE Z(z integer PRIMARY KEY, y integer CONSTRAINT fk2 REFERENCES Y(y) ON DELETE RESTRICT)",
        )
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        exec(a, "INSERT INTO X VALUES(1)")
        crr.clone_to(a, b, id=2)
        exec(a, "DELETE FROM X")
        a.backup(a_bak)
        exec(b, "INSERT INTO Y VALUES(1, 1)")
        exec(b, "INSERT INTO Z VALUES(1, 1)")

        crr.pull_from(a, tmp_path / "b.db")
        assert crr_from(a) == Crr(
            tbls={"X": {(1, (1, 1))}, "Y": {(1, 1, (2, 2))}, "Z": {(1, 1, (3, 2))}},
            ctx={1: 4, 2: 3},
            log={
                Val(ts=(2, 2), row=(2, 2), name="fk1", val=(1, 1)),
                Val(ts=(3, 2), row=(3, 2), name="fk2", val=(2, 2)),
                Undo(ts=(4, 1), obj=(1, 1), ul=2),
            },
        )

        crr.pull_from(b, tmp_path / "a.bak.db")
        assert crr_from(b) == Crr(
            tbls={"X": {(1, (1, 1))}, "Y": {(1, 1, (2, 2))}, "Z": {(1, 1, (3, 2))}},
            ctx={1: 2, 2: 4},
            log={
                Val(ts=(2, 2), row=(2, 2), name="fk1", val=(1, 1)),
                Val(ts=(3, 2), row=(3, 2), name="fk2", val=(2, 2)),
                Undo(ts=(4, 2), obj=(1, 1), ul=2),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_concur_del_fk_cascade(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "a.bak.db") as a_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x integer PRIMARY KEY)")
        exec(
            a,
            "CREATE TABLE Y(y integer PRIMARY KEY, x integer CONSTRAINT fk REFERENCES X(x) ON DELETE CASCADE)",
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
            ctx={1: 3, 2: 2},
            log={
                Undo(ts=(2, 1), obj=(1, 1), ul=1),
                Val(ts=(2, 2), row=(2, 2), name="fk", val=(1, 1)),
                Undo(ts=(3, 1), obj=(2, 2), ul=1),
            },
        )

        crr.pull_from(b, tmp_path / "a.bak.db")
        assert crr_from(b) == Crr(
            tbls={"X": set(), "Y": set()},
            ctx={1: 2, 2: 3},
            log={
                Undo(ts=(2, 1), obj=(1, 1), ul=1),
                Val(ts=(2, 2), row=(2, 2), name="fk", val=(1, 1)),
                Undo(ts=(3, 2), obj=(2, 2), ul=1),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_concur_del_fk_set_null(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "a.bak.db") as a_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x integer PRIMARY KEY)")
        exec(
            a,
            "CREATE TABLE Y(y integer PRIMARY KEY, x integer CONSTRAINT fk REFERENCES X ON DELETE SET NULL)",
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
            ctx={1: 2, 2: 2},
            log={
                Undo(ts=(2, 1), obj=(1, 1), ul=1),
                Val(ts=(2, 2), row=(2, 2), name="fk", val=(1, 1)),
            },
        )

        crr.pull_from(b, tmp_path / "a.bak.db")
        assert crr_from(b) == Crr(
            tbls={"X": set(), "Y": {(1, None, (2, 2))}},
            ctx={1: 2, 2: 2},
            log={
                Undo(ts=(2, 1), obj=(1, 1), ul=1),
                Val(ts=(2, 2), row=(2, 2), name="fk", val=(1, 1)),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_concur_del_fk_set_null_repl_col(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "a.bak.db") as a_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x int PRIMARY KEY)")
        exec(
            a,
            "CREATE TABLE Y(y integer PRIMARY KEY, x int CONSTRAINT fk REFERENCES X ON DELETE SET NULL)",
        )
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        exec(a, "INSERT INTO X VALUES(1)")
        crr.clone_to(a, b, id=2)
        exec(a, "DELETE FROM X")
        a.backup(a_bak)
        exec(b, "INSERT INTO Y VALUES(1, 1)")

        exec(a, "PRAGMA foreign_keys=OFF")
        crr.pull_from(a, tmp_path / "b.db")
        assert crr_from(a) == Crr(
            tbls={"X": set(), "Y": {(1, None, (2, 2))}},
            ctx={1: 2, 2: 2},
            log={
                Val(ts=(1, 1), row=(1, 1), name="x", val=1),
                Undo(ts=(2, 1), obj=(1, 1), ul=1),
                Val(ts=(2, 2), row=(2, 2), name="fk", val=(1, 1)),
            },
        )

        crr.pull_from(b, tmp_path / "a.bak.db")
        assert crr_from(b) == Crr(
            tbls={"X": set(), "Y": {(1, None, (2, 2))}},
            ctx={1: 2, 2: 2},
            log={
                Val(ts=(1, 1), row=(1, 1), name="x", val=1),
                Undo(ts=(2, 1), obj=(1, 1), ul=1),
                Val(ts=(2, 2), row=(2, 2), name="fk", val=(1, 1)),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_concur_up_fk_restrict(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "a.bak.db") as a_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x int PRIMARY KEY)")
        exec(
            a,
            "CREATE TABLE Y(y integer PRIMARY KEY, x integer CONSTRAINT fk REFERENCES X(x) ON UPDATE RESTRICT)",
        )
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        exec(a, "INSERT INTO X VALUES(1)")
        crr.clone_to(a, b, id=2)
        exec(a, "UPDATE X SET x=2")
        a.backup(a_bak)
        exec(b, "INSERT INTO Y VALUES(1, 1)")

        crr.pull_from(a, tmp_path / "b.db")
        assert crr_from(a) == Crr(
            tbls={"X": {(1, (1, 1))}, "Y": {(1, 1, (2, 2))}},
            ctx={1: 3, 2: 2},
            log={
                Val(ts=(1, 1), row=(1, 1), name="x", val=1),
                Val(ts=(2, 1), row=(1, 1), name="x", val=2),
                Val(ts=(2, 2), row=(2, 2), name="fk", val=(1, 1)),
                Undo(ts=(3, 1), obj=(2, 1), ul=1),
            },
        )

        crr.pull_from(b, tmp_path / "a.bak.db")
        assert crr_from(b) == Crr(
            tbls={"X": {(1, (1, 1))}, "Y": {(1, 1, (2, 2))}},
            ctx={1: 2, 2: 3},
            log={
                Val(ts=(1, 1), row=(1, 1), name="x", val=1),
                Val(ts=(2, 1), row=(1, 1), name="x", val=2),
                Val(ts=(2, 2), row=(2, 2), name="fk", val=(1, 1)),
                Undo(ts=(3, 2), obj=(2, 1), ul=1),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_concur_up2_fk_restrict(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "a.bak.db") as a_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x int PRIMARY KEY)")
        exec(
            a,
            "CREATE TABLE Y(y integer PRIMARY KEY, x integer CONSTRAINT fk REFERENCES X(x) ON UPDATE RESTRICT)",
        )
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        exec(a, "INSERT INTO X VALUES(1)")
        crr.clone_to(a, b, id=2)
        exec(a, "UPDATE X SET x=2")
        exec(a, "UPDATE X SET x=3")
        a.backup(a_bak)
        exec(b, "INSERT INTO Y VALUES(1, 1)")

        crr.pull_from(a, tmp_path / "b.db")
        assert crr_from(a) == Crr(
            tbls={"X": {(1, (1, 1))}, "Y": {(1, 1, (2, 2))}},
            ctx={1: 4, 2: 2},
            log={
                Val(ts=(1, 1), row=(1, 1), name="x", val=1),
                Val(ts=(2, 1), row=(1, 1), name="x", val=2),
                Val(ts=(3, 1), row=(1, 1), name="x", val=3),
                Val(ts=(2, 2), row=(2, 2), name="fk", val=(1, 1)),
                Undo(ts=(4, 1), obj=(2, 1), ul=1),
                Undo(ts=(4, 1), obj=(3, 1), ul=1),
            },
        )

        crr.pull_from(b, tmp_path / "a.bak.db")
        assert crr_from(b) == Crr(
            tbls={"X": {(1, (1, 1))}, "Y": {(1, 1, (2, 2))}},
            ctx={1: 3, 2: 4},
            log={
                Val(ts=(1, 1), row=(1, 1), name="x", val=1),
                Val(ts=(2, 1), row=(1, 1), name="x", val=2),
                Val(ts=(3, 1), row=(1, 1), name="x", val=3),
                Val(ts=(2, 2), row=(2, 2), name="fk", val=(1, 1)),
                Undo(ts=(4, 2), obj=(2, 1), ul=1),
                Undo(ts=(4, 2), obj=(3, 1), ul=1),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_concur_up_fk_cascade(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "a.bak.db") as a_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x int PRIMARY KEY)")
        exec(
            a,
            "CREATE TABLE Y(y integer PRIMARY KEY, x integer CONSTRAINT fk REFERENCES X(x) ON UPDATE CASCADE)",
        )
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        exec(a, "INSERT INTO X VALUES(1)")
        crr.clone_to(a, b, id=2)
        exec(a, "UPDATE X SET x=2")
        a.backup(a_bak)
        exec(b, "INSERT INTO Y VALUES(1, 1)")

        crr.pull_from(a, tmp_path / "b.db")
        assert crr_from(a) == Crr(
            tbls={"X": {(2, (1, 1))}, "Y": {(1, 2, (2, 2))}},
            ctx={1: 2, 2: 2},
            log={
                Val(ts=(1, 1), row=(1, 1), name="x", val=1),
                Val(ts=(2, 1), row=(1, 1), name="x", val=2),
                Val(ts=(2, 2), row=(2, 2), name="fk", val=(1, 1)),
            },
        )

        crr.pull_from(b, tmp_path / "a.bak.db")
        assert crr_from(b) == Crr(
            tbls={"X": {(2, (1, 1))}, "Y": {(1, 2, (2, 2))}},
            ctx={1: 2, 2: 2},
            log={
                Val(ts=(1, 1), row=(1, 1), name="x", val=1),
                Val(ts=(2, 1), row=(1, 1), name="x", val=2),
                Val(ts=(2, 2), row=(2, 2), name="fk", val=(1, 1)),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_concur_up_fk_set_null(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "a.bak.db") as a_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x int PRIMARY KEY)")
        exec(
            a,
            "CREATE TABLE Y(y integer PRIMARY KEY, x integer CONSTRAINT fk REFERENCES X(x) ON UPDATE SET NULL)",
        )
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        exec(a, "INSERT INTO X VALUES(1)")
        crr.clone_to(a, b, id=2)
        exec(a, "UPDATE X SET x=2")
        a.backup(a_bak)
        exec(b, "INSERT INTO Y VALUES(1, 1)")

        crr.pull_from(a, tmp_path / "b.db")
        assert crr_from(a) == Crr(
            tbls={"X": {(2, (1, 1))}, "Y": {(1, None, (2, 2))}},
            ctx={1: 4, 2: 2},
            log={
                Val(ts=(1, 1), row=(1, 1), name="x", val=1),
                Val(ts=(2, 1), row=(1, 1), name="x", val=2),
                Val(ts=(2, 2), row=(2, 2), name="fk", val=(1, 1)),
                Val(ts=(4, 1), row=(2, 2), name="fk", val=(None, None)),
            },
        )

        crr.pull_from(b, tmp_path / "a.bak.db")
        assert crr_from(b) == Crr(
            tbls={"X": {(2, (1, 1))}, "Y": {(1, None, (2, 2))}},
            ctx={1: 2, 2: 4},
            log={
                Val(ts=(1, 1), row=(1, 1), name="x", val=1),
                Val(ts=(2, 1), row=(1, 1), name="x", val=2),
                Val(ts=(2, 2), row=(2, 2), name="fk", val=(1, 1)),
                Val(ts=(4, 2), row=(2, 2), name="fk", val=(None, None)),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_concur_complex_1(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "a.bak.db") as a_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE X(x int PRIMARY KEY)")
        exec(
            a,
            "CREATE TABLE Y(x int PRIMARY KEY CONSTRAINT fk REFERENCES X(x) ON DELETE RESTRICT ON UPDATE CASCADE)",
        )
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        exec(a, "INSERT INTO X VALUES(1)")
        crr.clone_to(a, b, id=2)
        exec(a, "UPDATE X SET x=2")
        exec(a, "DELETE FROM X")
        a.backup(a_bak)
        exec(b, "INSERT INTO Y(x) VALUES(1)")
        exec(b, "INSERT INTO X VALUES(2)")

        crr.pull_from(a, tmp_path / "b.db")
        assert crr_from(a) == Crr(
            tbls={"X": {(2, (1, 1))}, "Y": {(2, (2, 2))}},
            ctx={1: 4, 2: 3},
            log={
                Val(ts=(1, 1), row=(1, 1), name="x", val=1),
                Val(ts=(2, 1), row=(1, 1), name="x", val=2),
                Val(ts=(2, 2), row=(2, 2), name="fk", val=(1, 1)),
                Val(ts=(3, 2), row=(3, 2), name="x", val=2),
                Undo(ts=(4, 1), obj=(1, 1), ul=2),
                Undo(ts=(4, 1), obj=(3, 2), ul=1),
            },
        )

        crr.pull_from(b, tmp_path / "a.bak.db")
        assert crr_from(b) == Crr(
            tbls={"X": {(2, (1, 1))}, "Y": {(2, (2, 2))}},
            ctx={1: 3, 2: 4},
            log={
                Val(ts=(1, 1), row=(1, 1), name="x", val=1),
                Val(ts=(2, 1), row=(1, 1), name="x", val=2),
                Val(ts=(2, 2), row=(2, 2), name="fk", val=(1, 1)),
                Val(ts=(3, 2), row=(3, 2), name="x", val=2),
                Undo(ts=(4, 2), obj=(1, 1), ul=2),
                Undo(ts=(4, 2), obj=(3, 2), ul=1),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_concur_complex_2(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a, sqlite3.connect(
        tmp_path / "b.db"
    ) as b, sqlite3.connect(tmp_path / "a.bak.db") as a_bak:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, "CREATE TABLE book(id integer PRIMARY KEY AUTOINCREMENT, name text)")
        exec(
            a, "CREATE TABLE publisher(id integer PRIMARY KEY AUTOINCREMENT, name text)"
        )
        exec(a, "CREATE TABLE store(id integer PRIMARY KEY AUTOINCREMENT, name text)")
        exec(
            a,
            """CREATE TABLE published_book(
                book_id integer CONSTRAINT book_fk REFERENCES book,
                publisher_id integer CONSTRAINT publisher_fk REFERENCES publisher,
                PRIMARY KEY(book_id, publisher_id)
            )""",
        )
        exec(
            a,
            """CREATE TABLE availability(
                book_id integer,
                publisher_id integer,
                store_id integer CONSTRAINT store_fk REFERENCES store,
                count integer,
                PRIMARY KEY(book_id, publisher_id, store_id),
                CONSTRAINT published_book_fk FOREIGN KEY(book_id, publisher_id) REFERENCES published_book
            )""",
        )
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        exec(a, "INSERT INTO book(name) VALUES ('B1'), ('B2')")
        exec(a, "INSERT INTO publisher(name) VALUES ('P1'), ('P2')")
        crr.clone_to(a, b, id=2)
        exec(a, "INSERT INTO store(name) VALUES('S1')")
        exec(a, "INSERT INTO published_book VALUES(1, 1)")
        exec(a, "INSERT INTO availability VALUES(1, 1, 1, 4)")
        a.backup(a_bak)
        exec(b, "INSERT INTO store(name) VALUES('S2')")
        exec(b, "INSERT INTO published_book VALUES(1, 2)")
        exec(b, "INSERT INTO availability VALUES(1, 2, 1, 5)")

        crr.pull_from(a, tmp_path / "b.db")
        assert crr_from(a) == Crr(
            tbls={
                "book": {(1, "B1", (1, 1)), (2, "B2", (2, 1))},
                "publisher": {(1, "P1", (3, 1)), (2, "P2", (4, 1))},
                "store": {(1, "S1", (5, 1)), (2, "S2", (5, 2))},
                "published_book": {(1, 1, (6, 1)), (1, 2, (6, 2))},
                "availability": {(1, 1, 1, 4, (7, 1)), (1, 2, 2, 5, (7, 2))},
            },
            ctx={1: 7, 2: 7},
            log={
                Val(ts=(1, 1), row=(1, 1), name="name", val="B1"),
                Val(ts=(2, 1), row=(2, 1), name="name", val="B2"),
                Val(ts=(3, 1), row=(3, 1), name="name", val="P1"),
                Val(ts=(4, 1), row=(4, 1), name="name", val="P2"),
                Val(ts=(5, 1), row=(5, 1), name="name", val="S1"),
                Val(ts=(5, 2), row=(5, 2), name="name", val="S2"),
                Val(ts=(6, 1), row=(6, 1), name="book_fk", val=(1, 1)),
                Val(ts=(6, 1), row=(6, 1), name="publisher_fk", val=(3, 1)),
                Val(ts=(6, 2), row=(6, 2), name="book_fk", val=(1, 1)),
                Val(ts=(6, 2), row=(6, 2), name="publisher_fk", val=(4, 1)),
                Val(ts=(7, 1), row=(7, 1), name="published_book_fk", val=(6, 1)),
                Val(ts=(7, 1), row=(7, 1), name="store_fk", val=(5, 1)),
                Val(ts=(7, 1), row=(7, 1), name="count", val=4),
                Val(ts=(7, 2), row=(7, 2), name="published_book_fk", val=(6, 2)),
                Val(ts=(7, 2), row=(7, 2), name="store_fk", val=(5, 2)),
                Val(ts=(7, 2), row=(7, 2), name="count", val=5),
            },
        )

        crr.pull_from(b, tmp_path / "a.bak.db")
        assert crr_from(b) == Crr(
            tbls={
                "book": {(1, "B1", (1, 1)), (2, "B2", (2, 1))},
                "publisher": {(1, "P1", (3, 1)), (2, "P2", (4, 1))},
                "store": {(2, "S1", (5, 1)), (1, "S2", (5, 2))},
                "published_book": {(1, 1, (6, 1)), (1, 2, (6, 2))},
                "availability": {(1, 1, 2, 4, (7, 1)), (1, 2, 1, 5, (7, 2))},
            },
            ctx={1: 7, 2: 7},
            log={
                Val(ts=(1, 1), row=(1, 1), name="name", val="B1"),
                Val(ts=(2, 1), row=(2, 1), name="name", val="B2"),
                Val(ts=(3, 1), row=(3, 1), name="name", val="P1"),
                Val(ts=(4, 1), row=(4, 1), name="name", val="P2"),
                Val(ts=(5, 1), row=(5, 1), name="name", val="S1"),
                Val(ts=(5, 2), row=(5, 2), name="name", val="S2"),
                Val(ts=(6, 1), row=(6, 1), name="book_fk", val=(1, 1)),
                Val(ts=(6, 1), row=(6, 1), name="publisher_fk", val=(3, 1)),
                Val(ts=(6, 2), row=(6, 2), name="book_fk", val=(1, 1)),
                Val(ts=(6, 2), row=(6, 2), name="publisher_fk", val=(4, 1)),
                Val(ts=(7, 1), row=(7, 1), name="published_book_fk", val=(6, 1)),
                Val(ts=(7, 1), row=(7, 1), name="store_fk", val=(5, 1)),
                Val(ts=(7, 1), row=(7, 1), name="count", val=4),
                Val(ts=(7, 2), row=(7, 2), name="published_book_fk", val=(6, 2)),
                Val(ts=(7, 2), row=(7, 2), name="store_fk", val=(5, 2)),
                Val(ts=(7, 2), row=(7, 2), name="count", val=5),
            },
        )
        exec(a, "PRAGMA integrity_check")


def test_spaced_names(tmp_path: pathlib.Path) -> None:
    with sqlite3.connect(tmp_path / "a.db") as a:
        exec(a, "PRAGMA foreign_keys=ON")
        exec(a, 'CREATE TABLE "X "("x " int PRIMARY KEY)')
        exec(
            a,
            'CREATE TABLE "Y "("x " int PRIMARY KEY CONSTRAINT fk REFERENCES "X "("x "))',
        )
        crr.init(a, id=1, conf=_DEFAULT_CONF)
        exec(a, 'INSERT INTO "X " VALUES(1)')
        exec(a, 'INSERT INTO "Y "("x ") VALUES(1)')
        assert crr_from(a) == Crr(
            tbls={"X ": {(1, (1, 1))}, "Y ": {(1, (2, 1))}},
            ctx={1: 2},
            log={
                Val(ts=(1, 1), row=(1, 1), name="x ", val=1),
                Val(ts=(2, 1), row=(2, 1), name="fk", val=(1, 1)),
            },
        )
        exec(a, "PRAGMA integrity_check")
