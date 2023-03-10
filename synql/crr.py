# Copyright (c) 2022 Inria, Victorien Elvinger
# Licensed under the MIT License (https://mit-license.org/)

"""
This module provides primitives to replicate a SQLite database.

It si based on a Git-like model where a database is first initialized to replicable database.
Once initialized, the database can be cloned (replicated).
Modifications can be concurrently performed.
Finally, a database can integrate the change of another database via a pull.

See the [unit tests](../test/test_crr.py) for code examples.
"""

import typing
import logging
import pathlib
import textwrap
from contextlib import closing
from dataclasses import dataclass
import pysqlite3 as sqlite3
from sqlschm.parser import parse_schema
from sqlschm import sql
from synql import sqlschm_utils as utils

logging.basicConfig(level=logging.DEBUG)


@dataclass(frozen=True, kw_only=True, slots=True)
class Config:
    """Configuration to change Synql behavior."""

    physical_clock: bool = True
    no_action_is_cascade: bool = False


def init(
    db: sqlite3.Connection, /, *, replica_id: int | None = None, conf: Config = Config()
) -> None:
    """Make a database a replicable database.

    Once initialized, a database can be cloned, and can pull another a replica.
    See `clone_to` and `pull_from` functions."""

    sql_ar_schema = _get_schema(db)
    tables = sql.symbols(parse_schema(sql_ar_schema))
    with closing(db.cursor()) as cursor:
        cursor.executescript(
            _CREATE_TABLE_CONTEXT
            + _CREATE_REPLICATION_TABLES
            + _CREATE_LOCAL_TABLES_VIEWS
        )
        cursor.executescript(_synql_triggers(tables, conf))
        if not conf.physical_clock:
            cursor.execute("DROP TRIGGER _synql_local_clock;")
    _allocate_id(db, replica_id=replica_id)


def clone_to(
    source: sqlite3.Connection,
    target: sqlite3.Connection,
    /,
    *,
    replica_id: int | None = None,
) -> None:
    """Clone `source` to `target` using `replica_id` as `target` replica identifier.

    If `replica_id` is not provided, a random identifier is generated."""
    source.commit()
    source.backup(target)
    _allocate_id(target, replica_id=replica_id)


def pull_from(db: sqlite3.Connection, remote_db_path: pathlib.Path | str, /) -> None:
    """Pull state of `remote_db_path` in `db`."""
    sql_ar_schema = _get_schema(db)
    tables = sql.symbols(parse_schema(sql_ar_schema))
    merging = _create_pull(tables)
    result = f"""
        PRAGMA defer_foreign_keys = ON;  -- automatically switch off at the end of transaction

        ATTACH DATABASE '{remote_db_path}' AS extern;

        UPDATE _synql_local SET is_merging = 1;

        {_PULL_EXTERN}
        {_CONFLICT_RESOLUTION}
        {merging}
        {_MERGE_END}

        UPDATE _synql_local SET is_merging = 0;

        DETACH DATABASE extern;
        """
    result = textwrap.dedent(result)
    with closing(db.cursor()) as cursor:
        cursor.executescript(result)


def fingerprint(db: sqlite3.Connection, fp_path: pathlib.Path | str, /) -> None:
    """Write the causal context of `db` to `fp_path`.

    This can be used as a fingerprint in order to compute a delta on another replica.
    Note that an entire database can behave like a fingerprint."""
    db.commit()
    with sqlite3.connect(fp_path) as target, closing(target.cursor()) as cursor:
        cursor.executescript(_CREATE_TABLE_CONTEXT)
    script = f"""
    ATTACH DATABASE '{fp_path}' AS extern;
    INSERT INTO extern._synql_context SELECT * FROM _synql_context;
    DETACH DATABASE extern;
    """
    db.executescript(script)


def delta(
    db: sqlite3.Connection,
    fp_path: pathlib.Path | str,
    delta_path: pathlib.Path | str,
    /,
) -> None:
    """Compute a delta from a fingerprint.

    Note that an entire database can behave like a fingerprint and a delta."""

    db.commit()
    with sqlite3.connect(delta_path) as delta_db, closing(delta_db.cursor()) as cursor:
        cursor.executescript(_CREATE_TABLE_CONTEXT + _CREATE_REPLICATION_TABLES)
    _script = f"""
    ATTACH DATABASE '{fp_path}' AS fp;
    ATTACH DATABASE '{delta_path}' AS delta;
    INSERT INTO extern._synql_context SELECT * FROM _synql_context;
    DETACH DATABASE delta;
    DETACH DATABASE fp;
    """

    raise NotImplementedError("Unfinished implementation")


def _allocate_id(db: sqlite3.Connection, /, *, replica_id: int | None = None) -> None:
    with closing(db.cursor()) as cursor:
        if replica_id is None:
            # Generate an identifier with 48 bits of entropy
            cursor.execute("UPDATE _synql_local SET peer = (random() >> 16);")
        else:
            cursor.execute(f"UPDATE _synql_local SET peer = {replica_id};")
        cursor.execute(
            "INSERT INTO _synql_context(peer, ts) SELECT peer, 0 FROM _synql_local;"
        )


# Encoded value in metadata of the database.
#
# sql.OnUpdateDelete.NO_ACTION, None should be normalized to sql.OnUpdateDelete.CASCADE or
# sql.OnUpdateDelete.RESTRICT.
# sql.OnUpdateDelete.SET_DEFAULT is not supported.
_FK_ACTION = {
    sql.OnUpdateDelete.CASCADE: 0,
    sql.OnUpdateDelete.RESTRICT: 1,
    sql.OnUpdateDelete.SET_NULL: 2,
}


def _normalize_fk_action(
    action: sql.OnUpdateDelete | None, conf: Config, /
) -> (
    typing.Literal[sql.OnUpdateDelete.CASCADE]
    | typing.Literal[sql.OnUpdateDelete.RESTRICT]
    | typing.Literal[sql.OnUpdateDelete.SET_NULL]
):
    """Normalize `NO ACTION` and `None` to `RESTRICT` or `CASCADE` according to `conf`."""

    if action is sql.OnUpdateDelete.SET_DEFAULT:
        raise NotImplementedError("Synql does not support ON DELETE/UPDATE SET DEFAULT")
    if action is None or action is sql.OnUpdateDelete.NO_ACTION:
        if conf.no_action_is_cascade:
            return sql.OnUpdateDelete.CASCADE
        return sql.OnUpdateDelete.RESTRICT
    return action


_SELECT_USER_TABLE_SCHEMA = """--sql
SELECT sql FROM sqlite_master WHERE (type = 'table' OR type = 'index') AND
    name NOT LIKE 'sqlite_%' AND name NOT LIKE '_synql_%';
"""

_CREATE_TABLE_CONTEXT = """-- Causal context
CREATE TABLE _synql_context(
    peer integer PRIMARY KEY,
    ts integer NOT NULL DEFAULT 0 CHECK (ts >= 0)
) STRICT;
"""

_CREATE_REPLICATION_TABLES = """
CREATE TABLE _synql_id(
    row_ts integer NOT NULL,
    row_peer integer NOT NULL REFERENCES _synql_context(peer) ON DELETE CASCADE ON UPDATE CASCADE,
    tbl integer NOT NULL,
    PRIMARY KEY(row_ts DESC, row_peer DESC),
    UNIQUE(row_peer, row_ts)
) STRICT, WITHOUT ROWID;

CREATE TABLE _synql_id_undo(
    row_ts integer NOT NULL,
    row_peer integer NOT NULL,
    ul integer NOT NULL DEFAULT 0 CHECK(ul >= 0), -- undo length
    ts integer NOT NULL CHECK(ts >= row_ts),
    peer integer NOT NULL,
    PRIMARY KEY(row_ts DESC, row_peer DESC),
    FOREIGN KEY(row_ts, row_peer) REFERENCES _synql_id(row_ts, row_peer)
        ON DELETE CASCADE ON UPDATE CASCADE
) STRICT, WITHOUT ROWID;
CREATE INDEX _synql_id_undo_index_ts ON _synql_id_undo(peer, ts);

CREATE TABLE _synql_log(
    ts integer NOT NULL CHECK(ts >= row_ts),
    peer integer NOT NULL,
    row_ts integer NOT NULL,
    row_peer integer NOT NULL,
    field integer NOT NULL,
    val any,
    PRIMARY KEY(row_ts, row_peer, field, ts, peer),
    FOREIGN KEY(row_ts, row_peer) REFERENCES _synql_id(row_ts, row_peer)
        ON DELETE CASCADE ON UPDATE CASCADE
) STRICT;
CREATE INDEX _synql_log_index_ts ON _synql_log(peer, ts);

CREATE TABLE _synql_fklog(
    ts integer NOT NULL CHECK(ts >= row_ts),
    peer integer NOT NULL,
    row_ts integer NOT NULL,
    row_peer integer NOT NULL,
    field integer NOT NULL,
    foreign_row_ts integer DEFAULT NULL,
    foreign_row_peer integer DEFAULT NULL,
    PRIMARY KEY(row_ts, row_peer, field, ts, peer),
    FOREIGN KEY(row_ts, row_peer) REFERENCES _synql_id(row_ts, row_peer)
        ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY(foreign_row_ts, foreign_row_peer) REFERENCES _synql_id(row_ts, row_peer)
        ON DELETE NO ACTION ON UPDATE CASCADE
) STRICT;
CREATE INDEX _synql_fklog_index_ts ON _synql_fklog(peer, ts);

CREATE TABLE _synql_undolog(
    obj_ts integer NOT NULL,
    obj_peer integer NOT NULL,
    ul integer NOT NULL DEFAULT 0 CHECK(ul >= 0), -- undo length
    ts integer NOT NULL CHECK(ts >= obj_ts),
    peer integer NOT NULL,
    PRIMARY KEY(obj_ts DESC, obj_peer DESC)
) STRICT, WITHOUT ROWID;
CREATE INDEX _synql_undolog_ts ON _synql_undolog(peer, ts);
"""

_CREATE_LOCAL_TABLES_VIEWS = """
CREATE TABLE _synql_local(
    id integer PRIMARY KEY DEFAULT 1 CHECK(id = 1),
    peer integer NOT NULL DEFAULT 0,
    ts integer NOT NULL DEFAULT 0 CHECK(ts >= 0),
    is_merging integer NOT NULL DEFAULT 0 CHECK(is_merging & 1 = is_merging)
) STRICT;
INSERT INTO _synql_local DEFAULT VALUES;

-- use `UPDATE _synql_local SET ts = ts + 1` to refresh the hybrid logical clock
CREATE TRIGGER          _synql_local_clock
AFTER UPDATE OF ts ON _synql_local WHEN (OLD.ts + 1 = NEW.ts)
BEGIN
    UPDATE _synql_local SET ts = max(NEW.ts, CAST(
        ((julianday('now') - julianday('1970-01-01')) * 86400.0 * 1000000.0) AS int
            -- unix epoch in nano-seconds
            -- https://www.sqlite.org/lang_datefunc.html#examples
    ));
END;

CREATE TABLE _synql_uniqueness(
    field integer NOT NULL,
    tbl_index integer NOT NULL,
    PRIMARY KEY(field, tbl_index)
) STRICT;

CREATE TABLE _synql_fk(
    field integer PRIMARY KEY,
    -- 0: CASCADE, 1: RESTRICT, 2: SET NULL
    on_delete integer NOT NULL CHECK(on_delete BETWEEN 0 AND 2),
    on_update integer NOT NULL CHECK(on_update BETWEEN 0 AND 2),
    foreign_index integer NOT NULL
) STRICT;

CREATE VIEW _synql_log_extra AS
SELECT log.*,
    ifnull(undo.ul, 0) AS ul, undo.ts AS ul_ts, undo.peer AS ul_peer,
    ifnull(tbl_undo.ul, 0) AS row_ul, tbl_undo.ts AS row_ul_ts, tbl_undo.peer AS row_ul_peer
FROM _synql_log AS log
    LEFT JOIN _synql_id_undo AS tbl_undo
        USING(row_ts, row_peer)
    LEFT JOIN _synql_undolog AS undo
        ON log.ts = undo.obj_ts AND log.peer = undo.obj_peer;

CREATE VIEW _synql_log_effective AS
SELECT log.* FROM _synql_log_extra AS log
WHERE log.ul%2 = 0 AND NOT EXISTS(
    SELECT 1 FROM _synql_log_extra AS self
    WHERE self.row_ts = log.row_ts AND self.row_peer = log.row_peer AND self.field = log.field AND
        (self.ts > log.ts OR (self.ts = log.ts AND self.peer > log.peer)) AND self.ul%2 = 0
);

CREATE VIEW _synql_fklog_extra AS
SELECT fklog.*,
    ifnull(undo.ul, 0) AS ul, undo.ts AS ul_ts, undo.peer AS ul_peer,
    ifnull(tbl_undo.ul, 0) AS row_ul, tbl_undo.ts AS row_ul_ts, tbl_undo.peer AS row_ul_peer,
    fk.on_update, fk.on_delete, fk.foreign_index
FROM _synql_fklog AS fklog
    LEFT JOIN _synql_id_undo AS tbl_undo
        USING(row_ts, row_peer)
    LEFT JOIN _synql_undolog AS undo
        ON fklog.ts = undo.obj_ts AND fklog.peer = undo.obj_peer
    LEFT JOIN _synql_fk AS fk
        USING(field);

CREATE VIEW _synql_fklog_effective AS
SELECT fklog.* FROM _synql_fklog_extra AS fklog
WHERE fklog.ul%2=0 AND NOT EXISTS(
    SELECT 1 FROM _synql_fklog_extra AS self
    WHERE self.ul%2=0 AND
        self.row_ts = fklog.row_ts AND self.row_peer = fklog.row_peer AND self.field = fklog.field AND
        (self.ts > fklog.ts OR (self.ts = fklog.ts AND self.peer > fklog.peer))
);

CREATE TRIGGER _synql_fklog_effective_insert
INSTEAD OF INSERT ON _synql_fklog_effective WHEN (
    NEW.peer IS NULL AND NEW.ts IS NULL
)
BEGIN
    UPDATE _synql_local SET ts = ts + 1;

    INSERT INTO _synql_fklog(
        ts, peer, row_ts, row_peer, field,
        foreign_row_ts, foreign_row_peer
    ) SELECT local.ts, local.peer, NEW.row_ts, NEW.row_peer, NEW.field,
        NEW.foreign_row_ts, NEW.foreign_row_peer
    FROM _synql_local AS local;
END;

-- Debug-only and test-only tables/views

CREATE TABLE _synql_names(
    id integer PRIMARY KEY,
    name text NOT NULL
) STRICT;

CREATE VIEW _synql_id_debug AS
SELECT id.row_ts, id.row_peer, id.tbl, tbl.name, undo.ul
FROM _synql_id AS id
    LEFT JOIN _synql_id_undo AS undo USING(row_ts, row_peer)
    LEFT JOIN _synql_names AS tbl ON tbl = id;
"""


def _synql_triggers(tables: sql.Symbols, conf: Config, /) -> str:
    # We do not support schema where:
    #
    # - `WITHOUT ROWID` tables
    # - tables with a column `rowid` that is not an alias of _SQLite_ `rowid` or an autoinc key
    #
    # These two limitations could be removed by using the primary key instead of `rowid`.
    #
    # Moreover Synql presents some limitations in the following cases:
    #
    # - referred columns by a foreign key that are themselves a foreign key
    # - table with at least one column used in distinct foreign keys
    result = ""
    ids = utils.ids(tables)
    for tbl_name, tbl in tables.items():
        tbl_uniqueness = tuple(tbl.uniqueness())
        replicated_cols = tuple(utils.replicated_columns(tbl))
        # we use a labelled timestamp (ts, peer) to globally and uniquely identify an object.
        # An object is either a row or a log entry.
        #
        # _synql_id_{tbl} contains a mapping between rows (rowid) and their id (labelled timestamp)
        # We use the (real) primary key of the table as a way to locally identify a row.
        # In SQLITE ROWID tables have an implicit rowid column as real primary key.
        # To simplify, we only support ROWID tables.
        #
        # We assume that rowid refers to the rowid column or an alias of that
        # (any INTEGER PRIMARY KEY).
        # We do not support the case where rowid is used to designate another col.
        # _synql_id_{tbl} explicitly declares rowid when {tbl} aliases rowid.
        # This enables to exhibit consistent behavior upon database vacuum,
        # and so to keep rowid correspondence between {tbl} and _synql_id_{tbl}.
        primary_key = tbl.primary_key()
        maybe_autoinc = (
            " AUTOINCREMENT"
            if primary_key is not None and primary_key.autoincrement
            else ""
        )
        maybe_rowid_alias = (
            f"rowid integer PRIMARY KEY{maybe_autoinc},"
            if utils.has_rowid_alias(tbl)
            else ""
        )
        table_synql_id = f"""
        CREATE TABLE "_synql_id_{tbl_name}"(
            {maybe_rowid_alias}
            row_ts integer NOT NULL,
            row_peer integer NOT NULL,
            UNIQUE(row_ts, row_peer),
            FOREIGN KEY(row_ts, row_peer) REFERENCES _synql_id(row_ts, row_peer)
                ON DELETE RESTRICT ON UPDATE CASCADE
        ) STRICT;

        CREATE TRIGGER "_synql_delete_{tbl_name}"
        AFTER DELETE ON "{tbl_name}"
        BEGIN
            DELETE FROM "_synql_id_{tbl_name}" WHERE rowid = OLD.rowid;
        END;

        CREATE TRIGGER "_synql_delete_id_{tbl_name}"
        AFTER DELETE ON "_synql_id_{tbl_name}"
        WHEN (SELECT NOT is_merging FROM _synql_local)
        BEGIN
            UPDATE _synql_local SET ts = ts + 1;
            UPDATE _synql_context SET ts = _synql_local.ts
            FROM _synql_local WHERE _synql_context.peer = _synql_local.peer;

            INSERT INTO _synql_id_undo(ts, peer, row_ts, row_peer, ul)
            SELECT local.ts, local.peer, OLD.row_ts, OLD.row_peer, 1
            FROM _synql_local AS local
            WHERE true  -- avoid parsing ambiguity
            ON CONFLICT
            DO UPDATE SET ul = ul + 1, ts = excluded.ts, peer = excluded.peer;
        END;
        """
        metadata = f"INSERT INTO _synql_names VALUES({ids[tbl]}, '{tbl_name}');"
        for cst in tbl.all_constraints():
            if cst.name is not None:
                metadata += f"""INSERT INTO _synql_names VALUES({ids[(tbl, cst)]}, '{cst.name}');"""
        for col in replicated_cols:
            metadata += f"""
            INSERT INTO _synql_names VALUES({ids[(tbl, col)]}, '{col.name}');
            """
            for uniq in tbl_uniqueness:
                if col.name in uniq.columns():
                    metadata += f"""
                    INSERT INTO _synql_uniqueness(field, tbl_index)
                    VALUES({ids[(tbl, col)]}, {ids[(tbl, uniq)]});
                    """.rstrip()
        for foreign_key in tbl.foreign_keys():
            for uniq in tbl_uniqueness:
                if any(col_name in uniq.columns() for col_name in foreign_key.columns):
                    metadata += f"""
                    INSERT INTO _synql_uniqueness(field, tbl_index)
                    VALUES({ids[(tbl, foreign_key)]}, {ids[(tbl, uniq)]});
                    """.rstrip()
            foreign_tbl = tables[foreign_key.foreign_table[0]]
            referred_cols = sql.referred_columns(foreign_key, tables)
            f_uniq = next(
                f_uniq
                for f_uniq in foreign_tbl.uniqueness()
                if tuple(f_uniq.columns()) == referred_cols
            )
            metadata += f"""
            INSERT INTO _synql_fk(field, foreign_index, on_delete, on_update)
            VALUES(
                {ids[(tbl, foreign_key)]}, {ids[(foreign_tbl, f_uniq)]},
                {_FK_ACTION[_normalize_fk_action(foreign_key.on_delete, conf)]},
                {_FK_ACTION[_normalize_fk_action(foreign_key.on_update, conf)]}
            );
            """
        log_updates = ""
        log_insertions = ""
        if len(replicated_cols) > 0:
            log_tuples = ", ".join(
                f'({ids[(tbl, col)]}, NEW."{col.name}")' for col in replicated_cols
            )
            log_insertions += f"""
            INSERT INTO _synql_log(ts, peer, row_ts, row_peer, field, val)
            SELECT local.ts, local.peer, local.ts, local.peer, tuples.*
            FROM _synql_local AS local, (VALUES {log_tuples}) AS tuples;
            """.strip()
            log_changed_tuples = "UNION ALL".join(
                f"""
                SELECT {ids[(tbl, col)]}, NEW."{col.name}"
                WHERE OLD."{col.name}" IS NOT NEW."{col.name}"
                """
                for col in replicated_cols
            )
            log_updates += f"""
            INSERT INTO _synql_log(ts, peer, row_ts, row_peer, field, val)
            SELECT local.ts, local.peer, cur.row_ts, cur.row_peer, tuples.*
            FROM _synql_local AS local, "_synql_id_{tbl_name}" AS cur,
                ({log_changed_tuples}) AS tuples
            WHERE cur.rowid = NEW.rowid;
            """.strip()
        fklog_updates = ""
        fklog_insertions = ""
        for foreign_key in tbl.foreign_keys():
            foreign_tbl_name = foreign_key.foreign_table[0]
            foreign_tbl = tables[foreign_tbl_name]
            referred_cols = sql.referred_columns(foreign_key, tables)
            old_referred_match = " AND ".join(
                f'"{ref_col}" = OLD."{col}"'
                for ref_col, col in zip(referred_cols, foreign_key.columns)
            )
            new_referred_match = " AND ".join(
                f'"{ref_col}" = NEW."{col}"'
                for ref_col, col in zip(referred_cols, foreign_key.columns)
            )
            null_ref_match = " AND ".join(
                f'NEW."{col}" IS NULL'
                for ref_col, col in zip(referred_cols, foreign_key.columns)
            )
            fklog_insertions += f"""
                -- Handle case where at least one col is NULL
                INSERT INTO _synql_fklog(ts, peer, row_ts, row_peer, field, foreign_row_ts, foreign_row_peer)
                SELECT
                    local.ts, local.peer, local.ts, local.peer, {ids[(tbl, foreign_key)]},
                    target.row_ts, target.row_peer
                FROM _synql_local AS local LEFT JOIN (
                    SELECT row_ts, row_peer FROM "_synql_id_{foreign_tbl_name}"
                    WHERE rowid = (
                        SELECT rowid FROM "{foreign_tbl_name}"
                        WHERE {new_referred_match}
                    )
                ) AS target;
            """.rstrip()
            fklog_updates += f"""
                -- Handle case where at least one col is NULL
                INSERT INTO _synql_fklog(ts, peer, row_ts, row_peer, field, foreign_row_ts, foreign_row_peer)
                SELECT
                    local.ts, local.peer, cur.row_ts, cur.row_peer, {ids[(tbl, foreign_key)]},
                    target.row_ts, target.row_peer
                FROM _synql_local AS local, (
                    SELECT * FROM "_synql_id_{tbl_name}" WHERE rowid = NEW.rowid
                ) AS cur LEFT JOIN (
                    SELECT row_ts, row_peer FROM "_synql_id_{foreign_tbl_name}"
                    WHERE rowid = (
                        SELECT rowid FROM "{foreign_tbl_name}"
                        WHERE {new_referred_match}
                    )
                ) AS target
                WHERE NOT EXISTS(
                    -- is ON UPDATE CASCADE?
                    SELECT 1 FROM (
                        SELECT foreign_row_ts, foreign_row_peer FROM _synql_fklog
                        WHERE row_ts = cur.row_ts AND row_peer = cur.row_peer AND
                            field = {ids[(tbl, foreign_key)]}
                        ORDER BY ts, peer LIMIT 1
                    )
                    WHERE foreign_row_ts = target.row_ts AND foreign_row_peer = target.row_peer
                ) AND (
                    -- is not ON DELETE SET NULL?
                    NOT ({null_ref_match}) OR EXISTS(
                        SELECT 1 FROM "{foreign_tbl_name}"
                        WHERE {old_referred_match}
                    )
                );
            """
        triggers = f"""
        CREATE TRIGGER "_synql_log_insert_{tbl_name}"
        AFTER INSERT ON "{tbl_name}"
        WHEN (SELECT NOT is_merging FROM _synql_local)
        BEGIN
            -- Handle INSERT OR REPLACE
            -- Delete trigger is not fired when recursive triggers are disabled.
            -- To ensure that the pre-existing row is deleted, we attempt a deletion.
            DELETE FROM "_synql_id_{tbl_name}" WHERE rowid = NEW.rowid;

            UPDATE _synql_local SET ts = ts + 1;
            UPDATE _synql_context SET ts = _synql_local.ts
            FROM _synql_local WHERE _synql_context.peer = _synql_local.peer;

            INSERT INTO "_synql_id_{tbl_name}"(rowid, row_ts, row_peer)
            SELECT NEW.rowid, ts, peer FROM _synql_local;

            INSERT INTO _synql_id(row_ts, row_peer, tbl)
            SELECT ts, peer, {ids[tbl]} FROM _synql_local;
            {log_insertions}
            {fklog_insertions}
        END;
        """.rstrip()
        tracked_cols = [f'"{x.name}"' for x in replicated_cols] + [
            f'"{name}"' for name in utils.foreign_column_names(tbl)
        ]
        if len(tracked_cols) > 0:
            triggers += f"""
            CREATE TRIGGER "_synql_log_update_{tbl_name}"
            AFTER UPDATE OF {','.join(tracked_cols)} ON "{tbl_name}"
            WHEN (SELECT NOT is_merging FROM _synql_local)
            BEGIN
                UPDATE _synql_local SET ts = ts + 1;
                UPDATE _synql_context SET ts = _synql_local.ts
                FROM _synql_local WHERE _synql_context.peer = _synql_local.peer;
                {log_updates}
                {fklog_updates}
            END;
            """.rstrip()
        rowid_aliases = utils.rowid_aliases(tbl)
        if len(rowid_aliases) > 0:
            triggers += f"""
            CREATE TRIGGER "_synql_log_update_rowid_{tbl_name}_rowid"
            AFTER UPDATE OF {", ".join(rowid_aliases)} ON "{tbl_name}"
            BEGIN
                UPDATE "_synql_id_{tbl_name}" SET rowid = NEW."{rowid_aliases[0]}"
                WHERE rowid = OLD."{rowid_aliases[0]}";
            END;
            """.rstrip()
        result += metadata + textwrap.dedent(table_synql_id) + textwrap.dedent(triggers)
    return result.strip()


def _get_schema(db: sqlite3.Connection, /) -> str:
    with closing(db.cursor()) as cursor:
        cursor.execute(_SELECT_USER_TABLE_SCHEMA)
        result = ";".join(r[0] for r in cursor) + ";"
    return result


_PULL_EXTERN = """
-- Update clock
UPDATE _synql_local SET ts = max(_synql_local.ts, max(ctx.ts)) + 1
FROM extern._synql_context AS ctx;

-- Add missing peers in the context with a ts of 0
INSERT OR IGNORE INTO _synql_context SELECT peer, 0 FROM extern._synql_context;
INSERT OR IGNORE INTO extern._synql_context SELECT peer, 0 FROM _synql_context;

-- Add new id and log entries
INSERT INTO _synql_id
SELECT id.* FROM extern._synql_id AS id JOIN _synql_context AS ctx
    ON id.row_ts > ctx.ts AND id.row_peer = ctx.peer;

INSERT INTO _synql_log
SELECT log.* FROM extern._synql_log AS log JOIN _synql_context AS ctx
    ON log.ts > ctx.ts AND log.peer = ctx.peer;

INSERT INTO _synql_fklog
SELECT fklog.*
FROM extern._synql_fklog AS fklog JOIN _synql_context AS ctx
    ON fklog.ts > ctx.ts AND fklog.peer = ctx.peer;

INSERT INTO _synql_id_undo
SELECT log.* FROM extern._synql_id_undo AS log JOIN _synql_context AS ctx
    ON log.ts > ctx.ts AND log.peer = ctx.peer
WHERE true  -- avoid parsing ambiguity
ON CONFLICT DO UPDATE SET ul = excluded.ul, ts = excluded.ts, peer = excluded.peer
WHERE ul < excluded.ul;

INSERT INTO _synql_undolog
SELECT log.* FROM extern._synql_undolog AS log JOIN _synql_context AS ctx
    ON log.ts > ctx.ts AND log.peer = ctx.peer
WHERE true  -- avoid parsing ambiguity
ON CONFLICT DO UPDATE SET ul = excluded.ul, ts = excluded.ts, peer = excluded.peer
WHERE ul < excluded.ul;
"""

_CONFLICT_RESOLUTION = """
-- A. ON UPDATE RESTRICT
-- undo all concurrent updates to a restrict ref
INSERT OR REPLACE INTO _synql_undolog(ts, peer, obj_peer, obj_ts, ul)
SELECT local.ts, local.peer, log.peer, log.ts, log.ul + 1
FROM _synql_local AS local, _synql_context AS ctx, extern._synql_context AS ectx,
    _synql_log_extra AS log JOIN _synql_uniqueness AS uniq USING(field), _synql_fklog_effective AS fklog
WHERE (
    log.ts > fklog.ts OR (log.ts = fklog.ts AND log.peer = fklog.peer) OR
    (log.ts > ctx.ts AND log.peer = ctx.peer AND fklog.ts > ectx.ts AND fklog.peer = ectx.peer) OR
    (log.ts > ectx.ts AND log.peer = ectx.peer AND fklog.ts > ctx.ts AND fklog.peer = ctx.peer)
) AND (
    log.row_ts = fklog.foreign_row_ts AND
    log.row_peer = fklog.foreign_row_peer AND
    uniq.tbl_index = fklog.foreign_index
) AND fklog.on_update = 1 AND log.ul%2 = 0;

-- B. ON DELETE RESTRICT
INSERT OR REPLACE INTO _synql_id_undo(ts, peer, row_ts, row_peer, ul)
WITH RECURSIVE _synql_restrict_refs(foreign_row_ts, foreign_row_peer) AS (
    SELECT foreign_row_ts, foreign_row_peer
    FROM _synql_fklog_effective
    WHERE on_delete = 1 AND row_ul%2 = 0
    UNION
    SELECT target.foreign_row_ts, target.foreign_row_peer
    FROM _synql_restrict_refs AS src JOIN _synql_fklog_effective AS target
        ON src.foreign_row_ts = target.row_ts AND src.foreign_row_peer = target.row_peer
    WHERE on_delete = 0
)
SELECT local.ts, local.peer, row_ts, row_peer, ul + 1
FROM _synql_local AS local, _synql_restrict_refs JOIN _synql_id_undo
    ON foreign_row_ts = row_ts AND foreign_row_peer = row_peer
WHERE ul%2 = 1;

-- C. ON UPDATE SET NULL
INSERT INTO _synql_fklog_effective(row_ts, row_peer, field)
SELECT fklog.row_ts, fklog.row_peer, fklog.field
FROM _synql_context AS ctx, extern._synql_context AS ectx,
    _synql_log_effective AS log JOIN _synql_uniqueness AS uniq USING(field), _synql_fklog_effective AS fklog
WHERE (
    (log.ts > ctx.ts AND log.peer = ctx.peer AND fklog.ts > ectx.ts AND fklog.peer = ectx.peer) OR
    (log.ts > ectx.ts AND log.peer = ectx.peer AND fklog.ts > ctx.ts AND fklog.peer = ctx.peer)
) AND (
    log.row_ts = fklog.foreign_row_ts AND
    log.row_peer = fklog.foreign_row_peer AND
    uniq.tbl_index = fklog.foreign_index
) AND fklog.on_update = 2;

-- D. resolve uniqueness conflicts
-- undo latest rows with conflicting unique keys
INSERT OR REPLACE INTO _synql_id_undo(ts, peer, row_ts, row_peer, ul)
WITH _synql_unified_log_effective AS (
    SELECT
        log.ts, log.peer, log.row_ts, log.row_peer, log.field,
        log.val, NULL AS foreign_row_ts, NULL AS foreign_row_peer, log.row_ul
    FROM _synql_log_effective AS log
    UNION ALL
    SELECT
        fklog.ts, fklog.peer, fklog.row_ts, fklog.row_peer, fklog.field,
        NULL AS val, fklog.foreign_row_ts, fklog.foreign_row_peer, fklog.row_ul
    FROM _synql_fklog_effective AS fklog
)
SELECT DISTINCT local.ts, local.peer, log.row_ts, log.row_peer, log.row_ul + 1
FROM _synql_local AS local, _synql_unified_log_effective AS log JOIN _synql_unified_log_effective AS self
        ON log.field = self.field AND (
            log.val = self.val OR (
                log.foreign_row_ts = self.foreign_row_ts AND
                log.foreign_row_peer = self.foreign_row_peer
            )
        ) JOIN _synql_uniqueness AS uniq USING(field),
    _synql_context AS ctx, extern._synql_context AS ectx
WHERE (
    log.row_ts > self.row_ts OR (
        log.row_ts = self.row_ts AND log.row_peer > self.row_peer
    )
)
-- AND (
--      -- FIXME: Should also take ul_ts, ul_peer, row_ul_ts, row_ul_peer into account
--     (log.ts > ctx.ts AND log.peer = ctx.peer AND self.ts > ectx.ts AND self.peer = ectx.peer) OR
--     (log.ts > ectx.ts AND log.peer = ectx.peer AND self.ts > ctx.ts AND self.peer = ctx.peer)
-- )
GROUP BY log.row_ts, log.row_peer, self.row_ts, self.row_peer, uniq.tbl_index
HAVING count(DISTINCT log.field) >= (
    SELECT count(DISTINCT field) FROM _synql_uniqueness WHERE tbl_index = uniq.tbl_index
);

-- E. ON DELETE CASCADE
INSERT OR REPLACE INTO _synql_id_undo(ts, peer, row_ts, row_peer, ul)
WITH RECURSIVE _synql_dangling_refs(row_ts, row_peer, row_ul) AS (
    SELECT fklog.row_ts, fklog.row_peer, fklog.row_ul
    FROM _synql_fklog_effective AS fklog JOIN _synql_id_undo AS undo
        ON fklog.foreign_row_ts = undo.row_ts AND fklog.foreign_row_peer = undo.row_peer
    WHERE fklog.on_delete <> 2 AND fklog.row_ul%2 = 0 AND undo.ul%2 = 1
    UNION
    SELECT src.row_ts, src.row_peer, src.row_ul
    FROM _synql_dangling_refs AS target JOIN _synql_fklog_effective AS src
        ON src.foreign_row_ts = target.row_ts AND src.foreign_row_peer = target.row_peer
    WHERE src.row_ul%2 = 0
)
SELECT local.ts, local.peer, row_ts, row_peer, row_ul+1
FROM _synql_local AS local, _synql_dangling_refs
WHERE row_ul%2 = 0;
"""

_MERGE_END = """
-- Update context
UPDATE _synql_context SET ts = ctx.ts FROM extern._synql_context AS ctx
WHERE ctx.ts > _synql_context.ts AND _synql_context.peer = ctx.peer;

UPDATE _synql_context SET ts = local.ts
FROM _synql_local AS local JOIN _synql_id_undo USING(peer, ts)
WHERE _synql_context.peer = local.peer;

UPDATE _synql_context SET ts = local.ts
FROM _synql_local AS local JOIN _synql_undolog USING(peer, ts)
WHERE _synql_context.peer = local.peer;

UPDATE _synql_context SET ts = local.ts
FROM _synql_local AS local JOIN _synql_log USING(peer, ts)
WHERE _synql_context.peer = local.peer;

UPDATE _synql_context SET ts = local.ts
FROM _synql_local AS local JOIN _synql_fklog USING(peer, ts)
WHERE _synql_context.peer = local.peer;
"""


def _create_pull(tables: sql.Symbols, /) -> str:
    result = ""
    merger = ""
    ids = utils.ids(tables)
    for tbl_name, tbl in tables.items():
        selectors = ["id.rowid"]
        col_names = ["rowid"]
        for col in utils.replicated_columns(tbl):
            col_names += [col.name]
            selectors += [
                f'''(
                SELECT log.val FROM _synql_log_extra AS log
                WHERE log.row_ts = id.row_ts AND log.row_peer = id.row_peer AND
                    log.field = {ids[(tbl, col)]} AND log.ul%2 = 0
                ORDER BY log.ts DESC, log.peer DESC LIMIT 1
            ) AS "{col.name}"'''
            ]
        for foreign_key in tbl.foreign_keys():
            for col_name in foreign_key.columns:
                if col_name not in col_names:
                    col_names += [col_name]
                    selector = f"""
                    SELECT fklog.* FROM _synql_fklog_extra AS fklog
                    WHERE fklog.field = {ids[(tbl, foreign_key)]} AND fklog.ul%2 = 0 AND
                        fklog.row_peer = id.row_peer AND fklog.row_ts = id.row_ts AND
                        fklog.row_ul%2 = 0
                    ORDER BY fklog.ts DESC, fklog.peer DESC LIMIT 1
                    """
                    referred_tbl = tables[foreign_key.foreign_table[0]]
                    for referred in sql.resolve_foreign_key(
                        foreign_key, col_name, tables
                    ):
                        if isinstance(referred, sql.ForeignKey):
                            selector = f"""
                            SELECT fklog2.* FROM (
                                {selector}
                            ) AS fklog LEFT JOIN _synql_fklog_extra AS fklog2
                                ON fklog2.field = {ids[(referred_tbl, referred)]} AND
                                    fklog2.ul%2 = 0 AND
                                    fklog.foreign_row_peer = fklog2.row_peer AND
                                    fklog.foreign_row_ts = fklog2.row_ts
                            WHERE fklog2.row_ul%2 = 0
                            ORDER BY fklog.ts DESC, fklog.peer DESC LIMIT 1
                            """
                            referred_tbl = tables[referred.foreign_table[0]]
                        else:
                            ref_col = referred_tbl.column(referred)
                            assert ref_col is not None
                            if utils.is_rowid_alias(
                                ref_col, referred_tbl.primary_key()
                            ):
                                selector = f"""
                                SELECT rw.rowid FROM (
                                        {selector}
                                ) AS fklog LEFT JOIN "_synql_id_{referred_tbl.name[0]}" AS rw
                                    ON fklog.foreign_row_peer = rw.row_peer AND
                                        fklog.foreign_row_ts = rw.row_ts
                                """
                            else:
                                selector = f"""
                                SELECT log.val FROM (
                                        {selector}
                                ) AS fklog LEFT JOIN _synql_log_extra AS log
                                    ON log.row_peer = fklog.foreign_row_peer AND
                                        log.row_ts = fklog.foreign_row_ts AND
                                        log.field = {ids[(referred_tbl, ref_col)]} AND log.ul%2 = 0
                                WHERE log.row_ul%2 = 0
                                ORDER BY log.ts DESC, log.peer DESC LIMIT 1
                                """
                            selectors += [f'({selector}) AS "{col_name}"']
        merger += f"""
        INSERT OR REPLACE INTO "{tbl_name}"({', '.join(col_names)})
        WITH RECURSIVE _synql_unified_log AS (
            SELECT
            log.ts, log.peer, log.row_ts, log.row_peer, log.field,
            log.val, NULL AS foreign_row_ts, NULL AS foreign_row_peer,
            log.ul, log.ul_ts, log.ul_peer, log.row_ul
            FROM _synql_log_effective AS log
            UNION ALL
            SELECT
                fklog.ts, fklog.peer, fklog.row_ts, fklog.row_peer, fklog.field,
                NULL AS val, fklog.foreign_row_ts, fklog.foreign_row_peer,
                fklog.ul, fklog.ul_ts, fklog.ul_peer, fklog.row_ul
            FROM _synql_fklog_effective AS fklog
        ), _synql_cascade_refs(row_ts, row_peer, field) AS (
            -- on update cascade triggered by extern updates OR undone updates
            SELECT fklog.row_ts, fklog.row_peer, fklog.field
            FROM _synql_context AS ctx, extern._synql_context AS ectx, _synql_unified_log AS log
                JOIN _synql_uniqueness AS uniq USING(field)
                JOIN _synql_fklog_effective AS fklog
                    ON log.row_ts = fklog.foreign_row_ts AND
                        log.row_peer = fklog.foreign_row_peer AND
                        uniq.tbl_index = fklog.foreign_index
            WHERE fklog.on_update = 0 AND fklog.row_ul%2 = 0 AND
                ((log.peer = ctx.peer AND log.ts > ctx.ts) OR
                (log.ul_peer = ctx.peer AND log.ul_ts > ctx.ts))
            UNION
            SELECT src.row_ts, src.row_peer, src.field
            FROM _synql_cascade_refs AS target
                JOIN _synql_uniqueness AS uniq USING(field)
                JOIN _synql_fklog_effective AS src
                    ON src.foreign_row_ts = target.row_ts AND
                        src.foreign_row_peer = target.row_peer AND
                        uniq.tbl_index = src.foreign_index
            WHERE src.on_update = 0 AND src.row_ul%2 = 0
        )
        SELECT {', '.join(selectors)} FROM (
            SELECT id.rowid, id.row_ts, id.row_peer FROM (
                SELECT log.row_ts, log.row_peer FROM _synql_unified_log AS log
                    JOIN _synql_context AS ctx
                        ON (log.ul%2 = 0 AND log.peer = ctx.peer AND log.ts > ctx.ts) OR
                            (log.ul%2 = 1 AND log.ul_peer = ctx.peer AND log.ul_ts > ctx.ts)
                WHERE log.row_ul%2 = 0
                UNION
                SELECT redo.row_ts, redo.row_peer FROM _synql_id_undo AS redo
                    JOIN _synql_context AS ctx
                        ON redo.ts > ctx.ts AND redo.peer = ctx.peer
                WHERE NOT redo.ul%2 = 1
                UNION
                SELECT log.row_ts, log.row_peer FROM _synql_context AS ctx
                    JOIN _synql_undolog AS undo
                        ON undo.ts > ctx.ts AND undo.peer = ctx.peer
                    JOIN _synql_log AS log
                        ON undo.obj_ts = log.ts AND undo.obj_peer = log.peer
                WHERE undo.ul%2 = 1
                UNION
                SELECT log.row_ts, log.row_peer FROM _synql_context AS ctx
                    JOIN _synql_undolog AS undo
                        ON undo.ts > ctx.ts AND undo.peer = ctx.peer
                    JOIN _synql_fklog AS log
                        ON undo.obj_ts = log.ts AND undo.obj_peer = log.peer
                WHERE undo.ul%2 = 1
                UNION
                SELECT row_ts, row_peer FROM _synql_cascade_refs
                UNION
                SELECT fklog.row_ts, fklog.row_peer FROM _synql_fklog_extra AS fklog
                    JOIN _synql_id_undo AS undo
                        ON fklog.foreign_row_peer = undo.row_peer AND
                            fklog.foreign_row_ts = undo.row_ts
                WHERE undo.ul%2 = 1 AND fklog.on_delete = 2
            ) JOIN "_synql_id_{tbl_name}" AS id
                USING(row_ts, row_peer)
            UNION
            SELECT id.rowid, id.row_ts, id.row_peer FROM "_synql_id_{tbl_name}" AS id
                JOIN _synql_context AS ctx
                    ON id.row_ts > ctx.ts AND id.row_peer = ctx.peer
        ) AS id;
        """
        result += f"""
        -- Foreign keys must be disabled

        -- Apply deletion (existing rows)
        DELETE FROM "{tbl_name}" WHERE rowid IN (
            SELECT id.rowid FROM "_synql_id_{tbl_name}" AS id
                JOIN _synql_id_undo AS undo
                    ON id.row_ts = undo.row_ts AND id.row_peer = undo.row_peer
            WHERE undo.ul%2 = 1
        );
 
        -- Auto-assign local rowids for new active rows
        INSERT INTO "_synql_id_{tbl_name}"(row_peer, row_ts)
        SELECT id.row_peer, id.row_ts
        FROM _synql_id AS id JOIN _synql_context AS ctx
            ON id.row_ts > ctx.ts AND id.row_peer = ctx.peer
        WHERE id.tbl = {ids[tbl]} AND NOT EXISTS(
            SELECT 1 FROM _synql_id_undo AS undo
            WHERE undo.ul%2 = 1 AND
                undo.row_ts = id.row_ts AND undo.row_peer = id.row_peer
        );

        -- Auto-assign local rowids for redone rows
        INSERT OR IGNORE INTO "_synql_id_{tbl_name}"(row_ts, row_peer)
        SELECT id.row_ts, id.row_peer
        FROM _synql_id_undo AS redo
            JOIN _synql_context AS ctx
                ON redo.ts > ctx.ts AND redo.peer = ctx.peer
            JOIN _synql_id AS id
                ON redo.row_ts = id.row_ts AND redo.row_peer = id.row_peer
                    AND id.tbl = {ids[tbl]}
        WHERE redo.ul%2 = 0;
        """
    result += merger
    return result
