from sqlschm.parser import parse_schema
from sqlschm import sql
from contextlib import closing
import logging
import pathlib
import textwrap
import pysqlite3 as sqlite3
from synqlite import sqlschm_utils as utils
from dataclasses import dataclass
from typing import Literal

logging.basicConfig(level=logging.DEBUG)


@dataclass(frozen=True, kw_only=True, slots=True)
class Config:
    physical_clock: bool = True
    no_action_is_cascade = False


FK_ACTION = {
    sql.OnUpdateDelete.CASCADE: 0,
    sql.OnUpdateDelete.RESTRICT: 1,
    sql.OnUpdateDelete.SET_NULL: 2,
}


def normalize_fk_action(
    action: sql.OnUpdateDelete | None, conf: Config
) -> Literal[sql.OnUpdateDelete.CASCADE] | Literal[
    sql.OnUpdateDelete.RESTRICT
] | Literal[sql.OnUpdateDelete.SET_NULL]:
    if action is None or action is sql.OnUpdateDelete.NO_ACTION:
        if conf.no_action_is_cascade:
            return sql.OnUpdateDelete.CASCADE
        else:
            return sql.OnUpdateDelete.RESTRICT
    elif action is sql.OnUpdateDelete.SET_DEFAULT:
        raise Exception("SynQLite does not support On DELETE/UPDATE SET DEFAULT")
    return action


_SELECT_AR_TABLE_SCHEMA = """--sql
SELECT sql FROM sqlite_master WHERE (type = 'table' OR type = 'index') AND
    name NOT LIKE 'sqlite_%' AND name NOT LIKE '_synq_%';
"""

_CREATE_TABLE_CONTEXT = """-- Causal context
CREATE TABLE _synq_context(
    peer integer PRIMARY KEY,
    ts integer NOT NULL DEFAULT 0 CHECK (ts >= 0)
) STRICT;
"""

_CREATE_REPLICATION_TABLES = """
CREATE TABLE _synq_id(
    row_ts integer NOT NULL,
    row_peer integer NOT NULL REFERENCES _synq_context(peer) ON DELETE CASCADE ON UPDATE CASCADE,
    tbl integer NOT NULL,
    PRIMARY KEY(row_ts DESC, row_peer DESC),
    UNIQUE(row_peer, row_ts)
) STRICT, WITHOUT ROWID;

CREATE TABLE _synq_id_undo(
    row_ts integer NOT NULL,
    row_peer integer NOT NULL,
    ul integer NOT NULL DEFAULT 0 CHECK(ul >= 0), -- undo length
    ts integer NOT NULL CHECK(ts >= row_ts),
    peer integer NOT NULL,
    PRIMARY KEY(row_ts DESC, row_peer DESC),
    FOREIGN KEY(row_ts, row_peer) REFERENCES _synq_id(row_ts, row_peer)
        ON DELETE CASCADE ON UPDATE CASCADE
) STRICT, WITHOUT ROWID;
CREATE INDEX _synq_id_undo_index_ts ON _synq_id_undo(peer, ts);

CREATE TABLE _synq_log(
    ts integer NOT NULL CHECK(ts >= row_ts),
    peer integer NOT NULL,
    row_ts integer NOT NULL,
    row_peer integer NOT NULL,
    field integer NOT NULL,
    val any,
    PRIMARY KEY(row_ts, row_peer, field, ts, peer),
    FOREIGN KEY(row_ts, row_peer) REFERENCES _synq_id(row_ts, row_peer)
        ON DELETE CASCADE ON UPDATE CASCADE
) STRICT;
CREATE INDEX _synq_log_index_ts ON _synq_log(peer, ts);

CREATE TABLE _synq_fklog(
    ts integer NOT NULL CHECK(ts >= row_ts),
    peer integer NOT NULL,
    row_ts integer NOT NULL,
    row_peer integer NOT NULL,
    field integer NOT NULL,
    foreign_row_ts integer DEFAULT NULL,
    foreign_row_peer integer DEFAULT NULL,
    PRIMARY KEY(row_ts, row_peer, field, ts, peer),
    FOREIGN KEY(row_ts, row_peer) REFERENCES _synq_id(row_ts, row_peer)
        ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY(foreign_row_ts, foreign_row_peer) REFERENCES _synq_id(row_ts, row_peer)
        ON DELETE NO ACTION ON UPDATE CASCADE
) STRICT;
CREATE INDEX _synq_fklog_index_ts ON _synq_fklog(peer, ts);

CREATE TABLE _synq_undolog(
    obj_ts integer NOT NULL,
    obj_peer integer NOT NULL,
    ul integer NOT NULL DEFAULT 0 CHECK(ul >= 0), -- undo length
    ts integer NOT NULL CHECK(ts >= obj_ts),
    peer integer NOT NULL,
    PRIMARY KEY(obj_ts DESC, obj_peer DESC)
) STRICT, WITHOUT ROWID;
CREATE INDEX _synq_undolog_ts ON _synq_undolog(peer, ts);
"""

_CREATE_LOCAL_TABLES_VIEWS = """
CREATE TABLE _synq_local(
    id integer PRIMARY KEY DEFAULT 1 CHECK(id = 1),
    peer integer NOT NULL DEFAULT 0,
    ts integer NOT NULL DEFAULT 0 CHECK(ts >= 0),
    is_merging integer NOT NULL DEFAULT 0 CHECK(is_merging & 1 = is_merging)
) STRICT;
INSERT INTO _synq_local DEFAULT VALUES;

-- use `UPDATE _synq_local SET ts = ts + 1` to refresh the hybrid logical clock
CREATE TRIGGER          _synq_local_clock
AFTER UPDATE OF ts ON _synq_local WHEN (OLD.ts + 1 = NEW.ts)
BEGIN
    UPDATE _synq_local SET ts = max(NEW.ts, CAST(
        ((julianday('now') - julianday('1970-01-01')) * 86400.0 * 1000000.0) AS int
            -- unix epoch in nano-seconds
            -- https://www.sqlite.org/lang_datefunc.html#examples
    ));
END;

CREATE TABLE _synq_uniqueness(
    field integer NOT NULL,
    tbl_index integer NOT NULL,
    PRIMARY KEY(field, tbl_index)
) STRICT;

CREATE TABLE _synq_fk(
    field integer PRIMARY KEY,
    -- 0: CASCADE, 1: RESTRICT, 2: SET NULL
    on_delete integer NOT NULL CHECK(on_delete BETWEEN 0 AND 2),
    on_update integer NOT NULL CHECK(on_update BETWEEN 0 AND 2),
    foreign_index integer NOT NULL
) STRICT;

CREATE VIEW _synq_log_extra AS
SELECT log.*,
    ifnull(undo.ul, 0) AS ul, undo.ts AS ul_ts, undo.peer AS ul_peer,
    ifnull(tbl_undo.ul, 0) AS row_ul, tbl_undo.ts AS row_ul_ts, tbl_undo.peer AS row_ul_peer
FROM _synq_log AS log
    LEFT JOIN _synq_id_undo AS tbl_undo
        USING(row_ts, row_peer)
    LEFT JOIN _synq_undolog AS undo
        ON log.ts = undo.obj_ts AND log.peer = undo.obj_peer;

CREATE VIEW _synq_log_effective AS
SELECT log.* FROM _synq_log_extra AS log
WHERE log.ul%2 = 0 AND NOT EXISTS(
    SELECT 1 FROM _synq_log_extra AS self
    WHERE self.row_ts = log.row_ts AND self.row_peer = log.row_peer AND self.field = log.field AND
        (self.ts > log.ts OR (self.ts = log.ts AND self.peer > log.peer)) AND self.ul%2 = 0
);

CREATE VIEW _synq_fklog_extra AS
SELECT fklog.*,
    ifnull(undo.ul, 0) AS ul, undo.ts AS ul_ts, undo.peer AS ul_peer,
    ifnull(tbl_undo.ul, 0) AS row_ul, tbl_undo.ts AS row_ul_ts, tbl_undo.peer AS row_ul_peer,
    fk.on_update, fk.on_delete, fk.foreign_index
FROM _synq_fklog AS fklog
    LEFT JOIN _synq_id_undo AS tbl_undo
        USING(row_ts, row_peer)
    LEFT JOIN _synq_undolog AS undo
        ON fklog.ts = undo.obj_ts AND fklog.peer = undo.obj_peer
    LEFT JOIN _synq_fk AS fk
        USING(field);

CREATE VIEW _synq_fklog_effective AS
SELECT fklog.* FROM _synq_fklog_extra AS fklog
WHERE fklog.ul%2=0 AND NOT EXISTS(
    SELECT 1 FROM _synq_fklog_extra AS self
    WHERE self.ul%2=0 AND
        self.row_ts = fklog.row_ts AND self.row_peer = fklog.row_peer AND self.field = fklog.field AND
        (self.ts > fklog.ts OR (self.ts = fklog.ts AND self.peer > fklog.peer))
);

CREATE TRIGGER _synq_fklog_effective_insert
INSTEAD OF INSERT ON _synq_fklog_effective WHEN (
    NEW.peer IS NULL AND NEW.ts IS NULL
)
BEGIN
    UPDATE _synq_local SET ts = ts + 1;

    INSERT INTO _synq_fklog(
        ts, peer, row_ts, row_peer, field,
        foreign_row_ts, foreign_row_peer
    ) SELECT local.ts, local.peer, NEW.row_ts, NEW.row_peer, NEW.field,
        NEW.foreign_row_ts, NEW.foreign_row_peer
    FROM _synq_local AS local;
END;

-- Debug-only and test-only tables/views

CREATE TABLE _synq_names(
    id integer PRIMARY KEY,
    name text NOT NULL
) STRICT;

CREATE VIEW _synq_id_debug AS
SELECT id.row_ts, id.row_peer, id.tbl, tbl.name, undo.ul
FROM _synq_id AS id
    LEFT JOIN _synq_id_undo AS undo USING(row_ts, row_peer)
    LEFT JOIN _synq_names AS tbl ON tbl = id;
"""


def _synq_triggers(tables: sql.Symbols, conf: Config) -> str:
    # FIXME: We do not support schema where:
    # - tables that reuse the name rowid
    # - without rowid tables (that's fine)
    # - rowid is aliased with no-autoinc column
    # - referred columns by a foreign key that are themselves a foreign key
    # - table with at least one column used in distinct foreign keys
    result = ""
    ids = utils.ids(tables)
    for (tbl_name, tbl) in tables.items():
        tbl_uniqueness = tuple(tbl.uniqueness())
        replicated_cols = tuple(utils.replicated_columns(tbl))
        # we use a dot (peer, ts) to globally and uniquely identify an object.
        # An object is either a row or a log entry.
        # _synq_id_{tbl} contains a mapping between rows (rowid) and their id (dot)
        # We use the primary key of the table as a way to locally identify a row.
        # In SQLITE ROWID tables have an implicit rowid column as real primary key.
        # To simplify, we only support ROWID tables.
        # We assume that rowid refers to the rowid column or an alias of that
        # (any INTEGER PRIMARY KEY).
        # We do not support the case where rowid is used to designate another col.
        # _synq_id_{tbl} explicitly declares rowid when {tbl} aliases rowid.
        # This enables to exhibit consistent behavior upon database vacuum,
        # and so to keep rowid correspondence between {tbl} and _synq_id_{tbl}.
        pk = tbl.primary_key()
        maybe_autoinc = " AUTOINCREMENT" if pk is not None and pk.autoincrement else ""
        maybe_rowid_alias = (
            f"rowid integer PRIMARY KEY{maybe_autoinc},"
            if utils.has_rowid_alias(tbl)
            else ""
        )
        table_synq_id = f"""
        CREATE TABLE "_synq_id_{tbl_name}"(
            {maybe_rowid_alias}
            row_ts integer NOT NULL,
            row_peer integer NOT NULL,
            UNIQUE(row_ts, row_peer),
            FOREIGN KEY(row_ts, row_peer) REFERENCES _synq_id(row_ts, row_peer)
                ON DELETE RESTRICT ON UPDATE CASCADE
        ) STRICT;

        CREATE TRIGGER "_synq_delete_{tbl_name}"
        AFTER DELETE ON "{tbl_name}"
        BEGIN
            DELETE FROM "_synq_id_{tbl_name}" WHERE rowid = OLD.rowid;
        END;

        CREATE TRIGGER "_synq_delete_id_{tbl_name}"
        AFTER DELETE ON "_synq_id_{tbl_name}"
        WHEN (SELECT NOT is_merging FROM _synq_local)
        BEGIN
            UPDATE _synq_local SET ts = ts + 1;
            UPDATE _synq_context SET ts = _synq_local.ts
            FROM _synq_local WHERE _synq_context.peer = _synq_local.peer;

            INSERT INTO _synq_id_undo(ts, peer, row_ts, row_peer, ul)
            SELECT local.ts, local.peer, OLD.row_ts, OLD.row_peer, 1
            FROM _synq_local AS local
            WHERE true  -- avoid parsing ambiguity
            ON CONFLICT
            DO UPDATE SET ul = ul + 1, ts = excluded.ts, peer = excluded.peer;
        END;
        """
        metadata = f"INSERT INTO _synq_names VALUES({ids[tbl]}, '{tbl_name}');"
        for cst in tbl.all_constraints():
            if cst.name is not None:
                metadata += f"""INSERT INTO _synq_names VALUES({ids[(tbl, cst)]}, '{cst.name}');"""
        for col in replicated_cols:
            metadata += f"""
            INSERT INTO _synq_names VALUES({ids[(tbl, col)]}, '{col.name}');
            """
            for uniq in tbl_uniqueness:
                if col.name in uniq.columns():
                    metadata += f"""
                    INSERT INTO _synq_uniqueness(field, tbl_index)
                    VALUES({ids[(tbl, col)]}, {ids[(tbl, uniq)]});
                    """.rstrip()
        for fk in tbl.foreign_keys():
            for uniq in tbl_uniqueness:
                if any(col_name in uniq.columns() for col_name in fk.columns):
                    metadata += f"""
                    INSERT INTO _synq_uniqueness(field, tbl_index)
                    VALUES({ids[(tbl, fk)]}, {ids[(tbl, uniq)]});
                    """.rstrip()
            foreign_tbl = tables[fk.foreign_table[0]]
            referred_cols = sql.referred_columns(fk, tables)
            f_uniq = next(
                f_uniq
                for f_uniq in foreign_tbl.uniqueness()
                if tuple(f_uniq.columns()) == referred_cols
            )
            metadata += f"""
            INSERT INTO _synq_fk(field, foreign_index, on_delete, on_update)
            VALUES(
                {ids[(tbl, fk)]}, {ids[(foreign_tbl, f_uniq)]},
                {FK_ACTION[normalize_fk_action(fk.on_delete, conf)]},
                {FK_ACTION[normalize_fk_action(fk.on_update, conf)]}
            );
            """
        log_updates = ""
        log_insertions = ""
        if len(replicated_cols) > 0:
            log_tuples = ", ".join(
                f'({ids[(tbl, col)]}, NEW."{col.name}")' for col in replicated_cols
            )
            log_insertions += f"""
            INSERT INTO _synq_log(ts, peer, row_ts, row_peer, field, val)
            SELECT local.ts, local.peer, local.ts, local.peer, tuples.*
            FROM _synq_local AS local, (VALUES {log_tuples}) AS tuples;
            """.strip()
            log_changed_tuples = "UNION ALL".join(
                f"""
                SELECT {ids[(tbl, col)]}, NEW."{col.name}"
                WHERE OLD."{col.name}" IS NOT NEW."{col.name}"
                """
                for col in replicated_cols
            )
            log_updates += f"""
            INSERT INTO _synq_log(ts, peer, row_ts, row_peer, field, val)
            SELECT local.ts, local.peer, cur.row_ts, cur.row_peer, tuples.*
            FROM _synq_local AS local, "_synq_id_{tbl_name}" AS cur,
                ({log_changed_tuples}) AS tuples
            WHERE cur.rowid = NEW.rowid;
            """.strip()
        fklog_updates = ""
        fklog_insertions = ""
        for fk in tbl.foreign_keys():
            foreign_tbl_name = fk.foreign_table[0]
            foreign_tbl = tables[foreign_tbl_name]
            referred_cols = sql.referred_columns(fk, tables)
            old_referred_match = " AND ".join(
                f'"{ref_col}" = OLD."{col}"'
                for ref_col, col in zip(referred_cols, fk.columns)
            )
            new_referred_match = " AND ".join(
                f'"{ref_col}" = NEW."{col}"'
                for ref_col, col in zip(referred_cols, fk.columns)
            )
            null_ref_match = " AND ".join(
                f'NEW."{col}" IS NULL'
                for ref_col, col in zip(referred_cols, fk.columns)
            )
            fklog_insertions += f"""
                -- Handle case where at least one col is NULL
                INSERT INTO _synq_fklog(ts, peer, row_ts, row_peer, field, foreign_row_ts, foreign_row_peer)
                SELECT
                    local.ts, local.peer, local.ts, local.peer, {ids[(tbl, fk)]},
                    target.row_ts, target.row_peer
                FROM _synq_local AS local LEFT JOIN (
                    SELECT row_ts, row_peer FROM "_synq_id_{foreign_tbl_name}"
                    WHERE rowid = (
                        SELECT rowid FROM "{foreign_tbl_name}"
                        WHERE {new_referred_match}
                    )
                ) AS target;
            """.rstrip()
            fklog_updates += f"""
                -- Handle case where at least one col is NULL
                INSERT INTO _synq_fklog(ts, peer, row_ts, row_peer, field, foreign_row_ts, foreign_row_peer)
                SELECT
                    local.ts, local.peer, cur.row_ts, cur.row_peer, {ids[(tbl, fk)]},
                    target.row_ts, target.row_peer
                FROM _synq_local AS local, (
                    SELECT * FROM "_synq_id_{tbl_name}" WHERE rowid = NEW.rowid
                ) AS cur LEFT JOIN (
                    SELECT row_ts, row_peer FROM "_synq_id_{foreign_tbl_name}"
                    WHERE rowid = (
                        SELECT rowid FROM "{foreign_tbl_name}"
                        WHERE {new_referred_match}
                    )
                ) AS target
                WHERE NOT EXISTS(
                    -- is ON UPDATE CASCADE?
                    SELECT 1 FROM (
                        SELECT foreign_row_ts, foreign_row_peer FROM _synq_fklog
                        WHERE row_ts = cur.row_ts AND row_peer = cur.row_peer AND
                            field = {ids[(tbl, fk)]}
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
        CREATE TRIGGER "_synq_log_insert_{tbl_name}"
        AFTER INSERT ON "{tbl_name}"
        WHEN (SELECT NOT is_merging FROM _synq_local)
        BEGIN
            -- Handle INSERT OR REPLACE
            -- Delete trigger is not fired when recursive triggers are disabled.
            -- To ensure that the pre-existing row is deleted, we attempt a deletion.
            DELETE FROM "_synq_id_{tbl_name}" WHERE rowid = NEW.rowid;

            UPDATE _synq_local SET ts = ts + 1;
            UPDATE _synq_context SET ts = _synq_local.ts
            FROM _synq_local WHERE _synq_context.peer = _synq_local.peer;

            INSERT INTO "_synq_id_{tbl_name}"(rowid, row_ts, row_peer)
            SELECT NEW.rowid, ts, peer FROM _synq_local;

            INSERT INTO _synq_id(row_ts, row_peer, tbl)
            SELECT ts, peer, {ids[tbl]} FROM _synq_local;
            {log_insertions}
            {fklog_insertions}
        END;
        """.rstrip()
        tracked_cols = [f'"{x.name}"' for x in replicated_cols] + [
            f'"{name}"' for name in utils.foreign_column_names(tbl)
        ]
        if len(tracked_cols) > 0:
            triggers += f"""
            CREATE TRIGGER "_synq_log_update_{tbl_name}"
            AFTER UPDATE OF {','.join(tracked_cols)} ON "{tbl_name}"
            WHEN (SELECT NOT is_merging FROM _synq_local)
            BEGIN
                UPDATE _synq_local SET ts = ts + 1;
                UPDATE _synq_context SET ts = _synq_local.ts
                FROM _synq_local WHERE _synq_context.peer = _synq_local.peer;
                {log_updates}
                {fklog_updates}
            END;
            """.rstrip()
        rowid_aliases = utils.rowid_aliases(tbl)
        if len(rowid_aliases) > 0:
            triggers += f"""
            CREATE TRIGGER "_synq_log_update_rowid_{tbl_name}_rowid"
            AFTER UPDATE OF {", ".join(rowid_aliases)} ON "{tbl_name}"
            BEGIN
                UPDATE "_synq_id_{tbl_name}" SET rowid = NEW."{rowid_aliases[0]}"
                WHERE rowid = OLD."{rowid_aliases[0]}";
            END;
            """.rstrip()
        result += metadata + textwrap.dedent(table_synq_id) + textwrap.dedent(triggers)
    return result.strip()


def _get_schema(db: sqlite3.Connection) -> str:
    with closing(db.cursor()) as cursor:
        cursor.execute(_SELECT_AR_TABLE_SCHEMA)
        result = ";".join(r[0] for r in cursor) + ";"
    return result


def _allocate_id(db: sqlite3.Connection, /, *, id: int | None = None) -> None:
    with closing(db.cursor()) as cursor:
        if id is None:
            # Generate an identifier with 48 bits of entropy
            cursor.execute(f"UPDATE _synq_local SET peer = (random() >> 16);")
        else:
            cursor.execute(f"UPDATE _synq_local SET peer = {id};")
        cursor.execute(
            "INSERT INTO _synq_context(peer, ts) SELECT peer, 0 FROM _synq_local;"
        )


def init(
    db: sqlite3.Connection, /, *, id: int | None = None, conf: Config = Config()
) -> None:
    sql_ar_schema = _get_schema(db)
    tables = sql.symbols(parse_schema(sql_ar_schema))
    with closing(db.cursor()) as cursor:
        cursor.executescript(
            _CREATE_TABLE_CONTEXT
            + _CREATE_REPLICATION_TABLES
            + _CREATE_LOCAL_TABLES_VIEWS
        )
        cursor.executescript(_synq_triggers(tables, conf))
        if not conf.physical_clock:
            cursor.execute(f"DROP TRIGGER _synq_local_clock;")
    _allocate_id(db, id=id)


def fingerprint(db: sqlite3.Connection, fp_path: pathlib.Path | str, /) -> None:
    db.commit()
    with sqlite3.connect(fp_path) as target, closing(target.cursor()) as cursor:
        cursor.executescript(_CREATE_TABLE_CONTEXT)
    script = f"""
    ATTACH DATABASE '{fp_path}' AS extern;
    INSERT INTO extern._synq_context SELECT * FROM _synq_context;
    DETACH DATABASE extern;
    """
    db.executescript(script)


def delta(
    db: sqlite3.Connection,
    fp_path: pathlib.Path | str,
    delta_path: pathlib.Path | str,
    /,
) -> None:
    db.commit()
    with sqlite3.connect(delta_path) as delta, closing(delta.cursor()) as cursor:
        cursor.executescript(_CREATE_TABLE_CONTEXT + _CREATE_REPLICATION_TABLES)
    script = f"""
    ATTACH DATABASE '{fp_path}' AS fp;
    ATTACH DATABASE '{delta_path}' AS delta;
    INSERT INTO extern._synq_context SELECT * FROM _synq_context;
    DETACH DATABASE delta;
    DETACH DATABASE fp;
    """


def clone_to(
    db: sqlite3.Connection, target: sqlite3.Connection, /, *, id: int | None = None
) -> None:
    db.commit()
    db.backup(target)
    _allocate_id(target, id=id)


def pull_from(db: sqlite3.Connection, remote_db_path: pathlib.Path | str) -> None:
    sql_ar_schema = _get_schema(db)
    tables = sql.symbols(parse_schema(sql_ar_schema))
    merging = _create_pull(tables)
    result = f"""
        PRAGMA defer_foreign_keys = ON;  -- automatically switch off at the end of transaction

        ATTACH DATABASE '{remote_db_path}' AS extern;

        UPDATE _synq_local SET is_merging = 1;

        {_PULL_EXTERN}
        {_CONFLICT_RESOLUTION}
        {merging}
        {_MERGE_END}

        UPDATE _synq_local SET is_merging = 0;

        DETACH DATABASE extern;
        """
    result = textwrap.dedent(result)
    with closing(db.cursor()) as cursor:
        cursor.executescript(result)


_PULL_EXTERN = """
-- Update clock
UPDATE _synq_local SET ts = max(_synq_local.ts, max(ctx.ts)) + 1
FROM extern._synq_context AS ctx;

-- Add missing peers in the context with a ts of 0
INSERT OR IGNORE INTO _synq_context SELECT peer, 0 FROM extern._synq_context;
INSERT OR IGNORE INTO extern._synq_context SELECT peer, 0 FROM _synq_context;

-- Add new id and log entries
INSERT INTO _synq_id
SELECT id.* FROM extern._synq_id AS id JOIN _synq_context AS ctx
    ON id.row_ts > ctx.ts AND id.row_peer = ctx.peer;

INSERT INTO _synq_log
SELECT log.* FROM extern._synq_log AS log JOIN _synq_context AS ctx
    ON log.ts > ctx.ts AND log.peer = ctx.peer;

INSERT INTO _synq_fklog
SELECT fklog.*
FROM extern._synq_fklog AS fklog JOIN _synq_context AS ctx
    ON fklog.ts > ctx.ts AND fklog.peer = ctx.peer;

INSERT INTO _synq_id_undo
SELECT log.* FROM extern._synq_id_undo AS log JOIN _synq_context AS ctx
    ON log.ts > ctx.ts AND log.peer = ctx.peer
WHERE true  -- avoid parsing ambiguity
ON CONFLICT DO UPDATE SET ul = excluded.ul, ts = excluded.ts, peer = excluded.peer
WHERE ul < excluded.ul;

INSERT INTO _synq_undolog
SELECT log.* FROM extern._synq_undolog AS log JOIN _synq_context AS ctx
    ON log.ts > ctx.ts AND log.peer = ctx.peer
WHERE true  -- avoid parsing ambiguity
ON CONFLICT DO UPDATE SET ul = excluded.ul, ts = excluded.ts, peer = excluded.peer
WHERE ul < excluded.ul;
"""

_CONFLICT_RESOLUTION = f"""
-- A. ON UPDATE RESTRICT
-- undo all concurrent updates to a restrict ref
INSERT OR REPLACE INTO _synq_undolog(ts, peer, obj_peer, obj_ts, ul)
SELECT local.ts, local.peer, log.peer, log.ts, log.ul + 1
FROM _synq_local AS local, _synq_context AS ctx, extern._synq_context AS ectx,
    _synq_log_extra AS log JOIN _synq_uniqueness AS uniq USING(field), _synq_fklog_effective AS fklog
WHERE (
    (log.ts > ctx.ts AND log.peer = ctx.peer AND fklog.ts > ectx.ts AND fklog.peer = ectx.peer) OR
    (log.ts > ectx.ts AND log.peer = ectx.peer AND fklog.ts > ctx.ts AND fklog.peer = ctx.peer)
) AND (
    log.row_ts = fklog.foreign_row_ts AND
    log.row_peer = fklog.foreign_row_peer AND
    uniq.tbl_index = fklog.foreign_index
) AND fklog.on_update = 1 AND log.ul%2 = 0;

-- B. ON DELETE RESTRICT
INSERT OR REPLACE INTO _synq_id_undo(ts, peer, row_ts, row_peer, ul)
WITH RECURSIVE _synq_restrict_refs(foreign_row_ts, foreign_row_peer) AS (
    SELECT foreign_row_ts, foreign_row_peer
    FROM _synq_fklog_effective
    WHERE on_delete = 1 AND row_ul%2 = 0
    UNION
    SELECT target.foreign_row_ts, target.foreign_row_peer
    FROM _synq_restrict_refs AS src JOIN _synq_fklog_effective AS target
        ON src.foreign_row_ts = target.row_ts AND src.foreign_row_peer = target.row_peer
    WHERE on_delete = 0
)
SELECT local.ts, local.peer, row_ts, row_peer, ul + 1
FROM _synq_local AS local, _synq_restrict_refs JOIN _synq_id_undo
    ON foreign_row_ts = row_ts AND foreign_row_peer = row_peer
WHERE ul%2 = 1;

-- C. ON UPDATE SET NULL
INSERT INTO _synq_fklog_effective(row_ts, row_peer, field)
SELECT fklog.row_ts, fklog.row_peer, fklog.field
FROM _synq_context AS ctx, extern._synq_context AS ectx,
    _synq_log_effective AS log JOIN _synq_uniqueness AS uniq USING(field), _synq_fklog_effective AS fklog
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
INSERT OR REPLACE INTO _synq_id_undo(ts, peer, row_ts, row_peer, ul)
WITH _synq_unified_log_effective AS (
    SELECT
        log.ts, log.peer, log.row_ts, log.row_peer, log.field,
        log.val, NULL AS foreign_row_ts, NULL AS foreign_row_peer, log.row_ul
    FROM _synq_log_effective AS log
    UNION ALL
    SELECT
        fklog.ts, fklog.peer, fklog.row_ts, fklog.row_peer, fklog.field,
        NULL AS val, fklog.foreign_row_ts, fklog.foreign_row_peer, fklog.row_ul
    FROM _synq_fklog_effective AS fklog
)
SELECT DISTINCT local.ts, local.peer, log.row_ts, log.row_peer, log.row_ul + 1
FROM _synq_local AS local, _synq_unified_log_effective AS log JOIN _synq_unified_log_effective AS self
        ON log.field = self.field AND (
            log.val = self.val OR (
                log.foreign_row_ts = self.foreign_row_ts AND
                log.foreign_row_peer = self.foreign_row_peer
            )
        ) JOIN _synq_uniqueness AS uniq USING(field),
    _synq_context AS ctx, extern._synq_context AS ectx
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
    SELECT count(DISTINCT field) FROM _synq_uniqueness WHERE tbl_index = uniq.tbl_index
);

-- E. ON DELETE CASCADE
INSERT OR REPLACE INTO _synq_id_undo(ts, peer, row_ts, row_peer, ul)
WITH RECURSIVE _synq_dangling_refs(row_ts, row_peer, row_ul) AS (
    SELECT fklog.row_ts, fklog.row_peer, fklog.row_ul
    FROM _synq_fklog_effective AS fklog JOIN _synq_id_undo AS undo
        ON fklog.foreign_row_ts = undo.row_ts AND fklog.foreign_row_peer = undo.row_peer
    WHERE fklog.on_delete <> 2 AND fklog.row_ul%2 = 0 AND undo.ul%2 = 1
    UNION
    SELECT src.row_ts, src.row_peer, src.row_ul
    FROM _synq_dangling_refs AS target JOIN _synq_fklog_effective AS src
        ON src.foreign_row_ts = target.row_ts AND src.foreign_row_peer = target.row_peer
    WHERE src.row_ul%2 = 0
)
SELECT local.ts, local.peer, row_ts, row_peer, row_ul+1
FROM _synq_local AS local, _synq_dangling_refs
WHERE row_ul%2 = 0;
"""

_MERGE_END = """
-- Update context
UPDATE _synq_context SET ts = ctx.ts FROM extern._synq_context AS ctx
WHERE ctx.ts > _synq_context.ts AND _synq_context.peer = ctx.peer;

UPDATE _synq_context SET ts = local.ts
FROM _synq_local AS local JOIN _synq_id_undo USING(peer, ts)
WHERE _synq_context.peer = local.peer;

UPDATE _synq_context SET ts = local.ts
FROM _synq_local AS local JOIN _synq_undolog USING(peer, ts)
WHERE _synq_context.peer = local.peer;

UPDATE _synq_context SET ts = local.ts
FROM _synq_local AS local JOIN _synq_log USING(peer, ts)
WHERE _synq_context.peer = local.peer;

UPDATE _synq_context SET ts = local.ts
FROM _synq_local AS local JOIN _synq_fklog USING(peer, ts)
WHERE _synq_context.peer = local.peer;
"""


def _create_pull(tables: sql.Symbols) -> str:
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
                SELECT log.val FROM _synq_log_extra AS log
                WHERE log.row_ts = id.row_ts AND log.row_peer = id.row_peer AND
                    log.field = {ids[(tbl, col)]} AND log.ul%2 = 0
                ORDER BY log.ts DESC, log.peer DESC LIMIT 1
            ) AS "{col.name}"'''
            ]
        for fk in tbl.foreign_keys():
            f_tbl = tables[fk.foreign_table[0]]
            for col_name in fk.columns:
                if col_name not in col_names:
                    col_names += [col_name]
                    selector = f"""
                    SELECT fklog.* FROM _synq_fklog_extra AS fklog
                    WHERE fklog.field = {ids[(tbl, fk)]} AND fklog.ul%2 = 0 AND
                        fklog.row_peer = id.row_peer AND fklog.row_ts = id.row_ts AND
                        fklog.row_ul%2 = 0
                    ORDER BY fklog.ts DESC, fklog.peer DESC LIMIT 1
                    """
                    referred_tbl = tables[fk.foreign_table[0]]
                    for referred in sql.resolve_foreign_key(fk, col_name, tables):
                        if isinstance(referred, sql.ForeignKey):
                            selector = f"""
                            SELECT fklog2.* FROM (
                                {selector}
                            ) AS fklog LEFT JOIN _synq_fklog_extra AS fklog2
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
                                ) AS fklog LEFT JOIN "_synq_id_{referred_tbl.name[0]}" AS rw
                                    ON fklog.foreign_row_peer = rw.row_peer AND
                                        fklog.foreign_row_ts = rw.row_ts
                                """
                            else:
                                selector = f"""
                                SELECT log.val FROM (
                                        {selector}
                                ) AS fklog LEFT JOIN _synq_log_extra AS log
                                    ON log.row_peer = fklog.foreign_row_peer AND
                                        log.row_ts = fklog.foreign_row_ts AND
                                        log.field = {ids[(referred_tbl, ref_col)]} AND log.ul%2 = 0
                                WHERE log.row_ul%2 = 0
                                ORDER BY log.ts DESC, log.peer DESC LIMIT 1
                                """
                            selectors += [f'({selector}) AS "{col_name}"']
        merger += f"""
        INSERT OR REPLACE INTO "{tbl_name}"({', '.join(col_names)})
        WITH RECURSIVE _synq_unified_log AS (
            SELECT
            log.ts, log.peer, log.row_ts, log.row_peer, log.field,
            log.val, NULL AS foreign_row_ts, NULL AS foreign_row_peer,
            log.ul, log.ul_ts, log.ul_peer, log.row_ul
            FROM _synq_log_effective AS log
            UNION ALL
            SELECT
                fklog.ts, fklog.peer, fklog.row_ts, fklog.row_peer, fklog.field,
                NULL AS val, fklog.foreign_row_ts, fklog.foreign_row_peer,
                fklog.ul, fklog.ul_ts, fklog.ul_peer, fklog.row_ul
            FROM _synq_fklog_effective AS fklog
        ), _synq_cascade_refs(row_ts, row_peer, field) AS (
            -- on update cascade triggered by extern updates OR undone updates
            SELECT fklog.row_ts, fklog.row_peer, fklog.field
            FROM _synq_context AS ctx, extern._synq_context AS ectx, _synq_unified_log AS log
                JOIN _synq_uniqueness AS uniq USING(field)
                JOIN _synq_fklog_effective AS fklog
                    ON log.row_ts = fklog.foreign_row_ts AND
                        log.row_peer = fklog.foreign_row_peer AND
                        uniq.tbl_index = fklog.foreign_index
            WHERE fklog.on_update = 0 AND fklog.row_ul%2 = 0 AND
                ((log.peer = ctx.peer AND log.ts > ctx.ts) OR
                (log.ul_peer = ctx.peer AND log.ul_ts > ctx.ts))
            UNION
            SELECT src.row_ts, src.row_peer, src.field
            FROM _synq_cascade_refs AS target
                JOIN _synq_uniqueness AS uniq USING(field)
                JOIN _synq_fklog_effective AS src
                    ON src.foreign_row_ts = target.row_ts AND
                        src.foreign_row_peer = target.row_peer AND
                        uniq.tbl_index = src.foreign_index
            WHERE src.on_update = 0 AND src.row_ul%2 = 0
        )
        SELECT {', '.join(selectors)} FROM (
            SELECT id.rowid, id.row_ts, id.row_peer FROM (
                SELECT log.row_ts, log.row_peer FROM _synq_unified_log AS log
                    JOIN _synq_context AS ctx
                        ON (log.ul%2 = 0 AND log.peer = ctx.peer AND log.ts > ctx.ts) OR
                            (log.ul%2 = 1 AND log.ul_peer = ctx.peer AND log.ul_ts > ctx.ts)
                WHERE log.row_ul%2 = 0
                UNION
                SELECT redo.row_ts, redo.row_peer FROM _synq_id_undo AS redo
                    JOIN _synq_context AS ctx
                        ON redo.ts > ctx.ts AND redo.peer = ctx.peer
                WHERE NOT redo.ul%2 = 1
                UNION
                SELECT log.row_ts, log.row_peer FROM _synq_context AS ctx
                    JOIN _synq_undolog AS undo
                        ON undo.ts > ctx.ts AND undo.peer = ctx.peer
                    JOIN _synq_log AS log
                        ON undo.obj_ts = log.ts AND undo.obj_peer = log.peer
                WHERE undo.ul%2 = 1
                UNION
                SELECT log.row_ts, log.row_peer FROM _synq_context AS ctx
                    JOIN _synq_undolog AS undo
                        ON undo.ts > ctx.ts AND undo.peer = ctx.peer
                    JOIN _synq_fklog AS log
                        ON undo.obj_ts = log.ts AND undo.obj_peer = log.peer
                WHERE undo.ul%2 = 1
                UNION
                SELECT row_ts, row_peer FROM _synq_cascade_refs
                UNION
                SELECT fklog.row_ts, fklog.row_peer FROM _synq_fklog_extra AS fklog
                    JOIN _synq_id_undo AS undo
                        ON fklog.foreign_row_peer = undo.row_peer AND
                            fklog.foreign_row_ts = undo.row_ts
                WHERE undo.ul%2 = 1 AND fklog.on_delete = 2
            ) JOIN "_synq_id_{tbl_name}" AS id
                USING(row_ts, row_peer)
            UNION
            SELECT id.rowid, id.row_ts, id.row_peer FROM "_synq_id_{tbl_name}" AS id
                JOIN _synq_context AS ctx
                    ON id.row_ts > ctx.ts AND id.row_peer = ctx.peer
        ) AS id;
        """
        result += f"""
        -- Foreign keys must be disabled

        -- Apply deletion (existing rows)
        DELETE FROM "{tbl_name}" WHERE rowid IN (
            SELECT id.rowid FROM "_synq_id_{tbl_name}" AS id
                JOIN _synq_id_undo AS undo
                    ON id.row_ts = undo.row_ts AND id.row_peer = undo.row_peer
            WHERE undo.ul%2 = 1
        );
 
        -- Auto-assign local rowids for new active rows
        INSERT INTO "_synq_id_{tbl_name}"(row_peer, row_ts)
        SELECT id.row_peer, id.row_ts
        FROM _synq_id AS id JOIN _synq_context AS ctx
            ON id.row_ts > ctx.ts AND id.row_peer = ctx.peer
        WHERE id.tbl = {ids[tbl]} AND NOT EXISTS(
            SELECT 1 FROM _synq_id_undo AS undo
            WHERE undo.ul%2 = 1 AND
                undo.row_ts = id.row_ts AND undo.row_peer = id.row_peer
        );

        -- Auto-assign local rowids for redone rows
        INSERT OR IGNORE INTO "_synq_id_{tbl_name}"(row_ts, row_peer)
        SELECT id.row_ts, id.row_peer
        FROM _synq_id_undo AS redo
            JOIN _synq_context AS ctx
                ON redo.ts > ctx.ts AND redo.peer = ctx.peer
            JOIN _synq_id AS id
                ON redo.row_ts = id.row_ts AND redo.row_peer = id.row_peer
                    AND id.tbl = {ids[tbl]}
        WHERE redo.ul%2 = 0;
        """
    result += merger
    return result
