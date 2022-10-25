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
        raise Exception("SybQLite does not support On DELETE/UPDATE SET DEFAULT")
    else:
        return action


_SELECT_AR_TABLE_SCHEMA = """--sql
SELECT sql FROM sqlite_master WHERE type = 'table' AND
    name NOT LIKE 'sqlite_%' AND name NOT LIKE '_synq_%';
"""

_CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS _synq_local(
    rowid integer PRIMARY KEY DEFAULT 1 CHECK(rowid = 1),
    peer integer NOT NULL DEFAULT 0,
    ts integer NOT NULL DEFAULT 0 CHECK(ts >= 0),
    is_merging integer NOT NULL DEFAULT 0 CHECK(is_merging & 1 = is_merging)
) STRICT;
INSERT INTO _synq_local DEFAULT VALUES;

-- use `UPDATE _synq_local SET ts = ts + 1` to refresh the hybrid logical clock
DROP TRIGGER IF EXISTS  _synq_local_clock;
CREATE TRIGGER          _synq_local_clock
AFTER UPDATE OF ts ON _synq_local WHEN (OLD.ts + 1 = NEW.ts)
BEGIN
    UPDATE _synq_local SET ts = max(NEW.ts, CAST(
        ((julianday('now') - julianday('1970-01-01')) * 86400.0 * 1000000.0) AS int
            -- unix epoch in nano-seconds
            -- support 5 centuries with 64bits epochs
            -- https://www.sqlite.org/lang_datefunc.html#examples
    ));
END;

DROP TRIGGER IF EXISTS  _synq_local_context_update;
CREATE TRIGGER          _synq_local_context_update
AFTER UPDATE OF ts ON _synq_local WHEN (NOT NEW.is_merging)
BEGIN
	UPDATE _synq_context SET ts = NEW.ts WHERE _synq_context.peer = NEW.peer;
END;

-- Causal context
CREATE TABLE IF NOT EXISTS _synq_context(
    peer integer PRIMARY KEY,
    ts integer NOT NULL DEFAULT 0 CHECK (ts >= 0)
) STRICT;

CREATE TABLE IF NOT EXISTS _synq_id(
    row_ts integer NOT NULL,
    row_peer integer NOT NULL REFERENCES _synq_context(peer) ON DELETE CASCADE ON UPDATE CASCADE,
    tbl integer NOT NULL,
    PRIMARY KEY(row_ts DESC, row_peer DESC),
    UNIQUE(row_peer, row_ts)
) STRICT, WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS _synq_id_undo(
    row_ts integer NOT NULL,
    row_peer integer NOT NULL,
    ul integer NOT NULL DEFAULT 0 CHECK(ul >= 0), -- undo length
    ts integer NOT NULL CHECK(ts >= row_ts),
    peer integer NOT NULL,
    PRIMARY KEY(row_ts DESC, row_peer DESC),
    FOREIGN KEY(row_ts, row_peer) REFERENCES _synq_id(row_ts, row_peer)
        ON DELETE CASCADE ON UPDATE CASCADE
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS _synq_id_undo_index_ts ON _synq_id_undo(peer, ts);

DROP VIEW IF EXISTS _synq_id_extra;
CREATE VIEW         _synq_id_extra AS
SELECT id.row_ts, id.row_peer, id.tbl, tbl_data.name, ifnull(undo.ul, 0) AS row_ul
FROM _synq_id AS id
    LEFT JOIN _synq_id_undo AS undo USING(row_ts, row_peer)
    LEFT JOIN _synq_names AS tbl_data ON tbl = tbl_data.id;

CREATE TABLE IF NOT EXISTS _synq_names(
    id integer NOT NULL PRIMARY KEY,
    name text NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS _synq_uniqueness(
    field integer NOT NULL,
    tbl_index integer NOT NULL,
    PRIMARY KEY(field, tbl_index)
) STRICT;

CREATE TABLE IF NOT EXISTS _synq_fk(
    field integer NOT NULL PRIMARY KEY,
    -- ON DELETE/UPDATE action info
    -- 0: CASCADE, 1: RESTRICT, 2: SET NULL
    on_delete integer NOT NULL CHECK(0 <= on_delete AND on_delete <= 2),
    on_update integer NOT NULL CHECK(0 <= on_update AND on_update <= 2),
    foreign_index integer NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS _synq_log(
    ts integer NOT NULL CHECK(ts >= row_ts),
    peer integer NOT NULL,
    row_ts integer NOT NULL,
    row_peer integer NOT NULL,
    field integer NOT NULL,
    val any,
    -- index id (for unique keys)
    -- column of a composite key have the same index id
    -- WARNING: interleaved indexes are not supported
    PRIMARY KEY(row_ts, row_peer, field, ts, peer),
    FOREIGN KEY(row_ts, row_peer) REFERENCES _synq_id(row_ts, row_peer)
        ON DELETE CASCADE ON UPDATE CASCADE
) STRICT;
CREATE INDEX IF NOT EXISTS _synq_log_index_ts ON _synq_log(peer, ts);

CREATE TABLE IF NOT EXISTS _synq_fklog(
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
CREATE INDEX IF NOT EXISTS _synq_fklog_index_ts ON _synq_fklog(peer, ts);

CREATE TABLE IF NOT EXISTS _synq_undolog(
    obj_ts integer NOT NULL,
    obj_peer integer NOT NULL,
    ul integer NOT NULL DEFAULT 0 CHECK(ul >= 0), -- undo length
    ts integer NOT NULL CHECK(ts >= obj_ts),
    peer integer NOT NULL,
    PRIMARY KEY(obj_ts DESC, obj_peer DESC)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS _synq_undolog_ts ON _synq_undolog(peer, ts);

DROP VIEW IF EXISTS _synq_log_extra;
CREATE VIEW         _synq_log_extra AS
SELECT log.*,
    id.tbl, ifnull(undo.ul, 0) AS ul, id.row_ul,
    fields.tbl_index, field_data.name
FROM _synq_log AS log
    LEFT JOIN _synq_id_extra AS id
        USING(row_ts, row_peer)
    LEFT JOIN _synq_undolog AS undo
        ON log.ts = undo.obj_ts AND log.peer = undo.obj_peer
    LEFT JOIN _synq_uniqueness AS fields
        USING(field)
    LEFT JOIN _synq_names AS field_data
        ON field = field_data.id;

DROP VIEW IF EXISTS _synq_log_effective;
CREATE VIEW         _synq_log_effective AS
SELECT log.* FROM _synq_log_extra AS log
WHERE log.ul%2 = 0 AND NOT EXISTS(
    SELECT 1 FROM _synq_log_extra AS self
    WHERE self.row_ts = log.row_ts AND self.row_peer = log.row_peer AND self.field = log.field AND
        (self.ts > log.ts OR (self.ts = log.ts AND self.peer > log.peer)) AND self.ul%2 = 0
);

DROP VIEW IF EXISTS _synq_fklog_extra;
CREATE VIEW         _synq_fklog_extra AS
SELECT fklog.*,
    id.tbl, ifnull(undo.ul, 0) AS ul, id.row_ul,
    fields.tbl_index, field_data.name,
    fk.on_update, fk.on_delete, fk.foreign_index
FROM _synq_fklog AS fklog
    LEFT JOIN _synq_id_extra AS id
        USING(row_ts, row_peer)
    LEFT JOIN _synq_undolog AS undo
        ON fklog.ts = undo.obj_ts AND fklog.peer = undo.obj_peer
    LEFT JOIN _synq_uniqueness AS fields
        USING(field)
    LEFT JOIN _synq_fk AS fk
        USING(field)
    LEFT JOIN _synq_names AS field_data
        ON field = field_data.id;

DROP VIEW IF EXISTS _synq_fklog_effective;
CREATE VIEW         _synq_fklog_effective AS
SELECT fklog.* FROM _synq_fklog_extra AS fklog
WHERE fklog.ul%2=0 AND NOT EXISTS(
    SELECT 1 FROM _synq_fklog_extra AS self
    WHERE self.ul%2=0 AND
        self.row_ts = fklog.row_ts AND self.row_peer = fklog.row_peer AND self.field = fklog.field AND
        (self.ts > fklog.ts OR (self.ts = fklog.ts AND self.peer > fklog.peer))
);

DROP TRIGGER IF EXISTS  _synq_fklog_effective_insert;
CREATE TRIGGER          _synq_fklog_effective_insert
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

DROP VIEW IF EXISTS _synq_unified_log_effective;
CREATE VIEW         _synq_unified_log_effective AS
SELECT
    log.ts, log.peer, log.row_ts, log.row_peer, log.field, log.tbl_index,
    log.val AS val_p1, NULL AS val_p2, log.row_ul
FROM _synq_log_effective AS log
UNION ALL
SELECT
    fklog.ts, fklog.peer, fklog.row_ts, fklog.row_peer, fklog.field, fklog.tbl_index,
    fklog.foreign_row_ts AS val_p1, fklog.foreign_row_peer AS val_p2, fklog.row_ul
FROM _synq_fklog_effective AS fklog;
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
        tbl_uniqueness = list(tbl.uniqueness())
        replicated_cols = utils.replicated_columns(tbl)
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
        maybe_autoinc = " AUTOINCREMENT" if utils.primary_key(tbl).autoincrement else ""
        maybe_rowid_alias = (
            f"rowid integer PRIMARY KEY{maybe_autoinc},"
            if utils.has_rowid_alias(tbl)
            else ""
        )
        table_synq_id = f"""
        CREATE TABLE IF NOT EXISTS "_synq_id_{tbl_name}"(
            {maybe_rowid_alias}
            row_ts integer NOT NULL,
            row_peer integer NOT NULL,
            UNIQUE(row_ts, row_peer),
            FOREIGN KEY(row_ts, row_peer) REFERENCES _synq_id(row_ts, row_peer)
                ON DELETE RESTRICT ON UPDATE CASCADE
        ) STRICT;

        DROP TRIGGER IF EXISTS "_synq_delete_{tbl_name}";
        CREATE TRIGGER "_synq_delete_{tbl_name}"
        AFTER DELETE ON "{tbl_name}" WHEN (SELECT NOT is_merging FROM _synq_local)
        BEGIN
            DELETE FROM "_synq_id_{tbl_name}" WHERE rowid = OLD.rowid;
        END;

        DROP TRIGGER IF EXISTS "_synq_delete_id_{tbl_name}";
        CREATE TRIGGER "_synq_delete_id_{tbl_name}"
        AFTER DELETE ON "_synq_id_{tbl_name}"
        WHEN (SELECT NOT is_merging FROM _synq_local)
        BEGIN
            UPDATE _synq_local SET ts = ts + 1;

            INSERT INTO _synq_id_undo(ts, peer, row_ts, row_peer, ul)
            SELECT local.ts, local.peer, OLD.row_ts, OLD.row_peer, 1
            FROM _synq_local AS local
            WHERE true  -- avoid parsing ambiguity
            ON CONFLICT(row_ts, row_peer)
            DO UPDATE SET ul = ul + 1, ts = excluded.ts, peer = excluded.peer;
        END;
        """
        insertions = ""
        updates = ""
        triggers = ""
        metadata = (
            f"INSERT OR IGNORE INTO _synq_names VALUES({ids[tbl]}, '{tbl_name}');"
        )
        for cst in tbl.all_constraints():
            if cst.name is not None:
                metadata += f"""INSERT OR IGNORE INTO _synq_names VALUES({ids[(tbl, cst)]}, '{cst.name}');"""
        for col in replicated_cols:
            metadata += f"""
            INSERT OR IGNORE INTO _synq_names VALUES({ids[(tbl, col)]}, '{col.name}');
            """
            for uniq in tbl_uniqueness:
                if col.name in uniq.columns():
                    metadata += f"""
                    INSERT OR REPLACE INTO _synq_uniqueness(field, tbl_index)
                    VALUES({ids[(tbl, col)]}, {ids[(tbl, uniq)]});
                    """.rstrip()
            insertions += f"""
            INSERT INTO _synq_log(ts, peer, row_ts, row_peer, field, val)
            SELECT local.ts, local.peer, cur.row_ts, cur.row_peer, {ids[(tbl, col)]}, NEW."{col.name}"
            FROM _synq_local AS local, (
                SELECT * FROM "_synq_id_{tbl_name}" WHERE rowid = NEW.rowid
            ) AS cur;
            """.rstrip()
            updates += f"""
            INSERT INTO _synq_log(ts, peer, row_ts, row_peer, field, val)
            SELECT local.ts, local.peer, cur.row_ts, cur.row_peer, {ids[(tbl, col)]}, NEW."{col.name}"
            FROM _synq_local AS local, (
                SELECT * FROM "_synq_id_{tbl_name}" WHERE rowid = NEW.rowid
            ) AS cur
            WHERE OLD."{col.name}" IS NOT NEW."{col.name}";
            """
        for fk in tbl.foreign_keys():
            for uniq in tbl_uniqueness:
                if any(col_name in uniq.columns() for col_name in fk.columns):
                    metadata += f"""
                    INSERT OR REPLACE INTO _synq_uniqueness(field, tbl_index)
                    VALUES({ids[(tbl, fk)]}, {ids[(tbl, uniq)]});
                    """.rstrip()
            foreign_tbl_name = fk.foreign_table.name[0]
            foreign_tbl = tables.get(foreign_tbl_name)
            assert foreign_tbl is not None
            changed_values = " OR ".join(
                f'OLD."{col_name}" IS NOT NEW."{col_name}"' for col_name in fk.columns
            )
            referred_cols = list(
                fk.referred_columns
                if fk.referred_columns is not None
                else utils.primary_key(foreign_tbl).columns()
            )
            foreign_uniqueness = list(foreign_tbl.uniqueness())
            f_uniq = next(
                f_uniq
                for f_uniq in foreign_uniqueness
                if list(f_uniq.columns()) == referred_cols
            )
            new_referred_match = " AND ".join(
                f'"{ref_col}" = NEW."{col}"'
                for ref_col, col in zip(referred_cols, fk.columns)
            )
            metadata += f"""
            INSERT OR REPLACE INTO _synq_fk(field, foreign_index, on_delete, on_update)
            VALUES(
                {ids[(tbl, fk)]}, {ids[(foreign_tbl, f_uniq)]},
                {FK_ACTION[normalize_fk_action(fk.on_delete, conf)]},
                {FK_ACTION[normalize_fk_action(fk.on_update, conf)]}
            );
            """
            fk_ins_up = f"""
                UPDATE _synq_fklog SET
                    foreign_row_ts = target.row_ts,
                    foreign_row_peer = target.row_peer
                FROM _synq_local AS local, (
                    SELECT row_ts, row_peer FROM "_synq_id_{foreign_tbl_name}"
                    WHERE rowid = (
                        SELECT rowid FROM "{foreign_tbl_name}"
                        WHERE {new_referred_match}
                    )
                ) AS target
                WHERE _synq_fklog.ts = local.ts AND _synq_fklog.peer = local.peer AND
                    _synq_fklog.field = {ids[(tbl, fk)]};
            """.strip()
            insertions += f"""
                -- handle case where at least one col is NULL
                INSERT INTO _synq_fklog(ts, peer, row_ts, row_peer, field)
                SELECT
                    local.ts, local.peer, cur.row_ts, cur.row_peer, {ids[(tbl, fk)]}
                FROM _synq_local AS local, (
                    SELECT * FROM "_synq_id_{tbl_name}" WHERE rowid = NEW.rowid
                ) AS cur;
                {fk_ins_up}
            """.rstrip()
            updates += f"""
                -- handle case where at least one col is NULL
                INSERT INTO _synq_fklog(ts, peer, row_ts, row_peer, field)
                SELECT
                    local.ts, local.peer, cur.row_ts, cur.row_peer, {ids[(tbl, fk)]}
                FROM _synq_local AS local, (
                    SELECT * FROM "_synq_id_{tbl_name}" WHERE rowid = NEW.rowid
                ) AS cur
                WHERE {changed_values};
                {fk_ins_up}
            """
        triggers += f"""
        DROP TRIGGER IF EXISTS "_synq_log_insert_{tbl_name}";
        CREATE TRIGGER "_synq_log_insert_{tbl_name}"
        AFTER INSERT ON "{tbl_name}"
        WHEN (SELECT NOT is_merging FROM _synq_local)
        BEGIN
            -- handle INSERT OR REPLACE (DELETE if exists, then INSERT)
            -- The delete trigger is not fired whether recursive triggers
            -- are not enabled.
            -- To ensure that the prexisting row is deleted, we attempt a deletion.
            DELETE FROM "_synq_id_{tbl_name}" WHERE rowid = NEW.rowid;

            UPDATE _synq_local SET ts = ts + 1; -- triggers clock update

            INSERT INTO "_synq_id_{tbl_name}"(rowid, row_ts, row_peer)
            SELECT NEW.rowid, ts, peer FROM _synq_local;

            INSERT INTO _synq_id(row_ts, row_peer, tbl)
            SELECT ts, peer, {ids[tbl]} FROM _synq_local;
            {insertions}
        END;
        """.rstrip()
        tracked_cols = [f'"{x.name}"' for x in replicated_cols] + [
            f'"{name}"' for name in utils.foreign_column_names(tbl)
        ]
        if len(tracked_cols) > 0:
            triggers += f"""
            DROP TRIGGER IF EXISTS "_synq_log_update_{tbl_name}";
            CREATE TRIGGER "_synq_log_update_{tbl_name}"
            AFTER UPDATE OF {','.join(tracked_cols)} ON "{tbl_name}"
            WHEN (SELECT NOT is_merging FROM _synq_local)
            BEGIN
                UPDATE _synq_local SET ts = ts + 1; -- triggers clock update
                {updates}
            END;
            """.rstrip()
        rowid_aliases = utils.rowid_aliases(tbl)
        if len(rowid_aliases) > 0:
            triggers += f"""
            DROP TRIGGER IF EXISTS "_synq_log_update_rowid_{tbl_name}_rowid";
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
        cursor.executescript(_CREATE_TABLES)
        cursor.executescript(_synq_triggers(tables, conf))
        if not conf.physical_clock:
            cursor.execute(f"DROP TRIGGER IF EXISTS _synq_local_clock;")
    _allocate_id(db, id=id)


def clone_to(
    src: sqlite3.Connection, target: sqlite3.Connection, /, *, id: int | None = None
) -> None:
    src.commit()
    src.backup(target)
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
-- update clock
UPDATE _synq_local SET ts = max(
    _synq_local.ts,
    (SELECT max(ts) FROM extern._synq_context)
) + 1;

-- Add missing peers in the context with a ts of 0
INSERT OR IGNORE INTO _synq_context
SELECT peer, 0 FROM extern._synq_context;

INSERT OR IGNORE INTO extern._synq_context
SELECT peer, 0 FROM _synq_context;

-- Add new id and log entries
INSERT INTO _synq_id
SELECT ext_id.*
FROM extern._synq_id AS ext_id JOIN _synq_context AS ctx
    ON ext_id.row_ts > ctx.ts AND ext_id.row_peer = ctx.peer;

INSERT INTO _synq_log
SELECT log.*
FROM extern._synq_log AS log JOIN _synq_context AS ctx
    ON log.ts > ctx.ts AND log.peer = ctx.peer;

INSERT INTO _synq_fklog
SELECT fklog.*
FROM extern._synq_fklog AS fklog JOIN _synq_context AS ctx
    ON fklog.ts > ctx.ts AND fklog.peer = ctx.peer;

INSERT INTO _synq_id_undo(ts, peer, row_ts, row_peer, ul)
SELECT log.ts, log.peer, log.row_ts, log.row_peer, log.ul
FROM extern._synq_id_undo AS log JOIN _synq_context AS ctx
    ON log.ts > ctx.ts AND log.peer = ctx.peer
WHERE true  -- avoid parsing ambiguity
ON CONFLICT(row_ts, row_peer)
DO UPDATE SET ul = excluded.ul, ts = excluded.ts, peer = excluded.peer
WHERE ul < excluded.ul;

INSERT INTO _synq_undolog(ts, peer, obj_ts, obj_peer, ul)
SELECT log.ts, log.peer, log.obj_ts, log.obj_peer, log.ul
FROM extern._synq_undolog AS log JOIN _synq_context AS ctx
    ON log.ts > ctx.ts AND log.peer = ctx.peer
WHERE true  -- avoid parsing ambiguity
ON CONFLICT(obj_ts, obj_peer)
DO UPDATE SET ul = excluded.ul, ts = excluded.ts, peer = excluded.peer
WHERE ul < excluded.ul;
"""

_CONFLICT_RESOLUTION = f"""
-- Conflict resolution

-- A. ON DELETE RESTRICT
INSERT OR REPLACE INTO _synq_id_undo(ts, peer, row_ts, row_peer, ul)
WITH RECURSIVE restrict_refs(foreign_row_ts, foreign_row_peer) AS (
    SELECT foreign_row_ts, foreign_row_peer
    FROM _synq_fklog_effective
    WHERE on_delete = 1 AND row_ul%2 = 0
    UNION
    SELECT target.foreign_row_ts, target.foreign_row_peer
    FROM restrict_refs AS src JOIN _synq_fklog_effective AS target
        ON src.foreign_row_ts = target.row_ts AND src.foreign_row_peer = target.row_peer
)
SELECT local.ts, local.peer, row_ts, row_peer, ul + 1
FROM _synq_local AS local, restrict_refs JOIN _synq_id_undo
    ON foreign_row_ts = row_ts AND foreign_row_peer = row_peer
WHERE ul%2 = 1;

-- B. ON UPDATE
-- B.1. ON UPDATE RESTRICT
-- undo all concurrent updates to a restrict ref
INSERT OR REPLACE INTO _synq_undolog(ts, peer, obj_peer, obj_ts, ul)
SELECT local.ts, local.peer, log.peer, log.ts, log.ul + 1
FROM _synq_local AS local, _synq_context AS ctx, extern._synq_context AS ectx,
    _synq_log_extra AS log, _synq_fklog_effective AS fklog
WHERE (
    (log.ts > ctx.ts AND log.peer = ctx.peer AND fklog.ts > ectx.ts AND fklog.peer = ectx.peer) OR
    (log.ts > ectx.ts AND log.peer = ectx.peer AND fklog.ts > ctx.ts AND fklog.peer = ctx.peer)
) AND (
    log.row_ts = fklog.foreign_row_ts AND
    log.row_peer = fklog.foreign_row_peer AND
    log.tbl_index = fklog.foreign_index
) AND fklog.on_update = 1 AND log.ul%2 = 0;


-- B.2.. ON UPDATE CASCADE
INSERT INTO _synq_fklog_effective(
    row_ts, row_peer, field, foreign_row_ts, foreign_row_peer
)
SELECT
    fklog.row_ts, fklog.row_peer, fklog.field, log.row_ts, log.row_peer
FROM _synq_context AS ctx, extern._synq_context AS ectx,
    _synq_log_effective AS log, _synq_fklog_effective AS fklog
WHERE (
    (log.ts > ctx.ts AND log.peer = ctx.peer AND fklog.ts > ectx.ts AND fklog.peer = ectx.peer) OR
    (log.ts > ectx.ts AND log.peer = ectx.peer AND fklog.ts > ctx.ts AND fklog.peer = ctx.peer)
) AND (
    log.row_ts = fklog.foreign_row_ts AND
    log.row_peer = fklog.foreign_row_peer AND
    log.tbl_index = fklog.foreign_index
) AND fklog.on_update = 0;

-- B.3.. ON UPDATE SET NULL
INSERT INTO _synq_fklog_effective(row_ts, row_peer, field)
SELECT fklog.row_ts, fklog.row_peer, fklog.field
FROM _synq_context AS ctx, extern._synq_context AS ectx,
    _synq_log_effective AS log, _synq_fklog_effective AS fklog
WHERE (
    (log.ts > ctx.ts AND log.peer = ctx.peer AND fklog.ts > ectx.ts AND fklog.peer = ectx.peer) OR
    (log.ts > ectx.ts AND log.peer = ectx.peer AND fklog.ts > ctx.ts AND fklog.peer = ctx.peer)
) AND (
    log.row_ts = fklog.foreign_row_ts AND
    log.row_peer = fklog.foreign_row_peer AND
    log.tbl_index = fklog.foreign_index
) AND fklog.on_update = 2;

-- C. resolve uniqueness conflicts
-- undo latest rows with conflicting unique keys
INSERT OR REPLACE INTO _synq_id_undo(ts, peer, row_ts, row_peer, ul)
SELECT DISTINCT local.ts, local.peer, log.row_ts, log.row_peer, log.row_ul + 1
FROM _synq_local AS local, _synq_unified_log_effective AS log JOIN _synq_unified_log_effective AS self
        ON log.field = self.field AND log.tbl_index = self.tbl_index AND
            log.val_p1 = self.val_p1 AND log.val_p2 IS self.val_p2,
    _synq_context AS ctx, extern._synq_context AS ectx
WHERE (
    log.row_ts > self.row_ts OR (
        log.row_ts = self.row_ts AND log.row_peer > self.row_peer
    )
) AND (
    (log.ts > ctx.ts AND log.peer = ctx.peer AND self.ts > ectx.ts AND self.peer = ectx.peer) OR
    (log.ts > ectx.ts AND log.peer = ectx.peer AND self.ts > ctx.ts AND self.peer = ctx.peer)
)
GROUP BY log.row_ts, log.row_peer, self.row_ts, self.row_peer, log.tbl_index
HAVING count(*) >= (
    SELECT count(DISTINCT field) FROM _synq_uniqueness WHERE tbl_index = log.tbl_index
);

-- D. ON DELETE CASCADE
INSERT OR REPLACE INTO _synq_id_undo(ts, peer, row_ts, row_peer, ul)
WITH RECURSIVE dangling_refs(row_ts, row_peer, row_ul) AS (
    SELECT fklog.row_ts, fklog.row_peer, fklog.row_ul
    FROM _synq_fklog_effective AS fklog JOIN _synq_id_undo AS undo
        ON fklog.foreign_row_ts = undo.row_ts AND fklog.foreign_row_peer = undo.row_peer
    WHERE fklog.on_delete <> 2 AND fklog.row_ul%2 = 0 AND undo.ul%2 = 1
    UNION
    SELECT src.row_ts, src.row_peer, src.row_ul
    FROM dangling_refs AS target JOIN _synq_fklog_effective AS src
        ON src.foreign_row_ts = target.row_ts AND src.foreign_row_peer = target.row_peer
    WHERE src.row_ul%2 = 0
)
SELECT local.ts, local.peer, row_ts, row_peer, row_ul+1
FROM _synq_local AS local, dangling_refs
WHERE row_ul%2 = 0;

-- E. ON DELETE SET NULL
INSERT INTO _synq_fklog_effective(row_ts, row_peer, field)
SELECT fklog.row_ts, fklog.row_peer, fklog.field
FROM _synq_fklog_effective AS fklog JOIN _synq_id_undo AS undo
    ON fklog.foreign_row_ts = undo.row_ts AND fklog.foreign_row_peer = undo.row_peer
WHERE fklog.on_delete = 2 AND fklog.row_ul%2 = 0;
"""

_MERGE_END = """
-- Update context
UPDATE _synq_context SET ts = ctx.ts
FROM extern._synq_context AS ctx
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
        selectors: list[str] = ["id.rowid"]
        col_names: list[str] = ["rowid"]
        for col in utils.replicated_columns(tbl):
            col_names += [col.name]
            selectors += [
                f'''(
                SELECT log.val FROM _synq_log_extra AS log
                WHERE log.row_ts = id.row_ts AND log.row_peer = id.row_peer AND
                    log.field = {ids[(tbl, col)]} AND log.ul%2 = 0
                ORDER BY log.ts DESC, log.peer DESC
                LIMIT 1
            ) AS "{col.name}"'''
            ]
        for fk in tbl.foreign_keys():
            f_tbl_name = fk.foreign_table.name[0]
            f_tbl = tables.get(f_tbl_name)
            assert f_tbl is not None
            f_cols = utils.cols(f_tbl)
            f_repl_col_names = list(
                map(lambda col: col.name, utils.replicated_columns(f_tbl))
            )
            ref_col_names = (
                fk.referred_columns
                if fk.referred_columns is not None
                else tuple(utils.primary_key(f_tbl).columns())
            )
            if len(ref_col_names) == 1 and ref_col_names[0] not in f_repl_col_names:
                # auto-inc pk
                col_names += [fk.columns[0]]
                selectors += [
                    f'''(
                    SELECT rw.rowid
                    FROM _synq_fklog_extra AS fklog
                        LEFT JOIN "_synq_id_{f_tbl_name}" AS rw
                        ON fklog.foreign_row_ts = rw.row_ts AND
                            fklog.foreign_row_peer = rw.row_peer
                    WHERE fklog.ul%2 = 0 AND fklog.row_ts = id.row_ts AND
                        fklog.row_peer = id.row_peer AND
                        fklog.field = {ids[(tbl, fk)]}
                    ORDER BY fklog.ts DESC, fklog.peer DESC
                    LIMIT 1
                ) AS "{fk.columns[0]}"'''
                ]
            else:
                for col_name in fk.columns:
                    col_names += [col_name]
                    ref_col = f_cols[col_name]
                    selectors += [
                        f'''(
                        SELECT log.val
                        FROM _synq_fklog_extra AS fklog
                            LEFT JOIN _synq_log_extra AS log
                                ON log.row_ts = fklog.foreign_row_ts AND
                                log.row_peer = fklog.foreign_row_peer AND
                                log.field = {ids[(f_tbl, ref_col)]} AND log.ul%2 = 0
                        WHERE fklog.ul%2 = 0 AND fklog.row_ts = id.row_ts AND
                            fklog.row_peer = id.row_peer AND
                            fklog.field = {ids[(tbl, fk)]}
                        ORDER BY fklog.ts DESC, fklog.peer DESC, log.ts DESC, log.peer DESC
                        LIMIT 1
                    ) AS "{col.name}"'''
                    ]
        merger += f"""
        INSERT OR REPLACE INTO "{tbl_name}"({', '.join(col_names)})
        SELECT {', '.join(selectors)} FROM (
            SELECT id.rowid, id.row_ts, id.row_peer FROM (
                SELECT log.row_ts, log.row_peer
                FROM _synq_log_extra AS log
                    JOIN _synq_context AS ctx
                        ON log.ul%2 = 0 AND log.peer = ctx.peer AND log.ts > ctx.ts
                WHERE log.row_ul%2 = 0
                UNION
                SELECT fklog.row_ts, fklog.row_peer
                FROM _synq_fklog_extra AS fklog
                    JOIN _synq_context AS ctx
                        ON fklog.ul%2 = 0 AND fklog.peer = ctx.peer AND fklog.ts > ctx.ts
                WHERE fklog.row_ul%2 = 0
                UNION
                SELECT redo.row_ts, redo.row_peer
                FROM _synq_id_undo AS redo
                    JOIN _synq_context AS ctx
                        ON redo.ts > ctx.ts AND redo.peer = ctx.peer
                WHERE NOT redo.ul%2 = 1
                UNION
                SELECT log.row_ts, log.row_peer
                FROM _synq_context AS ctx
                    JOIN _synq_undolog AS undo
                        ON undo.ts > ctx.ts AND undo.peer = ctx.peer
                    JOIN _synq_log AS log
                        ON undo.obj_ts = log.ts AND undo.obj_peer = log.peer
                WHERE undo.ul%2 = 1
            ) JOIN "_synq_id_{tbl_name}" AS id
                USING(row_ts, row_peer)
            UNION
            SELECT id.rowid, id.row_ts, id.row_peer
            FROM "_synq_id_{tbl_name}" AS id
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

        DELETE FROM "_synq_id_{tbl_name}" WHERE rowid IN (
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
