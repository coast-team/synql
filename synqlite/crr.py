from sqlschm.parser import parse_schema
from sqlschm import sql
from contextlib import closing
import logging
import pathlib
import textwrap
import sqlite3
from synqlite import sqlschm_utils as utils

logging.basicConfig(level=logging.DEBUG)

FK_ACTION = {
    sql.OnUpdateDelete.CASCADE: 0,
    None: 1,  # same as NO_ACTION
    sql.OnUpdateDelete.NO_ACTION: 1,
    sql.OnUpdateDelete.RESTRICT: 2,
    sql.OnUpdateDelete.SET_DEFAULT: 3,
    sql.OnUpdateDelete.SET_NULL: 4,
}


_SELECT_AR_TABLE_SCHEMA = """--sql
SELECT sql FROM sqlite_master WHERE type = 'table' AND
    name NOT LIKE 'sqlite_%' AND name NOT LIKE '_synq_%';
"""

_CREATE_TABLES = """
-- use `UPDATE _synq_local SET ts = ts + 1` to refresh the hybrid logical clock

CREATE TABLE IF NOT EXISTS _synq_local(
    rowid integer PRIMARY KEY DEFAULT 1 CHECK(rowid = 1),
    peer integer NOT NULL DEFAULT (random() >> 16), -- 48bits of entropy
    ts integer NOT NULL DEFAULT 0 CHECK(ts >= 0),
    is_merging integer NOT NULL DEFAULT 0 CHECK(is_merging & 1 = is_merging),
    logcleanup integer NOT NULL DEFAULT 0 CHECK(logcleanup & 1 = logcleanup),
    undocleanup integer NOT NULL DEFAULT 0 CHECK(undocleanup & 1 = undocleanup)
);

DROP TRIGGER IF EXISTS  _synq_local_clock;
CREATE TRIGGER          _synq_local_clock
AFTER UPDATE OF ts ON _synq_local WHEN (OLD.ts + 1 = NEW.ts)
BEGIN
    UPDATE _synq_local SET ts = max(NEW.ts, CAST(
        ((julianday('now') - 2440587.5) * 86400.0 * 1000000.0) AS int
            -- unix epoch in nano-seconds
            -- https://www.sqlite.org/lang_datefunc.html#examples
    ));
END;

DROP TRIGGER IF EXISTS  _synq_local_context_update;
CREATE TRIGGER          _synq_local_context_update
AFTER UPDATE OF ts ON _synq_local WHEN (SELECT NOT is_merging FROM _synq_local)
BEGIN
	UPDATE _synq_context SET ts = NEW.ts WHERE _synq_context.peer = NEW.peer;
END;

-- Causal context
CREATE TABLE IF NOT EXISTS _synq_context(
    peer integer PRIMARY KEY,
    ts integer NOT NULL DEFAULT 0 CHECK (ts >= 0)
);

CREATE TABLE IF NOT EXISTS _synq_id(
    row_ts integer NOT NULL,
    row_peer integer NOT NULL,
    tbl text NOT NULL,
    PRIMARY KEY(row_ts, row_peer)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS _synq_log(
    ts integer NOT NULL,
    peer integer NOT NULL,
    row_ts integer NOT NULL,
    row_peer integer NOT NULL,
    col integer NOT NULL,
    val any,
    -- index id (for unique keys)
    -- column of a composite key have the same index id
    -- WARNING: interleaved indexes are not supported
    tbl_index integer DEFAULT NULL,
    PRIMARY KEY(ts, peer)
);

DROP TRIGGER IF EXISTS  _synq_log_cleanup;
CREATE TRIGGER          _synq_log_cleanup
AFTER INSERT ON _synq_log
-- Do not cleanup updates of indexes (they could be undone)
WHEN (NEW.tbl_index IS NULL AND (SELECT logcleanup FROM _synq_local))
BEGIN
    -- Delete shadowed entries
    DELETE FROM _synq_log WHERE (ts <> NEW.ts OR peer <> NEW.peer) AND
        row_ts = NEW.row_ts AND row_peer = NEW.row_peer AND col = NEW.col;
END;

CREATE TABLE IF NOT EXISTS _synq_fklog(
    ts integer NOT NULL,
    peer integer NOT NULL,
    row_ts integer NOT NULL,
    row_peer integer NOT NULL,
    fk_id integer NOT NULL,
    -- ON DELETE/UPDATE action info
    -- 0: CASCADE, 1: NO ACTION, 2: RESTRICT, 3: SET DEFAULT, 4: SET NULL
    on_delete integer NOT NULL CHECK(0 <= on_delete AND on_delete <= 4),
    on_update integer NOT NULL CHECK(0 <= on_delete AND on_delete <= 4),
    foreign_row_ts integer DEFAULT NULL,
    foreign_row_peer integer DEFAULT NULL,
    -- which tbl_index of the foreign row is used?
    foreign_index integer NOT NULL,
    -- allow graph marking
    -- 0: on delete mark, 1: on update mark
    mark integer DEFAULT NULL,
    PRIMARY KEY(ts, peer)
);

DROP TRIGGER IF EXISTS  _synq_fklog_cleanup;
CREATE TRIGGER          _synq_fklog_cleanup
AFTER INSERT ON _synq_fklog WHEN (SELECT logcleanup FROM _synq_local)
BEGIN
    -- Delete shadowed entries
    DELETE FROM _synq_fklog WHERE (ts <> NEW.ts OR peer <> NEW.peer) AND
        row_ts = NEW.row_ts AND row_peer = NEW.row_peer AND fk_id = NEW.fk_id;
END;

CREATE TABLE IF NOT EXISTS _synq_undolog(
    ts integer NOT NULL,
    peer integer NOT NULL,
    obj_ts integer NOT NULL,
    obj_peer integer NOT NULL,
    ul integer NOT NULL DEFAULT 0 CHECK(ul >= 0), -- undo length
    PRIMARY KEY(ts, peer)
) WITHOUT ROWID;

DROP TRIGGER IF EXISTS  _synq_undolog_cleanup;
CREATE TRIGGER          _synq_undolog_cleanup
AFTER INSERT ON _synq_undolog WHEN (SELECT (logcleanup OR undocleanup) FROM _synq_local)
BEGIN
    -- Delete shadowed entries
    DELETE FROM _synq_undolog WHERE (ts <> NEW.ts OR peer <> NEW.peer) AND
        obj_ts = NEW.obj_ts AND obj_peer = NEW.obj_peer AND ul <= NEW.ul;
END;

DROP VIEW IF EXISTS _synq_log_active;
CREATE VIEW         _synq_log_active AS
SELECT log.rowid, log.* FROM _synq_log AS log
    WHERE NOT EXISTS(
        -- do not take undone log entries and rows into account
        SELECT 1 FROM _synq_undolog AS undo
        WHERE (undo.obj_ts = log.ts AND undo.obj_peer = log.peer) OR
            (undo.obj_ts = log.row_ts AND undo.obj_peer = log.row_peer)
        GROUP BY undo.obj_ts, undo.obj_peer HAVING max(undo.ul)%2 = 1
    )
    GROUP BY row_ts, row_peer, col HAVING ts = max(ts);

DROP VIEW IF EXISTS _synq_fklog_active;
CREATE VIEW         _synq_fklog_active AS
SELECT log.rowid, log.* FROM _synq_fklog AS log
    WHERE NOT EXISTS(
        -- do not take undone log entries and rows into account
        SELECT 1 FROM _synq_undolog AS undo
        WHERE (undo.obj_ts = log.ts AND undo.obj_peer = log.peer) OR
            (undo.obj_ts = log.row_ts AND undo.obj_peer = log.row_peer)
        GROUP BY undo.obj_ts, undo.obj_peer HAVING max(undo.ul)%2 = 1
    )
    GROUP BY row_ts, row_peer, fk_id HAVING ts = max(ts);

DROP TRIGGER IF EXISTS  _synq_fklog_active_insert;
CREATE TRIGGER          _synq_fklog_active_insert
INSTEAD OF INSERT ON _synq_fklog_active WHEN (
    NEW.peer IS NULL AND NEW.ts IS NULL
)
BEGIN
    UPDATE _synq_local SET ts = ts + 1;

    INSERT INTO _synq_fklog(
        ts, peer, row_ts, row_peer, fk_id,
        on_delete, on_update, foreign_row_ts, foreign_row_peer,
        foreign_index, mark
    ) SELECT local.ts, local.peer, NEW.row_ts, NEW.row_peer, NEW.fk_id,
        NEW.on_delete, NEW.on_update, NEW.foreign_row_ts, NEW.foreign_row_peer,
        NEW.foreign_index, NEW.mark
    FROM _synq_local AS local;
END;

DROP VIEW IF EXISTS _synq_undolog_active;
CREATE VIEW         _synq_undolog_active AS
SELECT undo.* FROM _synq_undolog AS undo WHERE NOT EXISTS(
    SELECT 1 FROM _synq_undolog AS undo2
    WHERE undo.obj_ts = undo2.obj_ts AND undo.obj_peer = undo2.obj_peer AND
        undo2.ul > undo.ul
);

DROP VIEW IF EXISTS _synq_undolog_active_redo;
CREATE VIEW         _synq_undolog_active_redo AS
SELECT * FROM _synq_undolog_active WHERE ul%2 = 0;

DROP VIEW IF EXISTS _synq_undolog_active_undo;
CREATE VIEW         _synq_undolog_active_undo AS
SELECT * FROM _synq_undolog_active WHERE ul%2 = 1;

DROP TRIGGER IF EXISTS  _synq_undolog_active_insert;
CREATE TRIGGER          _synq_undolog_active_insert
INSTEAD OF INSERT ON _synq_undolog_active WHEN (
    NEW.peer IS NULL AND NEW.ts IS NULL
)
BEGIN
    UPDATE _synq_local SET ts = ts + 1;

    INSERT INTO _synq_undolog(ts, peer, obj_ts, obj_peer, ul)
    SELECT local.ts, local.peer, NEW.obj_ts, NEW.obj_peer, 1 + ifnull((
            SELECT max(ul) FROM _synq_undolog
            WHERE obj_ts = NEW.obj_ts AND obj_peer = NEW.obj_peer
        ), 0) FROM _synq_local AS local;
END;

-- following triggers are recursive triggers

DROP TRIGGER IF EXISTS  _synq_fklog_on_delete_cascade_marking;
CREATE TRIGGER          _synq_fklog_on_delete_cascade_marking
AFTER UPDATE OF mark ON _synq_fklog WHEN (
    OLD.mark <> 0 AND NEW.mark = 0 AND OLD.on_delete = 0 -- CASCADE
)
BEGIN
    UPDATE _synq_fklog SET mark = 0
    WHERE foreign_row_ts = OLD.row_ts AND foreign_row_peer = OLD.row_peer;
END;

DROP TRIGGER IF EXISTS  _synq_fklog_on_delete_restrict_marking;
CREATE TRIGGER          _synq_fklog_on_delete_restrict_marking
AFTER UPDATE OF mark ON _synq_fklog WHEN (OLD.mark <> 1 AND NEW.mark = 1)
BEGIN
    UPDATE _synq_fklog SET mark = 1
    WHERE row_ts = OLD.foreign_row_ts AND row_peer = OLD.foreign_row_peer AND
        mark = OLD.mark;
END;
"""


def _synq_triggers_for(tbl: sql.Table, tables: sql.Symbols) -> str:
    # FIXME: We do not support schema where:
    # - tables that reuse the name rowid
    # - without rowid tables (that's fine)
    # - rowid is aliased with no-autoinc column
    # - referred columns by a foreign key that are themselves a foreign key
    # - table with at least one column used in distinct foreign keys
    tbl_name = tbl.name[0]
    tbl_unique_columns = [x.columns() for x in utils.uniqueness(tbl)]
    replicated_cols = utils.replicated_columns(tbl)
    triggers = ""
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
        UNIQUE(row_ts, row_peer)
    );

    DROP TRIGGER IF EXISTS "_synq_id_update_{tbl_name}_pk_";
    CREATE TRIGGER "_synq_id_update_{tbl_name}_pk_"
    AFTER UPDATE OF rowid ON "{tbl_name}"
    WHEN (SELECT NOT is_merging FROM _synq_local)
    BEGIN
        UPDATE "_synq_id_{tbl_name}" SET rowid = NEW.rowid
        WHERE rowid = OLD.rowid;
    END;

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
        INSERT INTO _synq_undolog_active(obj_ts, obj_peer)
        VALUES(OLD.row_ts, OLD.row_peer);
    END;
    """
    triggers = ""
    insertions = ""
    for i, col in enumerate(replicated_cols):
        uniqueness_ids = [
            i
            for i in range(len(tbl_unique_columns))
            if col.name in tbl_unique_columns[i]
        ]
        tbl_index = uniqueness_ids[0] if len(uniqueness_ids) > 0 else "NULL"
        insertions += f"""
        UPDATE _synq_local SET ts = ts + 1; -- triggers clock update

        INSERT INTO _synq_log(ts, peer, row_ts, row_peer, col, val, tbl_index)
        SELECT local.ts, local.peer, cur.row_ts, cur.row_peer, {i}, NEW."{col.name}", {tbl_index}
        FROM _synq_local AS local, (
            SELECT * FROM "_synq_id_{tbl_name}" WHERE rowid = NEW.rowid
        ) AS cur;
        """.rstrip()
    fk_insertions = ""
    for fk_id, fk in enumerate(tbl.foreign_keys()):
        foreign_tbl_name = fk.foreign_table.name[0]
        foreign_tbl = tables.get(foreign_tbl_name)
        assert foreign_tbl is not None
        referred_cols = list(
            fk.referred_columns
            if fk.referred_columns is not None
            else utils.primary_key(foreign_tbl).columns()
        )
        foreign_uniqueness = foreign_tbl.uniqueness()
        foreign_index = next(
            i
            for i in range(len(foreign_uniqueness))
            if list(foreign_uniqueness[i].columns()) == referred_cols
        )
        new_referred_match = " AND ".join(
            f'"{ref_col}" = NEW."{col}"'
            for ref_col, col in zip(referred_cols, fk.columns)
        )
        coma_fk_cols = ", ".join(f'"{col}"' for col in fk.columns)
        fk_insertion = f"""
            UPDATE _synq_local SET ts = ts + 1; -- triggers clock update

            -- handle case where at least one col is NULL
            INSERT INTO _synq_fklog(
                ts, peer, row_ts, row_peer, fk_id,
                on_delete, on_update,
                foreign_index
            )
            SELECT
                local.ts, local.peer, cur.row_ts, cur.row_peer, {fk_id},
                {FK_ACTION[fk.on_delete]}, {FK_ACTION[fk.on_update]},
                {foreign_index}
            FROM _synq_local AS local, (
                SELECT * FROM "_synq_id_{tbl_name}" WHERE rowid = NEW.rowid
            ) AS cur;

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
            WHERE _synq_fklog.ts = local.ts AND _synq_fklog.peer = local.peer;
        """.rstrip()
        fk_insertions += fk_insertion
        triggers += f"""
        DROP TRIGGER IF EXISTS "_synq_fk_update_{tbl_name}_fk{fk_id}";
        CREATE TRIGGER "_synq_fk_update_{tbl_name}_fk{fk_id}"
        AFTER UPDATE OF {coma_fk_cols} ON "{tbl_name}"
        WHEN (SELECT NOT is_merging FROM _synq_local)
        BEGIN
            {fk_insertion}
        END;
        """.rstrip()
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
        SELECT ts, peer, '{tbl_name}' FROM _synq_local;
        {insertions}
        {fk_insertions}
    END;
    """.rstrip()
    for i, col in enumerate(replicated_cols):
        uniqueness_ids = [
            i
            for i in range(len(tbl_unique_columns))
            if col.name in tbl_unique_columns[i]
        ]
        tbl_index = uniqueness_ids[0] if len(uniqueness_ids) != 0 else "NULL"
        triggers += f"""
    DROP TRIGGER IF EXISTS "_synq_log_update_{tbl_name}_{col.name}";
    CREATE TRIGGER "_synq_log_update_{tbl_name}_{col.name}"
    AFTER UPDATE OF "{col.name}" ON "{tbl_name}"
    WHEN (SELECT NOT is_merging FROM _synq_local)
    BEGIN
        UPDATE _synq_local SET ts = ts + 1; -- triggers clock update

        INSERT INTO _synq_log(ts, peer, row_ts, row_peer, col, val, tbl_index)
        SELECT local.ts, local.peer, cur.row_ts, cur.row_peer, {i}, NEW."{col.name}", {tbl_index}
        FROM _synq_local AS local, (
            SELECT * FROM "_synq_id_{tbl_name}"
            -- we match against new and old because rowid may be updated.
            WHERE rowid = NEW.rowid OR rowid = OLD.rowid
        ) AS cur;
    END;
    """.rstrip()
    return textwrap.dedent(table_synq_id) + textwrap.dedent(triggers)


def _synq_script_for(tables: sql.Symbols) -> str:
    return "".join(
        _synq_triggers_for(tbl, tables)
        for tbl in tables.values()
        if not utils.is_temp(tbl)
    ).strip()


def _get_schema(db: sqlite3.Connection) -> str:
    with closing(db.cursor()) as cursor:
        cursor.execute(_SELECT_AR_TABLE_SCHEMA)
        result = ";".join(r[0] for r in cursor) + ";"
    return result


def _allocate_id(
    db: sqlite3.Connection, /, *, id: int | None = None, ts: bool = True
) -> None:
    with closing(db.cursor()) as cursor:
        cursor.execute("DELETE FROM _synq_local")
        cursor.execute("INSERT INTO _synq_local DEFAULT VALUES;")
        if id is not None:
            cursor.execute(f"UPDATE _synq_local SET peer = {id};")
        if not ts:
            cursor.execute(f"DROP TRIGGER IF EXISTS  _synq_local_clock;")
        cursor.execute("INSERT INTO _synq_context SELECT peer, ts FROM _synq_local;")


def init(db: sqlite3.Connection, /, *, id: int | None = None, ts: bool = True) -> None:
    sql_ar_schema = _get_schema(db)
    tables = sql.symbols(parse_schema(sql_ar_schema))
    with closing(db.cursor()) as cursor:
        cursor.executescript(_CREATE_TABLES)
        cursor.executescript(_synq_script_for(tables))
    _allocate_id(db, id=id, ts=ts)


def clone_to(
    src: sqlite3.Connection,
    target: sqlite3.Connection,
    /,
    *,
    id: int | None = None,
    ts: bool = True,
) -> None:
    src.commit()
    src.backup(target)
    _allocate_id(target, id=id, ts=ts)


def pull_from(db: sqlite3.Connection, remote_db_path: pathlib.Path | str) -> None:
    sql_ar_schema = _get_schema(db)
    tables = sql.symbols(parse_schema(sql_ar_schema))
    merging = _create_pull(tables)
    result = f"""
        PRAGMA defer_foreign_keys = ON;
        PRAGMA recursive_triggers = ON;

        ATTACH DATABASE '{remote_db_path}' AS extern;

        UPDATE main._synq_local SET is_merging = 1;

        {_MERGE_PREPARATION}
        {merging}
        {_MERGE_END}

        UPDATE main._synq_local SET is_merging = 0;

        DETACH DATABASE extern;

        PRAGMA defer_foreign_keys = OFF;
        PRAGMA recursive_triggers = OFF;
        """
    result = textwrap.dedent(result)
    with closing(db.cursor()) as cursor:
        cursor.executescript(result)


_MERGE_PREPARATION = """
-- Update clock
UPDATE _synq_local SET ts = max(
    main._synq_local.ts,
    (SELECT max(ts) FROM extern._synq_context)
);

-- Add missing peers in the context with a ts of 0
INSERT OR IGNORE INTO main._synq_context
SELECT peer, 0 FROM extern._synq_context;

-- Add new id and log entries
INSERT INTO main._synq_id
SELECT ext_id.*
FROM extern._synq_id AS ext_id JOIN main._synq_context AS ctx
    ON ext_id.row_ts > ctx.ts AND ext_id.row_peer = ctx.peer;

INSERT INTO main._synq_log
SELECT log.*
FROM extern._synq_log AS log JOIN main._synq_context AS ctx
    ON log.ts > ctx.ts AND log.peer = ctx.peer;

INSERT INTO main._synq_fklog
SELECT log.*
FROM extern._synq_fklog AS log JOIN main._synq_context AS ctx
    ON log.ts > ctx.ts AND log.peer = ctx.peer;

INSERT INTO main._synq_undolog
SELECT log.*
FROM extern._synq_undolog AS log JOIN main._synq_context AS ctx
    ON log.ts > ctx.ts AND log.peer = ctx.peer;

-- Conflict resolution

-- A. ON DELETE NO ACTION / RESTRICT

-- A.1. mark active rows that reference deleted rows
-- we start with concurrent active rows and use a recursive trigger
-- to traverse the graph of references.
-- FIXME: we should use proper context merging (min?)
-- to avoid duplicas
UPDATE main._synq_fklog SET mark = 0
FROM main._synq_context AS ctx, extern._synq_context AS ectx,
    _synq_undolog_active_undo AS undo
WHERE (
    (undo.ts > ctx.ts AND undo.peer = ctx.peer) OR
    (undo.ts > ectx.ts AND undo.peer = ectx.peer)
) AND (
    undo.obj_ts = main._synq_fklog.foreign_row_ts AND
    undo.obj_peer = main._synq_fklog.foreign_row_peer
);

-- A.2. mark rows that are (directly/transitively) referenced by a
-- ON DELETE NO ACTION / RESTRICT ref
UPDATE main._synq_fklog SET mark = 1
WHERE mark = 0 AND (on_delete = 1 OR on_delete = 2);

-- A.3. redo rows referenced by marked rows if undone
INSERT INTO _synq_undolog_active(obj_ts, obj_peer)
SELECT undo.obj_ts, undo.obj_peer
FROM _synq_undolog_active_undo AS undo JOIN _synq_fklog AS fk ON
    undo.obj_ts = fk.foreign_row_ts AND
    undo.obj_peer = fk.foreign_row_peer
WHERE fk.mark = 1;

-- A.4. un-mark remaining rows
UPDATE main._synq_fklog SET mark = NULL WHERE mark IS NOT NULL;

-- B. ON UPDATE

-- B.1. ON UPDATE NO ACTION / RESTRICT
INSERT INTO main._synq_undolog_active(obj_peer, obj_ts)
SELECT log.peer, log.ts
FROM main._synq_log_active AS log JOIN
    main._synq_fklog AS fk ON
        log.row_ts = fk.foreign_row_ts AND
        log.row_peer = fk.foreign_row_peer AND
        log.tbl_index = fk.foreign_index AND
        log.ts > fk.ts
WHERE fk.on_update = 1 OR fk.on_update = 2;

-- B.2.. ON UPDATE SET NULL
INSERT INTO main._synq_fklog_active(
    row_ts, row_peer, fk_id,
    on_delete, on_update, foreign_index
) SELECT
    fklog.row_ts, fklog.row_peer, fklog.fk_id,
    fklog.on_delete, fklog.on_update, fklog.foreign_index
FROM main._synq_log_active AS log JOIN main._synq_fklog AS fklog ON
    log.row_ts = fklog.foreign_row_ts AND
    log.row_peer = fklog.foreign_row_peer AND
    log.tbl_index = fklog.foreign_index AND
    log.ts > fklog.ts
WHERE fklog.on_update = 4;

-- C. resolve uniqueness conflicts

-- undo latest rows with conflicting unique keys
INSERT INTO main._synq_undolog_active(obj_ts, obj_peer)
SELECT DISTINCT log.row_ts, log.row_peer
FROM
    main._synq_log_active AS log
    JOIN main._synq_id AS id USING(row_ts, row_peer)
    JOIN (
        SELECT * FROM main._synq_log_active AS log
            JOIN _synq_id AS id USING(row_ts, row_peer)
    ) AS self USING(tbl, col, tbl_index, val)
WHERE log.row_ts > self.row_ts AND tbl_index IS NOT NULL
GROUP BY log.row_ts, log.row_peer, self.row_ts, self.row_peer
HAVING count(*) >= (
    SELECT count(*) FROM _synq_log_active
    WHERE row_ts = log.row_ts AND row_peer = log.row_peer AND tbl_index = log.tbl_index
);

-- D. ON DELETE CASCADE / SET NULL

-- D.1. mark active rows that reference deleted rows (same as A.1.)
-- FIXME: same as A.1.
UPDATE main._synq_fklog SET mark = 0
FROM main._synq_context AS ctx, extern._synq_context AS ectx,
    _synq_undolog_active_undo AS undo
WHERE ((
        main._synq_fklog.ts > ctx.ts AND
        main._synq_fklog.peer = ctx.peer
    ) OR (
        main._synq_fklog.ts > ectx.ts AND
        main._synq_fklog.peer = ectx.peer
)) AND (
    undo.obj_ts = main._synq_fklog.foreign_row_ts AND
    undo.obj_peer = main._synq_fklog.foreign_row_peer
);

-- D.2. ON DELETE CASCADE
INSERT INTO main._synq_undolog_active(obj_ts, obj_peer)
SELECT row_ts, row_peer
FROM main._synq_fklog
WHERE mark = 0 AND on_delete <> 4; -- except SET NULL

-- D.3. ON DELETE SET NULL
INSERT INTO main._synq_fklog_active(
    row_ts, row_peer, fk_id,
    on_delete, on_update, foreign_index
)
SELECT
    log.row_ts, log.row_peer, log.fk_id,
    log.on_delete, log.on_update, log.foreign_index
FROM main._synq_fklog AS log
WHERE mark = 0 AND on_delete = 4; -- only SET NULL

-- D.4. un-mark remaining rows
UPDATE _synq_fklog SET mark = NULL WHERE mark IS NOT NULL;

-- Prepare for building updated rows and new rows
-- todo...
"""

_MERGE_END = """
-- Update context
UPDATE main._synq_context SET ts = ctx.ts
FROM extern._synq_context AS ctx
WHERE ctx.ts > main._synq_context.ts AND main._synq_context.peer = ctx.peer;
"""


def _create_pull(tables: sql.Symbols) -> str:
    result = ""
    merger = ""
    for tbl in tables.values():
        tbl_name = tbl.name[0]
        repl_col_names = map(lambda col: col.name, utils.replicated_columns(tbl))
        selectors: list[str] = []
        col_names: list[str] = []
        for i, col_name in enumerate(repl_col_names):
            col_names += col_name
            selectors += f'''(
                SELECT log.val FROM main._synq_log_active AS log
                WHERE log.row_ts = id.row_ts AND log.row_peer = id.row_peer AND
                    log.col = {i}
            ) AS "{col_name}"'''
        for fk_id, fk in enumerate(tbl.foreign_keys()):
            f_tbl_name = fk.foreign_table.name[0]
            f_tbl = tables.get(f_tbl_name)
            assert f_tbl is not None
            f_repl_col_names = list(
                map(lambda col: col.name, utils.replicated_columns(tbl))
            )
            ref_col_names = (
                fk.referred_columns
                if fk.referred_columns is not None
                else utils.primary_key(f_tbl).columns()
            )
            if len(ref_col_names) == 1 and ref_col_names[0] not in f_repl_col_names:
                # auto-inc pk
                col_names += fk.columns[0]
                selectors += f'''(
                    SELECT rowid FROM main."_synq_id_{f_tbl_name}" AS rw
                        JOIN main._syn_fklog_active AS fklog
                            ON rw.row_ts = fklog.foreign_row_ts AND
                                rw.row_peer = fklog.foreign_row_peer
                    WHERE fklog.row_ts = id.row_ts AND
                        fklog.row_peer = id.row_peer AND
                        fklog.fk_id = {fk_id}
                ) AS "{fk.columns[0]}"'''
            else:
                ref_col_idx = [
                    f_repl_col_names.index(col_name) for col_name in ref_col_names
                ]
                assert len(ref_col_names) == len(fk.columns)
                for j, col_name in zip(ref_col_idx, fk.columns):
                    col_names += col_name
                    selectors += f'''(
                        SELECT val FROM main._synq_log_active AS log
                        WHERE log.rowid IN (
                            SELECT log.rowid FROM main._synq_log_active AS log
                                JOIN main._synq_fklog_active AS fklog
                                    ON log.row_ts = fklog.foreign_row_ts AND
                                    log.row_peer = fklog.foreign_row_peer
                            WHERE fklog.row_ts = id.row_ts AND
                                fklog.row_peer = id.row_peer AND
                                fklog.fk_id = {fk_id} AND log.col = {j}
                        )
                    ) AS "{col_name}"'''
        selectors = ["id.rowid"] + selectors
        merger += f"""
        INSERT INTO main."{tbl_name}"
        SELECT {', '.join(selectors)} FROM (
            SELECT DISTINCT id.rowid, id.row_peer, id.row_ts
            FROM main."_synq_id_{tbl_name}" AS id
                JOIN main._synq_context AS ctx
                    ON id.row_ts > ctx.ts AND id.row_peer = ctx.peer
        ) AS id;
        """
        result += f"""
        -- Foreign keys must be disabled

        -- Apply deletion (existing rows)
        DELETE FROM main."{tbl_name}" WHERE rowid IN (
            SELECT id.rowid FROM main."_synq_id_{tbl_name}" AS id
                JOIN main._synq_undolog_active_undo AS undo
                    ON id.row_ts = undo.obj_ts AND id.row_peer = undo.obj_peer
        );

        DELETE FROM main."_synq_id_{tbl_name}" WHERE rowid IN (
            SELECT id.rowid FROM main."_synq_id_{tbl_name}" AS id
                JOIN main._synq_undolog_active_undo AS undo
                    ON id.row_ts = undo.obj_ts AND id.row_peer = undo.obj_peer
        );

        -- Delete updated rows and redone rows
        -- This avoids uniqueness conflicts and simplify the merge
        DELETE FROM main."{tbl_name}" WHERE rowid IN (
            SELECT id.rowid FROM main._synq_context AS ctx
                JOIN main._synq_log_active AS log
                    ON log.ts > ctx.ts AND log.peer = ctx.peer 
                JOIN main."_synq_id_{tbl_name}" AS id
                    ON id.row_ts = log.row_ts AND id.row_peer = log.row_peer
            UNION SELECT id.rowid FROM main._synq_context AS ctx
                JOIN main._synq_fklog_active AS fklog
                    ON fklog.ts > ctx.ts AND fklog.peer = ctx.peer
                JOIN main."_synq_id_{tbl_name}" AS id
                    ON id.row_ts = fklog.row_ts AND id.row_peer = fklog.row_peer
            UNION SELECT id.rowid FROM main._synq_context AS ctx
                JOIN main._synq_undolog_active_redo AS redo
                    ON redo.ts > ctx.ts AND redo.peer = ctx.peer
                JOIN main."_synq_id_{tbl_name}" AS id
                    ON id.row_ts = redo.obj_ts AND id.row_peer = redo.obj_peer
        );
 
        -- Auto-assign local rowids for new active rows
        INSERT INTO main."_synq_id_{tbl_name}"(row_peer, row_ts)
        SELECT id.row_peer, id.row_ts
        FROM main._synq_id AS id JOIN main._synq_context AS ctx
            ON id.row_ts > ctx.ts AND id.row_peer = ctx.peer
        WHERE id.tbl = '{tbl_name}' AND NOT EXISTS(
            SELECT 1 FROM main._synq_undolog_active_undo AS undo
            WHERE undo.obj_ts = id.row_ts AND undo.obj_peer = id.row_peer
        );

        -- Auto-assign local rowids for redone rows
        INSERT OR IGNORE INTO main."_synq_id_{tbl_name}"(row_ts, row_peer)
        SELECT id.row_ts, id.row_peer
        FROM main._synq_undolog_active_redo AS redo
            JOIN main._synq_context AS ctx
                ON redo.ts > ctx.ts AND redo.peer = ctx.peer
            JOIN main._synq_id AS id
                ON redo.obj_ts = id.row_ts AND redo.obj_peer = id.row_peer
                    AND id.tbl = '{tbl_name}';
        """
    result += merger
    return result
