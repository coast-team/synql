import sqlite3
from sqlschm.parser import parse_schema
from sqlschm import sql
from contextlib import closing
import logging
import textwrap
from synqlite import sqlschm_utils as utils

logging.basicConfig(level=logging.DEBUG)

_SELECT_AR_TABLE_SCHEMA = f"""
SELECT sql
FROM sqlite_schema
WHERE type = 'table' AND
    name NOT LIKE 'sqlite_%' AND name NOT LIKE '_synq_%';
"""


_CREATE_TABLES = f"""
-- Causal context
CREATE TABLE IF NOT EXISTS _synq_context(
    uuid integer NOT NULL,
    ts integer NOT NULL DEFAULT 0,
    PRIMARY KEY(uuid),
    CHECK (ts >= 0)
) STRICT;

-- use `UPDATE _synq_meta SET ts = 0` to refresh the hybrid logical clock

DROP TABLE IF EXISTS _synq_meta;

CREATE TABLE _synq_meta(
    rowid integer NOT NULL PRIMARY KEY DEFAULT 1,
    uuid integer NOT NULL DEFAULT (abs(random())),
    ts integer NOT NULL DEFAULT 0, -- hybrid logical clock
    is_merging integer NOT NULL DEFAULT 0,
    CHECK (ts >= 0),
    CONSTRAINT is_merging_bool CHECK (is_merging IN (0, 1)),
    CONSTRAINT singleton CHECK (rowid = 1)
) STRICT;

INSERT INTO _synq_meta DEFAULT VALUES;

CREATE TRIGGER _synq_meta
    AFTER UPDATE OF ts ON _synq_meta WHEN NEW.ts = 0
BEGIN
	UPDATE _synq_meta SET ts = max(
        OLD.ts + 1,
        CAST(
            ((julianday('now') - 2440587.5) * 86400.0 * 1000000.0) AS int
                -- unix epoch in nano-seconds
                -- https://www.sqlite.org/lang_datefunc.html#examples
        )
    );
    INSERT OR REPLACE INTO _synq_context SELECT uuid, ts FROM _synq_meta;
END;

CREATE TABLE IF NOT EXISTS _synq_log(
    log_author integer NOT NULL,
    log_ts integer NOT NULL,
    row_author integer NOT NULL,
    row_ts integer NOT NULL,
    tbl_name text NOT NULL,
    col_name text NOT NULL,
    val any,
    PRIMARY KEY(log_author, log_ts, col_name)
) STRICT;

CREATE TABLE IF NOT EXISTS _synq_undo(
    obj_author integer NOT NULL,
    obj_ts integer NOT NULL,
    ul_ts integer NOT NULL, -- update timestamp of ul
    ul integer NOT NULL DEFAULT 0, -- undo length,
    PRIMARY KEY(obj_author, obj_ts)
) STRICT;

CREATE TABLE IF NOT EXISTS _synq_foreign_keys(
    log_author integer NOT NULL,
    log_ts integer NOT NULL,
    row_author integer NOT NULL,
    row_ts integer NOT NULL,
    fk_id integer NOT NULL,
    foreign_row_author integer NOT NULL,
    foreign_row_ts integer NOT NULL,
    PRIMARY KEY(log_author, log_ts, fk_id)
) STRICT;

CREATE TABLE IF NOT EXISTS _synq_mapping(
    row_author integer NOT NULL,
    row_ts integer NOT NULL,
    tbl_name text NOT NULL,
    row_id integer NOT NULL,
    PRIMARY KEY(row_author, row_ts),
    UNIQUE (tbl_name, row_id)
) STRICT;
"""


"""
INSERT OR IGNORE INTO main._synq_log
    SELECT * FROM remote._synq_log ORDER BY log_ts DESC;
"""


def _synq_triggers_for(tbl: sql.Table, symbols: utils.Symbols) -> str:
    replicated_cols = utils.replicated_columns(tbl)
    tbl_name = tbl.name[0]
    # we use the same id for the row and the log entry: (uuid, ts)
    insertions = "".join(
        f"""
            INSERT INTO _synq_log(log_author, log_ts, row_author, row_ts, tbl_name, col_name, val)
                SELECT uuid, ts, cur.row_author, cur.row_ts, '{tbl_name}', '{col.name}', NEW."{col.name}"
                FROM _synq_meta, (
                    SELECT row_author, row_ts FROM _synq_mapping
                    WHERE tbl_name = '{tbl_name}' AND row_id = NEW.rowid
                ) AS cur;
        """
        for col in replicated_cols
    )
    fk_insertions_list: list[str] = []
    fk_update_triggers_list: list[str] = []
    for nth, fk in enumerate(utils.foreign_keys(tbl)):
        foreign_tbl_name = fk.foreign_table[0]
        foreign_tbl = symbols.get(foreign_tbl_name)
        if foreign_tbl is None:
            raise Exception(f"table 'foreign_tbl_name' does not exists")
        referred_cols = (
            fk.referred_columns
            if fk.referred_columns is not None
            else utils.primary_key(foreign_tbl).columns
        )
        foreign_matching = " AND ".join(
            f'NEW."{col}" = "{foreign_col}"'
            for col, foreign_col in zip(fk.columns, referred_cols)
        )
        fk_insertions_list += f"""
            INSERT INTO _synq_foreign_keys(log_author, log_ts, row_author, row_ts, fk_id, foreign_row_author, foreign_row_ts)
            SELECT uuid, ts, cur.row_author, cur.row_ts, {nth}, target.row_author, target.row_ts
            FROM _synq_meta, (
                SELECT row_author, row_ts FROM _synq_mapping
                WHERE tbl_name = '{tbl_name}' AND row_id = NEW.rowid
            ) AS cur, (
                SELECT row_author, row_ts
                FROM _synq_mapping, (
                    SELECT rowid FROM "{foreign_tbl_name}"
                    WHERE {foreign_matching}
                ) AS row
                WHERE tbl_name = '{foreign_tbl_name}' AND row_id = row.rowid
            ) AS target;
        """
        coma_fk_cols = ", ".join(f'"{col}"' for col in fk.columns)
        underscore_fk_cols = "_".join(fk.columns)
        fk_update_triggers_list += f"""
        DROP TRIGGER IF EXISTS "_synq_fk_update_{tbl_name}_{underscore_fk_cols}";

        CREATE TRIGGER "_synq_fk_update_{tbl_name}_{underscore_fk_cols}" AFTER
            UPDATE OF {coma_fk_cols} ON "{tbl_name}" WHEN (
                NOT (SELECT is_merging FROM _synq_meta)
            )
        BEGIN
            UPDATE _synq_meta SET ts = 0; -- triggers clock update

            INSERT INTO _synq_foreign_keys
            SELECT uuid, ts, cur.row_author, cur.row_ts, {nth}, target.row_author, target.row_ts
            FROM  _synq_meta, (
                SELECT row_author, row_ts FROM _synq_mapping
                WHERE tbl_name = '{tbl_name}' AND row_id = NEW.rowid
            ) AS cur, (
                SELECT row_author, row_ts
                FROM _synq_mapping, (
                    SELECT rowid FROM "{foreign_tbl_name}"
                    WHERE {foreign_matching}
                ) AS row
                WHERE tbl_name = '{foreign_tbl_name}' AND row_id = row.rowid
            ) AS target;
        END;
        """
    fk_insertions = "".join(fk_insertions_list)
    fk_update_triggers = "".join(fk_update_triggers_list)
    ins_trigger = f"""
    DROP TRIGGER IF EXISTS "_synq_log_insert_{tbl_name}";

    CREATE TRIGGER "_synq_log_insert_{tbl_name}" AFTER
        INSERT ON "{tbl_name}" WHEN (
            NOT (SELECT is_merging FROM _synq_meta)
        )
    BEGIN
        UPDATE _synq_meta SET ts = 0; -- triggers clock update

        -- Create a new uuid for the row or reuse the uuid of the replaced
        -- row (in case of INSERT OR REPLACE without delete trigger firing)
        -- See https://sqlite.org/lang_conflict.html
        INSERT OR REPLACE INTO _synq_mapping(row_author, row_ts, tbl_name, row_id)
            SELECT uuid, ifnull((
                    SELECT row_ts FROM _synq_mapping
                    WHERE tbl_name = '{tbl_name}' AND row_id = NEW.rowid
                ), ts), '{tbl_name}', NEW.rowid FROM _synq_meta;
        {insertions}
        {fk_insertions}
    END;
    """
    del_trigger = f"""
    DROP TRIGGER IF EXISTS "_synq_log_delete_{tbl_name}";

    CREATE TRIGGER "_synq_log_delete_{tbl_name}" AFTER
        DELETE ON "{tbl_name}" WHEN (
            NOT (SELECT is_merging FROM _synq_meta)
        )
    BEGIN
        UPDATE _synq_meta SET ts = 0; -- triggers clock update

        INSERT OR REPLACE INTO _synq_undo(obj_author, obj_ts, ul_ts, ul)
        SELECT cur.row_author, cur.row_ts, meta.ts, 1 + ifnull((
                SELECT ul AS previous_ul FROM _synq_undo
                WHERE obj_author = cur.row_author AND obj_ts = cur.row_ts
            ), 0
        )
        FROM _synq_meta AS meta, (
            SELECT row_author, row_ts FROM _synq_mapping
            WHERE tbl_name = '{tbl_name}' AND row_id = OLD.rowid
        ) as cur;

        DELETE FROM _synq_mapping WHERE row_id = OLD.rowid;
    END;
    """
    # we assume that rowid cannot be updated
    update_triggers = "".join(
        f"""
        DROP TRIGGER IF EXISTS "_synq_log_update_{tbl_name}_{col.name}";

        CREATE TRIGGER "_synq_log_update_{tbl_name}_{col.name}" AFTER
            UPDATE OF "{col.name}" ON "{tbl_name}" WHEN (
                OLD."{col.name}" <> NEW."{col.name}" AND
                NOT (SELECT is_merging FROM _synq_meta)
            )
        BEGIN
            UPDATE _synq_meta SET ts = 0; -- triggers clock update

            INSERT INTO _synq_log(log_author, log_ts, row_author, row_ts, tbl_name, col_name, val)
            SELECT meta.uuid, meta.ts, cur.row_author, cur.row_ts, '{tbl_name}', '{col.name}', NEW."{col.name}"
            FROM _synq_meta AS meta, (
                SELECT row_author, row_ts FROM _synq_mapping
                WHERE tbl_name = '{tbl_name}' AND row_id = NEW.rowid
            ) AS cur;
        END;
        """
        for col in replicated_cols
    )
    return (
        textwrap.dedent(del_trigger)
        + textwrap.dedent(ins_trigger)
        + textwrap.dedent(update_triggers)
        + textwrap.dedent(fk_update_triggers)
    )


def _synq_script_for(schema: sql.Schema) -> str:
    symbols = utils.symbols(schema)
    return "".join(_synq_triggers_for(tbl, symbols) for tbl in schema.tables)


def _get_schema(db: sqlite3.Connection) -> str:
    with closing(db.cursor()) as cursor:
        cursor.execute(_SELECT_AR_TABLE_SCHEMA)
        result = ";".join(r[0] for r in cursor) + ";"
    return result


class Crr:
    db: sqlite3.Connection
    schema: sql.Schema

    def __init__(self, db_path: str):
        self.db = sqlite3.connect(db_path, check_same_thread=False)
        sql_ar_schema = _get_schema(self.db)
        self.schema = parse_schema(sql_ar_schema)
        logging.debug(_synq_script_for(self.schema))
        # self.db.executescript(_CREATE_TABLES).close()
        # self.db.executescript(script_for(self.schema)).close()

    def __del__(self):
        self.db.close()


if __name__ == "__main__":
    Crr("./ref.sqlite3")
