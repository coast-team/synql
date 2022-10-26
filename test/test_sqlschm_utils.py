from synqlite import sqlschm_utils as utils
from sqlschm import sql
from sqlschm.parser import parse_schema


def test_fk_col_resolution() -> None:
    schema = parse_schema(
        """
        CREATE TABLE X(x integer PRIMARY KEY);
        CREATE TABLE Y(y integer PRIMARY KEY, x REFERENCES X UNIQUE);
        CREATE TABLE Z(z integer PRIMARY KEY, y REFERENCES Y(x));
        CREATE TABLE U(a integer REFERENCES X, b integer, PRIMARY KEY(a,b));
        CREATE TABLE V(b integer PRIMARY KEY, a integer, FOREIGN KEY(a,b) REFERENCES U);
        """
    )
    symbols = sql.symbols(schema)
    X, Y, Z = symbols["X"], symbols["Y"], symbols["Z"]
    U, V = symbols["U"], symbols["V"]
    fkY, fkZ = next(iter(Y.foreign_keys())), next(iter(Z.foreign_keys()))
    fkU, fkV = next(iter(U.foreign_keys())), next(iter(V.foreign_keys()))

    assert utils.fk_col_resolution(fkY, "x", symbols) == utils.FkResolution(
        foreign_key=fkY, referred="x"
    )

    assert utils.fk_col_resolution(fkZ, "y", symbols) == utils.FkResolution(
        foreign_key=fkZ, referred=utils.FkResolution(foreign_key=fkY, referred="x")
    )

    assert utils.fk_col_resolution(fkV, "a", symbols) == utils.FkResolution(
        foreign_key=fkV,
        referred=utils.FkResolution(
            foreign_key=fkU,
            referred="x",
        ),
    )
