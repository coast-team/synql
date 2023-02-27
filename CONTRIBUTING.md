# Contributing

## Getting started

The project uses [poetry](https://python-poetry.org) for managing dependencies.

First, install the project's dependencies:

```sh
poetry install
```

Once installed, you can execute the unit tests with [pytest](https://docs.pytest.org):

```sh
poetry run pytest
```

We use [python type annotations](https://docs.python.org/3/library/typing.html).
Type-check the code with [mypy](https://mypy-lang.org/):

```sh
poetry run mypy synql
```

Format the code with [black](https://github.com/psf/black):

```sh
poetry run black .
```

You can also lint the project thanks to [pylint](https://pylint.org/):

```sh
poetry run pylint synql
```

The project depends on _pysqlite3-binary_ in order to use the same version of sqlite3 in local and in CI.

## Commit messages

The project adheres to the [conventional commit specification](https://www.conventionalcommits.org/).

The following commit prefixes are supported:

- `feat:`, a new feature
- `fix:`, a bugfix
- `docs:`, a documentation update
- `test:`, a test update
- `chore:`, project housekeeping
- `perf:`, project performance
- `refactor:`, refactor of the code without change in functionality

See the _git log_ for well-formed messages.
