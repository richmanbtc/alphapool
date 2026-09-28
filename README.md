# Installation

```bash
pip install "git+https://github.com/richmanbtc/alphapool.git@v0.1.5#egg=alphapool"
```

# Usage

Pass an existing PostgreSQL DB-API connection to `Client(conn, tournament=None)`.
Install a PostgreSQL driver in the calling application; the development image uses
psycopg2 from requirements.txt.

The caller owns the connection and must commit successful writes, roll back failed
or cancelled transactions, and close the connection. Client does none of these.

Client validates and normalizes submitted data and reads positions into a pandas
DataFrame. It does not perform calculations or manage the database schema.

# Database migrations

Apply the SQL files in migrations/ in filename order before using Client. Client
initialization does not create or alter tables or indexes. Apply each migration
once; the SQL files are not safe to rerun, and Client does not track migration history.

The initial migration creates the positions table. If tournament is supplied,
Client uses the corresponding <tournament>_positions table, which must already exist.

# Tests

Run tests inside the devcontainer. Compose connects it to the repository's
PostgreSQL service; host database settings are not used. The image installs
requirements.txt during the build. Rebuild the devcontainer after dependency changes.

```bash
python3 -m unittest tests/test_client.py
```

Database tests create a temporary database, apply the existing SQL migrations,
clear the positions table before each test, and drop the database during class
cleanup. The database role needs permission to create and drop test databases.
CI runs the tests using the same development image.

To run only the unit tests, without a database connection:

```bash
python3 -m unittest tests.test_client.TestClientUnit
```
