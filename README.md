install

```bash
pip install "git+https://github.com/richmanbtc/alphapool.git@v0.1.5#egg=alphapool"
```

test

Run the tests inside the devcontainer. Compose connects it to the repository's PostgreSQL service; host database settings are not used. The image installs requirements.txt during the build. Rebuild the devcontainer after dependency changes.

The tests create a temporary database, apply the existing SQL migrations, clear its positions table before each test, and drop the database during class cleanup. CI runs the tests using the same development image.

```bash
python3 -m unittest tests/test_client.py
```

responsibilities

- abstract database access
- data format (validation, normalization)

out of scope

- calculation

db migration

- migration is done when client is created
- If multiple migrations are executed at the same time, it may stop with a lock
- https://github.com/pudo/dataset/blob/be81e8f00006442c640737cfc993754c3566386d/dataset/table.py#L311
