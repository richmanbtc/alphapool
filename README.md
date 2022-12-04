install

```bash
pip install "git+https://github.com/richmanbtc/alphapool.git@v0.1.2#egg=alphapool"
```

test

```bash
docker-compose run test
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
