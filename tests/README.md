# Running tests

## Case #1: Add job data to database

```bash
$ python3 src/run.py -c config.ini
```

## Case #2: Export/Extract from database

```bash
$ python3 src/run.py -c extract.ini
```

## Case #3: Rename collectl files and generate a list of existents files

```bash
$ bash src/scripts/xtransforme_collectl.sh 2026 1 1 sdumont2nd
``` 