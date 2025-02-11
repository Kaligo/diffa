# Diffa CLI

## Overview
Diffa is a command-line interface (CLI) tool for comparing data between two database systems. It supports configuration through both environment variables and a configuration file.

## Installation (Meltano)

On the meltano side, we can install the diffa just like any other plugin.

```yaml
  - name: diffa
    namespace: diffa
    pip_url: git+https://github.com/Kaligo/diffa.git
    executable: diffa
      config:
        uri:
          source: ${DIFFA__SOURCE_URI}
          target: ${DIFFA__TARGET_URI}
          diffa_db: ${DIFFA__DIFFA_DB_URI}
    settings:
      - name: uri.source
        env: DIFFA__SOURCE_URI
      - name: uri.target
        env: DIFFA__TARGET_URI
      - name: uri.diffa_db
        env: DIFFA__DIFFA_DB_URI
```

## Configuration

Users can configure database connection strings in two ways:

1. **Environment Variables** (higher priority)
   - `DIFFA__SOURCE_URI`: Connection string for the source database.
   - `DIFFA__TARGET_URI`: Connection string for the target database.
   - `DIFFA__DIFFA_DB_URI`: Connection string for the Diffa database.

2. **Configuration File** (if environment variables are not set)
   - Run the following command to configure Diffa interactively:

     ```sh
     diffa configure
     ```

   - This will store the connection strings in `~/.diffa/config.json`.

## Commands

### `configure`

- Interactively configure database connections and save them to `~/.diffa/config.json`.
- Environment variables take precedence over configuration in the file. 

```sh
diffa configure
```

### `migrate`

- Run `diffa` database migrations.

```sh
diffa migrate
```

### `data-diff`

The `data-diff` command checks data differences between two database systems.

#### Example

To compare data between two databases, run:

```sh
diffa data-diff \
    --source-database loyalty_engine_staging \
    --source-schema public \
    --source-table users \
    --target-database rc-us_dev \
    --target-schema loyalty_engine \
    --target-table users \
    --lookback-window 1 \
    --execution-date 2025-02-02
```

#### Options
- `--source-database`: Name of the source database (default: Infered from the connection string).
- `--source-schema`: Schema of the source table (default: `public`).
- `--source-table`: **(Required)** Name of the source table.
- `--target-database`: Name of the target database (default: Infered from the connection string).
- `--target-schema`: Schema of the target table (default: `public`).
- `--target-table`: **(Required)** Name of the target table.
- `--lookback-window`: **(Required)** Lookback window in days.
- `--execution-date`: **(Required)** Execution date in `YYYY-MM-DD` format.
