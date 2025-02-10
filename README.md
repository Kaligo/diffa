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
    env:
      - DIFFA__SOURCE_URI:
      - DIFFA__TARGET_URI:
      - DIFFA__DIFFA_URI:
```

## Configuration

Users can configure database connection strings in two ways:

1. **Environment Variables** (higher priority)
   - `DIFFA__SOURCE_URI`: Connection string for the source database.
   - `DIFFA__TARGET_URI`: Connection string for the target database.
   - `DIFFA__DIFFA_URI`: Connection string for the Diffa database.

2. **Configuration File** (if environment variables are not set)
   - Run the following command to configure Diffa interactively:

     ```sh
     diffa configure
     ```

   - This will store the connection strings in `~/.diffa/config.json`.

## Usage

### Check Data Differences

To compare data between two databases, use the `data-diff` command:

```sh
diffa data-diff \
    --source-schema public \
    --source-table users \
    --target-schema loyalty_engine \
    --target-table users \
    --lookback-window 1 \
    --execution-date 2025-02-02
```

## Commands

### `configure`

- Interactively configure database connections and save them to `~/.diffa/config.json`.
- Environment variables take precedence over configuration in the file. 

```sh
diffa configure
```

### `data-diff`

The `data-diff` command checks data differences between two database systems.

#### Example

To compare data between two databases, run:

```sh
diffa data-diff \
    --source-schema public \
    --source-table users \
    --target-schema loyalty_engine \
    --target-table users \
    --lookback-window 1 \
    --execution-date 2025-02-02
```

#### Options

- `--source-schema`: Schema of the source table (default: `public`).
- `--source-table`: **(Required)** Name of the source table.
- `--target-schema`: Schema of the target table (default: `public`).
- `--target-table`: **(Required)** Name of the target table.
- `--lookback-window`: **(Required)** Lookback window in days.
- `--execution-date`: **(Required)** Execution date in `YYYY-MM-DD` format.
