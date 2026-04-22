# Parquet Toolkit

A desktop GUI tool for working with Parquet files stored in Azure Blob Storage. Two independent tools in one app:

- **Schema Transformer** — modify column schemas across an entire folder without rewriting your data pipeline
- **Data Collector** — filter rows by device or sender ID across thousands of files and consolidate them into a single output file

Built to solve Databricks Autoloader compatibility problems and to enable targeted data extraction from large partitioned datasets.

---

## Features

### Schema Transformer

- **Visual schema editor** — loads the Parquet schema and displays every column with its current Arrow type
- **Per-column transform dropdowns** — choose a transformation independently for each column; leave others unchanged
- **Auto-suggest** — automatically pre-selects the right transform based on the detected column type
- **Dry run mode** — simulates the full transformation and logs what would change, without uploading anything
- **Folder-level batch processing** — applies the same transform to every `.parquet` file under the specified prefix
- **In-place or new prefix** — overwrite source files or write to a different prefix
- **Autoscaling worker pool** — adaptive concurrency that scales up/down based on upload throughput and error rate
- **Schema-aware skip** — files already in the target schema are detected early and skipped
- **Automatic retries** — failures are reprocessed in dedicated retry batches (up to 5 total attempts)
- **Cancellable** — stop processing after the current file finishes
- **Checkpoint & resume** — interrupted runs can be resumed from where they left off
- **Extensible** — add a new transform by writing one decorated Python function; it appears in all dropdowns automatically

### Data Collector

- **Filter by SenderUid or DeviceUid** — enter one or more IDs (one per line); all matching rows across all files are collected
- **Parallel producer-consumer architecture** — N download threads feed a single writer thread; configurable worker count
- **RAM-bounded streaming** — filtered data is written incrementally to a local temp file via `ParquetWriter`; configurable RAM limit
- **Autoscaling** — `CollectorScaler` adjusts worker count based on RAM pressure and download error rate
- **Recalculated metadata** — output file footer contains accurate `recordCount`, `dateFrom`, `dateTo`, `deviceIds`, `batchNumber`
- **Separate output container** — optionally write the result to a different Azure container
- **Auto-generated filename** — output blob is named after the filter column and IDs (spaces replaced with underscores), e.g. `collected/SenderUid_100413_156412_1.0.parquet`
- **Cancellable** — stop at any time; no partial file is uploaded

---

## Prerequisites

- Python 3.10 or later
- An Azure Storage account with a connection string
- For Schema Transformer: all Parquet files in the target folder must share the same schema

---

## Installation

```bash
git clone <repo-url>
cd parquet-toolkit
pip install -r requirements.txt
```

**Dependencies** (`requirements.txt`):

| Package | Purpose |
|---|---|
| `pyarrow >= 14.0` | Reading, transforming, and writing Parquet files |
| `azure-storage-blob >= 12.19` | Connecting to Azure Blob Storage |
| `PyQt6 >= 6.5` | Desktop GUI |

---

## Running the Tool

```bash
python main.py
```

The application opens with two tabs: **Schema Transformer** and **Data Collector**.

---

## Schema Transformer

### Step 1 — Enter connection details

Fill in the **Azure Connection** section:

- **Connection String** — your Azure Storage connection string (input is masked by default)
- **Container** — the blob container name (e.g. `my-container`)
- **Folder Prefix** — the folder path inside the container (e.g. `raw/events/`). All `.parquet` files under this prefix are processed.

Click **Load Schema**.

### Step 2 — Review the schema

The **Schema** section shows every column with its current Arrow type and a transform dropdown:

| Column | Current Type | Transform |
|---|---|---|
| Id | fixed_size_binary[16] | -> String (UUID-Format) ▼ |
| TsCreate | timestamp[ns] | -> timestamp[ms, UTC] (Spark) ▼ |
| Name | string | - no change - ▼ |

Transforms are auto-suggested based on the detected type. Override any dropdown or leave as `- no change -` to skip that column.

### Step 3 — Choose output destination

- **In-place** — overwrite the source files
- **New prefix** — write to a different prefix, e.g. `transformed/events/`

### Step 4 — Set concurrency

Use the **Workers** spinner to pick how many parallel threads run. Each worker downloads, transforms, and uploads independently. Every file is attempted up to **5 times** automatically.

### Step 5 — Dry run (recommended first)

Click **Dry Run** to simulate without uploading anything.

### Step 6 — Apply

Click **Apply to All Files**. Progress is shown in the status bar. Each file is logged individually with worker ID, full blob path, and duration.

---

## Data Collector

### Step 1 — Enter connection details

Fill in the **Azure Connection** section:

- **Connection String** — your Azure Storage connection string
- **Container** — source blob container
- **Output container** — destination container (leave empty to write to the same container)

### Step 2 — Configure collection

Under **Collection Settings**:

- **Filter by** — choose `SenderUid` or `DeviceUid`
- **IDs (one per line)** — enter the UIDs to collect, for example:
  ```
  100413 156412 1.0
  100412 141978 1.0
  ```
  IDs can contain spaces — they are handled as strings throughout.
- **Source prefix** — folder to scan, e.g. `transformed/data/`
- **Output prefix** — destination folder, e.g. `collected/`

### Step 3 — Configure performance

- **Workers** — number of parallel download threads (1–64). Disabled when Autoscale is on.
- **Autoscale** — let `CollectorScaler` automatically adjust worker count based on RAM pressure and download error rate (recommended)
- **RAM limit (MB)** — maximum RAM the in-memory buffer may use before the writer flushes to the temp file (128–8192 MB, default 1024 MB)

### Step 4 — Collect

Click **Collect**. The log shows:

- How many blobs were found
- Per-file errors (processing continues after errors)
- Autoscaling events (worker count changes with reason)
- Final result: row count and output blob path

The output file is a single `.parquet` file named after the filter and IDs, e.g.:
```
collected/SenderUid_100413_156412_1.0_100412_141978_1.0.parquet
```

The file's Parquet footer contains recalculated metadata: `recordCount`, `dateFrom`, `dateTo`, `deviceIds`, `batchNumber`.

Click **Cancel** to stop at any time. No file is uploaded if cancelled.

---

## Built-in Transforms (Schema Transformer)

| Transform | Source Type | Target Type | Parquet Annotation | When to use |
|---|---|---|---|---|
| `-> String (UUID-Format)` | `fixed_size_binary[16]` | `string` | `BYTE_ARRAY (UTF8)` | Binary UUIDs that Spark should read as strings |
| `-> timestamp[ms, UTC] (Spark)` | `timestamp[ns]` | `timestamp[ms, UTC]` | `TIMESTAMP(isAdjustedToUTC=True, MILLIS)` | Timestamps that represent UTC instants |

Sub-millisecond precision is truncated (not rounded) when converting to `ms`.

---

## Adding Custom Transforms

Open `parquet_transform/transforms.py` and write a decorated function:

```python
@register(
    "my_transform",              # internal registry key
    "-> My Target Type",          # label shown in the UI dropdown
    applicable_types=["int32"]   # Arrow type strings for auto-suggest (or None to always show)
)
def my_transform(array: pa.Array, params: dict) -> pa.Array:
    """Convert int32 to string representation."""
    return array.cast(pa.string())
```

The new transform appears in all column dropdowns automatically on the next start.

---

## Project Structure

```
parquet-toolkit/
├── parquet_transform/
│   ├── collector.py    # Data-Collector logic: filter, metadata accumulation, Parquet rewrite
│   ├── scaler.py       # AdaptiveScaler (Schema Transformer) + CollectorScaler (Data Collector)
│   ├── transforms.py   # Transform registry + built-in transforms
│   ├── storage.py      # Azure Blob Storage client (list / download / upload)
│   └── processor.py    # Core transform logic: apply transforms to a PyArrow Table
├── gui/
│   ├── main_window.py      # Main window with Schema Transformer / Data Collector tabs
│   ├── collector_panel.py  # Data Collector tab widget
│   ├── schema_table.py     # Schema display widget with per-column ComboBoxes
│   └── workers.py          # Background QThread workers (TransformWorker, DataCollectorWorker)
├── tests/
│   ├── test_collector.py         # Unit tests for collector pure logic
│   ├── test_scaler_collector.py  # Unit tests for CollectorScaler
│   ├── test_collector_worker.py  # Integration tests for DataCollectorWorker
│   └── ...
├── main.py             # Entry point
└── requirements.txt
```

---

## Troubleshooting

**"No .parquet files found under prefix '...'"**
Check the folder prefix — Azure Blob uses it as a string match. Make sure it matches the actual path exactly, including any trailing slash (e.g. `raw/events/`).

**"Failed to load schema" — ResourceNotFoundError**
The container name or connection string is incorrect. Verify both in the Azure Portal under your storage account → Access keys.

**ArrowInvalid: Casting from timestamp[ns] to timestamp[ms] would lose data**
This should not occur — `safe=False` is used internally. If you see it in a custom transform, add `safe=False` to your `.cast()` call.

**Transformed files look correct in PyArrow but Spark still fails**
Inspect the Parquet footer with `pyarrow.parquet.read_schema(path).metadata`. Confirm the UTC timestamp transform was selected and the Spark schema matches the annotation.

**Data Collector: output file missing rows**
Check the log for `file_error` entries — download failures are skipped (not retried). If a large fraction of files errored, reduce the worker count manually or let Autoscale handle it.

**Data Collector: job hangs at 0% progress**
Verify the source prefix exists and the connection string has read access to the source container.

---

## Security

**Never commit your Azure connection string to Git.** All connection string fields in the UI are masked by default. Consider reading the string from an environment variable and pre-filling the field programmatically rather than typing it directly each time.

---

## License

MIT — see [LICENSE](LICENSE).
