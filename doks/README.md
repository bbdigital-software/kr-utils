# Utility Scripts for AWS S3 & PostgreSQL Backup

This utility provides scripts to download AWS S3 bucket contents concurrently and create PostgreSQL database backups, along with a simple configuration system using environment variables.

## Installation

### Prerequisites
Ensure Python (3.8+) is installed on your system.

### Clone and Install Dependencies

Clone this repository and install required Python packages:

```bash
git clone https://github.com/bbdigital-software/kr-utils.git
cd doks
pip install -r requirements.txt
```


**Note:**  
- Ensure `pg_dump` (PostgreSQL client utilities) is installed and accessible in your system path for database dumps.

## Configuration

A configuration file named `doks_utils.env` must be present. To generate a template, run in root path:

```bash
python doks config
```

Then fill out `doks_utils.env` with your AWS and PostgreSQL credentials.


## Usage
### Commands

**Download specific AWS S3 buckets:**

```bash
python doks dump_bucket bucket-name-1 bucket-name-2
```

**Dump PostgreSQL database:**

```bash
python doks dump_db
```

**Perform both S3 bucket download and PostgreSQL dump:**

```bash
python doks dump_all bucket-name-1 bucket-name-2
```

## Utilities Provided

| Command      | Description                                                      |
|--------------|------------------------------------------------------------------|
| `dump_bucket`| Downloads one or multiple AWS S3 buckets concurrently.           |
| `dump_db`    | Dumps the specified PostgreSQL database into a compressed file.  |
| `dump_all`   | Downloads S3 buckets and dumps the database sequentially.        |
| `config`     | Generates a template configuration file (`doks_utils.env`).      |

## Notes

- Downloads are saved into timestamped compressed archives (`.tar.gz`) in the current directory.
- Database dumps are saved in `.sql` format with timestamped filenames.

## Dependencies

- `boto3`: AWS SDK for Python (AWS S3 integration)
- `python-dotenv`: Load configuration from `.env` files
- `fire`: CLI tool to simplify command-line interfaces
- `tqdm`: Progress bars for visual feedback during downloads
- PostgreSQL's `pg_dump` utility (system-level dependency)

