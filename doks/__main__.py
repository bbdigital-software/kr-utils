import boto3
import os
import shutil
from pathlib import Path
import fire
from tqdm import tqdm
import tarfile
import subprocess
from datetime import datetime
from dotenv import load_dotenv
import concurrent.futures
from functools import partial

ENV_FILE = 'doks_utils.env'

def get_current_time() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

def create_env_template():
    """
    Creates a template environment configuration file (`doks_utils.env`) with default settings.
    """
    template = """\
# S3 Configuration
AWS_PROFILE=default
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_REGION=us-east-1
LOCAL_DOWNLOAD_DIR=./download

# PostgreSQL Configuration
POSTGRES_DB=my-db-name
POSTGRES_USER=my-username
POSTGRES_PASSWORD=my-password
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
"""
    with open(ENV_FILE, 'w') as f:
        f.write(template)
    print(f"Template configuration created at {ENV_FILE}.")

def load_env():
    if not os.path.exists(ENV_FILE):
        create_env_template()
        raise FileNotFoundError(f"{ENV_FILE} not found. A template has been created. Please fill it out and retry.")
    load_dotenv(ENV_FILE)

def get_boto3_session():
    aws_profile = os.getenv('AWS_PROFILE')
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = os.getenv('AWS_REGION', 'us-east-1')

    if aws_access_key_id and aws_secret_access_key:
        return boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )
    elif aws_profile:
        return boto3.Session(profile_name=aws_profile, region_name=aws_region)
    else:
        raise ValueError("Provide AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY or AWS_PROFILE in your .env file.")

def download_object(bucket, bucket_local_dir, obj):
    """
    Downloads a single S3 object to the corresponding local path.
    """
    target = bucket_local_dir / obj.key
    target.parent.mkdir(parents=True, exist_ok=True)
    if not obj.key.endswith('/'):
        bucket.download_file(obj.key, str(target))

def download_s3_buckets(*bucket_names):
    """
    Downloads specified AWS S3 buckets concurrently using a thread pool
    and saves each bucket into a separate compressed `.tar.gz` file.
    """
    load_env()
    session = get_boto3_session()
    s3 = session.resource('s3')

    local_dir = os.getenv('LOCAL_DOWNLOAD_DIR', './download')

    if not bucket_names:
        raise ValueError("You must specify at least one bucket name as a parameter.")

    for bucket_name in bucket_names:
        bucket = s3.Bucket(bucket_name)
        objects = list(bucket.objects.all())
        bucket_local_dir = Path(local_dir) / bucket_name

        with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()-2 or 1) as executor:
            download_func = partial(download_object, bucket, bucket_local_dir)
            list(tqdm(executor.map(download_func, objects), total=len(objects), desc=f"Downloading from {bucket_name}"))
        
        print("Compressing downloaded files...")
        tar_filename = f"{bucket_name}_{get_current_time()}.tar.gz"
        with tarfile.open(tar_filename, "w:gz") as tar:
            tar.add(bucket_local_dir, arcname=bucket_name)

        shutil.rmtree(bucket_local_dir)
        print(f"Bucket '{bucket_name}' downloaded and compressed into {tar_filename}.")

def dump_database():
    """
    Creates a PostgreSQL database dump as specified in the environment file (`doks_utils.env`).
    """
    load_env()

    db_name = os.getenv('POSTGRES_DB')
    user = os.getenv('POSTGRES_USER')
    host = os.getenv('POSTGRES_HOST', 'localhost')
    port = os.getenv('POSTGRES_PORT', '5432')
    dump_file = f'db_dump_{get_current_time()}.sql'
    password = os.getenv('POSTGRES_PASSWORD')

    if not all([db_name, user, password]):
        raise ValueError("POSTGRES_DB, POSTGRES_USER, and POSTGRES_PASSWORD must be set in your .env file.")

    command = [
        'pg_dump',
        '-h', host,
        '-p', port,
        '-U', user,
        '-F', 'c',
        '-f', dump_file,
        db_name
    ]

    env = os.environ.copy()
    env['PGPASSWORD'] = password
    os.environ['PGPASSWORD'] = password

    subprocess.run(command, env=env, check=True)
    print(f"Database '{db_name}' dumped to {dump_file}.")


def dump_all(*bucket_names):
    """
    Performs both S3 bucket(s) download and PostgreSQL database dump sequentially.
    """
    print('Dumping specified S3 buckets...')
    download_s3_buckets(*bucket_names)
    print('\n===================')
    print('Dumping database...')
    dump_database()

if __name__ == '__main__':
    fire.Fire({
        'dump_bucket': download_s3_buckets,
        'dump_db': dump_database,
        'config': create_env_template,
        'dump_all': dump_all
    })
