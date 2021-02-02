import argparse
import asyncio
import pathlib
import logging
from alertbase.db import Database


async def main():
    args = parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.INFO)
        logging.getLogger("botocore.retries.standard").setLevel(logging.DEBUG)
        logging.getLogger("botocore.retryhandler").setLevel(logging.DEBUG)
        logging.getLogger("boto3").setLevel(logging.DEBUG)
        logging.getLogger("aiobotocore").setLevel(logging.DEBUG)
    db = initialize_db(args)
    await db.upload_tarfile(args.tarfile, limit=args.limit)


def initialize_db(args: argparse.Namespace) -> Database:
    logging.info(f"initializing database: bucket={args.bucket}")
    logging.info(f"initializing database: indexdb={args.database}")
    logging.info(f"initializing database: create={args.create_db}")
    return Database(
        bucket=args.bucket,
        s3_region=args.s3_region,
        indexdb_path=args.database,
        create_if_missing=args.create_db,
    )


def parse_args() -> argparse.Namespace:
    argparser = argparse.ArgumentParser(
        description="Load an entire tar file into the database",
    )
    argparser.add_argument(
        "database",
        type=pathlib.Path,
        default="alerts.db",
        help="path to the directory of an index database",
    )
    argparser.add_argument(
        "tarfile",
        type=pathlib.Path,
        help="path to a gzipped tarfile to upload",
    )
    argparser.add_argument(
        "--bucket",
        type=str,
        default="ztf-alert-archive-prototyping",
        help="S3 bucket to store alerts in"
    )
    argparser.add_argument(
        "--skip-existing", type=bool, default=True,
        help="skip any alerts which are already in the database (based on candidate ID)",
    )
    argparser.add_argument(
        "--create-db", type=bool, default=False,
        help="create database if it does not exist",
    )
    argparser.add_argument(
        "--verbose", type=bool, default=True,
        help="be a little chatty with logs",
    )
    argparser.add_argument(
        "--upload-worker-count", type=int,
        help="use n concurrent worker tasks for uploads",
    )
    argparser.add_argument(
        "--limit", type=int,
        help="only upload the first N alerts",
    )
    argparser.add_argument(
        "--s3-region", type=str, default="us-west-2",
        help="s3 region to contact",
    )
    return argparser.parse_args()


if __name__ == "__main__":
    asyncio.run(main(), debug=True)
