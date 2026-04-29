"""
scheduler.py  –  Run ETL on a schedule (daily/hourly)
Usage:
    python scheduler.py --source local --interval daily --time 06:00
"""

import argparse
import logging
import time

import schedule

from etl_pipeline import ETLPipeline

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s  [%(levelname)-8s]  %(message)s",
)
logger = logging.getLogger("Scheduler")


def run_etl(source: str, config: str) -> None:
    try:
        pipeline = ETLPipeline(config_path=config)
        pipeline.run(source=source)
    except Exception as exc:
        logger.error(f"Pipeline failed: {exc}", exc_info=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Scheduler")
    parser.add_argument("--source",   default="local",               choices=["local","azure","databricks"])
    parser.add_argument("--config",   default="config/etl_config.yaml")
    parser.add_argument("--interval", default="daily",               choices=["hourly","daily","weekly"])
    parser.add_argument("--time",     default="06:00",               help="HH:MM for daily/weekly runs")
    args = parser.parse_args()

    job = lambda: run_etl(args.source, args.config)

    if args.interval == "hourly":
        schedule.every().hour.do(job)
        logger.info("Scheduled: every hour")
    elif args.interval == "weekly":
        schedule.every().monday.at(args.time).do(job)
        logger.info(f"Scheduled: every Monday at {args.time}")
    else:  # daily
        schedule.every().day.at(args.time).do(job)
        logger.info(f"Scheduled: every day at {args.time}")

    # Run immediately on start
    job()

    logger.info("Scheduler running … (Ctrl+C to stop)")
    while True:
        schedule.run_pending()
        time.sleep(60)
