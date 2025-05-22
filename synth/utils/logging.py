import os
import logging
from logging.handlers import RotatingFileHandler
import bittensor as bt
import datetime

# from google.cloud.logging_v2.services.logging_service_v2 import (
#     LoggingServiceV2Client,
# )
# from google.cloud.logging_v2.types import LogEntry, WriteLogEntriesRequest
# from google.api.monitored_resource_pb2 import MonitoredResource
from google.auth.transport.requests import AuthorizedSession
import google.auth

EVENTS_LEVEL_NUM = 38
DEFAULT_LOG_BACKUP_COUNT = 10


def setup_events_logger(full_path, events_retention_size):
    logging.addLevelName(EVENTS_LEVEL_NUM, "EVENT")

    logger = logging.getLogger("event")
    logger.setLevel(EVENTS_LEVEL_NUM)

    def event(self, message, *args, **kws):
        if self.isEnabledFor(EVENTS_LEVEL_NUM):
            self._log(EVENTS_LEVEL_NUM, message, args, **kws)

    logging.Logger.event = event

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = RotatingFileHandler(
        os.path.join(full_path, "events.log"),
        maxBytes=events_retention_size,
        backupCount=DEFAULT_LOG_BACKUP_COUNT,
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(EVENTS_LEVEL_NUM)
    logger.addHandler(file_handler)

    return logger


class WandBHandler(logging.Handler):
    def __init__(self, wandb_run):
        super().__init__()
        self.wandb_run = wandb_run

    def emit(self, record):
        try:
            log_entry = self.format(record)
            if record.levelno >= 40:
                self.wandb_run.alert(
                    title="An error occurred",
                    text=log_entry,
                    level=record.levelname,
                )
        except Exception as err:
            filter = "will be ignored. Please make sure that you are using an active run"
            msg = f"Error occurred while sending alert to wandb: ---{str(err)}--- the message: ---{log_entry}---"
            if filter not in str(err):
                bt.logging.trace(msg)
            else:
                bt.logging.warning(msg)


def setup_wandb_alert(wandb_run):
    """Miners can use this to send alerts to wandb."""
    wandb_handler = WandBHandler(wandb_run)
    wandb_handler.setLevel(logging.ERROR)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    wandb_handler.setFormatter(formatter)

    return wandb_handler


class BucketLogHandler(logging.Handler):
    def __init__(
        self,
        project_id: str,
        bucket_id: str,
        log_id: str,
        timeout: float = 30.0,
    ):
        super().__init__()
        self.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        )

        # 1) build creds (only needs logging.write)
        scopes = ["https://www.googleapis.com/auth/logging.write"]
        creds, _ = google.auth.default(scopes=scopes)

        # 2) figure out your project
        self.project = project_id

        # 3) prepare an AuthorizedSession
        self.session = AuthorizedSession(creds)
        self.timeout = timeout

        # 4) bake your bucket into the JSON resourceNames
        self.bucket_rn = (
            f"projects/{self.project}/locations/global/buckets/{bucket_id}"
        )

        # 5) your logical log name (under that bucket)
        self.log_name = f"projects/{self.project}/logs/{log_id}"

        # 6) the one and only HTTP endpoint
        self.url = "https://logging.googleapis.com/v2/entries:write"

    def emit(self, record):
        text = self.format(record)
        now = (
            datetime.datetime.utcnow()
            .replace(tzinfo=datetime.timezone.utc)
            .isoformat()
        )

        body = {
            "entries": [
                {
                    "logName": self.log_name,  # "projects/.../logs/...”
                    "resource": {"type": "global", "labels": {}},
                    "textPayload": text,
                    "timestamp": now,
                }
            ]
        }

        resp = self.session.post(self.url, json=body, timeout=self.timeout)
        try:
            resp.raise_for_status()
        except Exception as e:
            raise RuntimeError(
                f"GCP write failed [{resp.status_code}]: {resp.text}"
            ) from e


def setup_gcp_logging(project_id, log_id_prefix):
    """
    bucket_id:     your user-defined bucket
    log_id_prefix: e.g. "validator-testnet" → logName="validator-testnet-app"
    keyfile:       (optional) SA JSON keyfile
    project:       (optional) GCP project override
    """
    log_id = f"{log_id_prefix}-synth-validator"
    bucket_id = "synth-validators"
    handler = BucketLogHandler(project_id, bucket_id, log_id)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(handler)
