import os
import logging
from logging.handlers import RotatingFileHandler
import bittensor as bt
from google.cloud.logging_v2.services.logging_service_v2 import (
    LoggingServiceV2Client,
)
from google.cloud.logging_v2.types import LogEntry, WriteLogEntriesRequest
from google.api.monitored_resource_pb2 import MonitoredResource

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
    wandb_handler = WandBHandler(wandb_run)
    wandb_handler.setLevel(logging.ERROR)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    wandb_handler.setFormatter(formatter)

    return wandb_handler


class BucketLogHandler(logging.Handler):
    def __init__(self, project_id, log_id="python-app-log"):
        super().__init__()
        self.client = LoggingServiceV2Client()
        self.log_name = f"projects/{project_id}/logs/{log_id}"
        self.resource = MonitoredResource(type="global", labels={})

    def emit(self, record):
        payload = self.format(record)
        entry = LogEntry(
            log_name=self.log_name,
            resource=self.resource,
            text_payload=payload,
        )
        req = WriteLogEntriesRequest(entries=[entry])
        self.client.write_log_entries(request=req)


def setup_gcp_logging(project_id, log_id="synth-validator"):
    handler = BucketLogHandler(project_id, log_id=log_id)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)

    return handler
