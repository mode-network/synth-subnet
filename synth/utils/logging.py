import os
import logging
from logging.handlers import RotatingFileHandler
import bittensor as bt

import google.cloud.logging
import google.auth.exceptions
from slack_sdk import WebClient


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


def setup_gcp_logging(log_id_prefix):
    log_id = f"{log_id_prefix}-synth-validator"
    bt.logging.info(f"setting up GCP log forwarder with log_id: {log_id}")
    try:
        client = google.cloud.logging.Client()
    except google.auth.exceptions.GoogleAuthError as e:
        bt.logging.warning(
            f"Failed to set up GCP logging. GoogleAuthError: {e}",
            "log forwarder",
        )
    else:
        if log_id_prefix is None:
            bt.logging.warning(
                "log_id_prefix is None. GCP logging will not be set up."
            )
        else:
            client.setup_logging(labels={"log_id": log_id})


class SlackHandler(logging.Handler):
    def __init__(self, personal_slack_token, channel_id):
        super().__init__()
        self.client = WebClient(token=personal_slack_token)
        self.channel_id = channel_id

    def emit(self, record):
        try:
            log_entry = self.format(record)
            if record.levelno >= 40:
                self.client.chat_postMessage(
                    channel=self.channel_id, text=log_entry
                )
        except Exception as err:
            msg = f"Error occurred while sending alert to slack: ---{str(err)}--- the message: ---{log_entry}---"
            bt.logging.warning(msg)


def setup_slack_alert():
    """Miners and validators can use this to send alerts to slack."""
    personal_slack_token = os.getenv("SLACK_TOKEN")
    if not personal_slack_token:
        bt.logging.warning(
            "SLACK_TOKEN not set. Slack alerts will not be sent."
        )
        return None
    channel_id = os.getenv("SLACK_CHANNEL_ID")
    if not channel_id:
        bt.logging.warning(
            "SLACK_CHANNEL_ID not set. Slack alerts will not be sent."
        )
        return None
    slack_handler = SlackHandler(personal_slack_token, channel_id)
    slack_handler.setLevel(logging.ERROR)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    slack_handler.setFormatter(formatter)

    bt.logging._logger.addHandler(slack_handler)
