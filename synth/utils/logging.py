import os
import logging
from logging.handlers import RotatingFileHandler

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


def setup_wandb_alert(wandb_run):
    class WandBHandler(logging.Handler):
        def emit(self, record):
            try:
                log_entry = self.format(record)
                filter = "Perhaps it is too soon to set weights"
                if filter in log_entry:
                    print("Filtering out log entry:", log_entry)
                    return
                if record.levelno >= 40:
                    wandb_run.alert(
                        title="An error occurred",
                        text=log_entry,
                        level=record.levelname,
                    )
            except Exception:
                print(
                    "Error occurred while sending alert to wandb:", log_entry
                )

    wandb_handler = WandBHandler()
    wandb_handler.setLevel(logging.ERROR)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    wandb_handler.setFormatter(formatter)

    return wandb_handler
