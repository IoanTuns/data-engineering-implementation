"""Configuration constants for dataDP package."""

COLUMNS_TO_NOT_UPDATE = ["created_at", "valid_from"]
DEFAULT_BATCH_SIZE = 10000
MAX_RETRY_ATTEMPTS = 3
RETRY_WAIT_SECONDS = 5
ENABLE_SCHEMA_EVOLUTION = True
ALLOW_FIELD_ADDITION = True  # Allow adding new columns during schema evolution
ALLOW_FIELD_RELAXATION = False  # Allow changing column nullability during schema evolution
ALLOW_FIELD_TYPE_PROMOTION = True  # Allow promoting column data types during schema evolution
ENABLE_AUTO_OPTIMIZE = True
ENABLE_AUTO_COMPACT = True
DEFAULT_PARTITION_COLUMNS = ["year", "month", "day"]
DEFAULT_TIMESTAMP_COLUMNS = ["created_at", "updated_at"]
DEFAULT_IS_ACTIVE_COLUMN = "is_active"
DEFAULT_VALID_FROM_COLUMN = "valid_from"
DEFAULT_VALID_TO_COLUMN = "valid_to"
DEFAULT_AUDIT_COLUMNS = [
    DEFAULT_IS_ACTIVE_COLUMN,
    DEFAULT_VALID_FROM_COLUMN,
    DEFAULT_VALID_TO_COLUMN,
]
TECHNICAL_AUDIT_COLUMNS = DEFAULT_AUDIT_COLUMNS + DEFAULT_TIMESTAMP_COLUMNS
DEFAULT_STORAGE_FORMAT = "delta"  # Default storage format for tables
LOGGING_CONFIG = {
    "version": 1,
    "formatters": {
        "standard": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"},
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
            "level": "INFO",
        },
    },
    "loggers": {
        "dataDP": {"handlers": ["console"], "level": "INFO", "propagate": False},
    },
}
