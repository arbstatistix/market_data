"""
=========

Overview
--------
Shared logging utilities used across the codebase.

This module provides:
- A safe helper to force UTF-8 encoding for stdout/stderr without replacing file descriptors. [web:26]
- A structured `LogErrorType` enum for categorising logged errors. [web:24]
- A `LoggerConfig` dataclass for consistent logger configuration.
- A `LoggerBase` wrapper around `logging.Logger` with rotating file and optional stderr handlers. [web:18][web:19][web:20]
- A `ScopedLogger` context manager for scoped, prefixed logging blocks. [web:22][web:25][web:28]


Function: _force_utf8_stdio
---------------------------
`_force_utf8_stdio() -> None`
    Ensure that `sys.stdout` and `sys.stderr` use UTF-8 encoding while
    reusing the underlying file descriptors.

    Behaviour:
    - Skips work if streams are already wrapped (checks `_forced_utf8` attr).
    - For each of `stdout` and `stderr`, if the stream has a `buffer`,
      re-opens the file descriptor with `encoding="utf-8"` and `buffering=1`.
    - Marks the new stream with `_forced_utf8 = True` and replaces
      `sys.stdout` / `sys.stderr` in place.

    Notes:
    - Call this early in your application entrypoint if you need strict
      UTF-8 behaviour (e.g., when piping or in mixed locales). [web:26]


Enum: LogErrorType
------------------
`class LogErrorType(str, Enum)`
    Enumeration of high-level error categories to tag log entries with
    consistent error types.

Variants include:
- `FILE`           – file I/O failures.
- `MONGO`          – MongoDB-related errors.
- `REDIS`          – Redis-related errors.
- `MISSING_DATA`   – required data not found.
- `EMPTY_DATA`     – data present but empty.
- `KEY_INDEX`      – key/index lookup failures.
- `VALUE`          – invalid value provided.
- `TYPE`           – type mismatch or invalid type.
- `ATTR`           – missing or invalid attribute access.
- `CONNECTION`     – network/connection-level problems.
- `HTTP`           – HTTP protocol or response errors.
- `PERMISSION`     – permission/ACL-related errors.
- `OS`             – generic OS-level errors.
- `REGEX`          – regular expression compilation/match errors.
- `MISSING_DIR`    – expected directory does not exist.
- `MISSING_FILE`   – expected file does not exist.
- `CUSTOM`         – fallback for domain-specific or ad-hoc errors. [web:24]


Dataclass: LoggerConfig
-----------------------
`@dataclass(slots=True)`
`class LoggerConfig`
    Configuration container for `LoggerBase` instances.

Attributes:
    name (str):
        Logger name (typically a class or module name).
    log_dir (Path):
        Directory where log files will be written. Defaults to `Path.cwd()`.
    level (int):
        Logging level (e.g. `logging.INFO`).
    max_bytes (int):
        Maximum log file size before rotation (~5 MB by default). [web:18][web:20]
    backup_count (int):
        Number of rotated backup files to keep.
    to_stderr (bool):
        Whether to attach a stderr stream handler.
    propagate (bool):
        Whether messages should propagate to ancestor loggers. [web:26]
    fmt (str):
        Log message format string.
    datefmt (str | None):
        Optional date format string for timestamps.

Methods:
    log_file_path() -> Path
        Build a dated log file path in `log_dir` using the pattern
        `"{name}_{DDMMYY}.log"` based on the current local date.


Class: LoggerBase
-----------------
`class LoggerBase`
    Lightweight wrapper around `logging.Logger` that enforces a consistent
    configuration and provides structured error helpers. [web:19][web:29]

Constructor:
    `__init__(self, logger: Logger | None = None, config: LoggerConfig | None = None) -> None`
        - If `config` is `None`, creates a default `LoggerConfig` with the
          class name as `name`.
        - Uses a shared cache (`_LOGGERS`) to ensure only one logger instance
          per name within the process.
        - Creates and attaches handlers (stderr + rotating file) on first use.

Class attributes:
    `_LOGGERS: dict[str, Logger]`
        Process-wide cache of loggers keyed by name.

Key methods:

`_get_or_create_logger(config: LoggerConfig) -> Logger` (classmethod)
    - Returns an existing logger for `config.name` if present.
    - Otherwise, creates and configures a new logger:
      sets level, `propagate`, and attaches handlers if none are present. [web:26]

`_attach_handlers(logger: Logger, config: LoggerConfig) -> None` (staticmethod)
    - Builds a `logging.Formatter` using `fmt` and `datefmt`.
    - Optionally attaches a `StreamHandler` to `sys.stderr` if `to_stderr`
      is `True`.
    - Ensures the log directory exists, then attaches a `RotatingFileHandler`
      using `log_file_path()`, `max_bytes`, and `backup_count`. [web:18][web:20]

Properties:
    `logger -> Logger`
        Underlying `logging.Logger` instance.
    `name -> str`
        Convenience access to `logger.name`.

Logging methods:
    `log(level: int, msg: str, *args: Any, **kwargs: Any) -> None`
        Thin wrapper around `Logger.log` with lazy string formatting
        semantics (deferred until the message is actually emitted). [web:26]
    `debug/info/warning/error/critical(...)`
        Delegates directly to the corresponding methods on the underlying
        logger.

Exception helpers:
    `log_exception(error_type: LogErrorType | str, msg: str, *args: Any, exc: BaseException | None = None, level: int = logging.ERROR, **kwargs: Any) -> None`
        - Prefixes the message with `[ErrorType]`.
        - If `exc` is provided, logs with `exc_info=exc` to include a traceback.
        - Supports both enum values and raw string error types.

Typed convenience wrappers:
    `file_error/mongo_error/redis_error/...`
        - One method per `LogErrorType` variant.
        - Each forwards to `log_exception` with the appropriate error type.

Scoped logging:
    `scoped(scope_name: str) -> ScopedLogger`
        - Returns a `ScopedLogger` context manager that automatically prefixes
          messages with `[scope_name]`.
        - On exit, logs any unhandled exception in the scope using the
          `CUSTOM` error type. [web:22][web:25][web:28]


Class: ScopedLogger
-------------------
`class ScopedLogger`
    Context manager providing a lightweight, prefixed view on top of a
    `LoggerBase` instance.

Constructor:
    `__init__(self, base: LoggerBase, scope: str) -> None`
        - Stores the base logger and a scope name.

Context manager protocol:
    `__enter__(self) -> ScopedLogger`
        - Returns `self` for use within a `with` block.
    `__exit__(self, exc_type, exc, tb) -> None`
        - If `exc` is not `None`, calls `base.log_exception` with
          `LogErrorType.CUSTOM` and a scope-specific message.

Logging methods:
    `debug/info/warning/error/critical(...)`
        - Prefixes the message with `"[{scope}] "` and delegates to
          the corresponding method on `LoggerBase`.

Example:
    .. code-block:: python

        from logger import LoggerBase

        log_base = LoggerBase()

        with log_base.scoped("order_book") as log:
            log.info("Loaded snapshot id=%s", snapshot_id)
            # On exception, a CUSTOM error with scope metadata is logged.
"""


from __future__ import annotations

import datetime as _dt
import logging
import os
import sys
from dataclasses import dataclass, field
from enum import Enum
from logging import Logger
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Final, Iterable, Optional


# ---- UTF-8 stdio (safer pattern) -------------------------------------------

def _force_utf8_stdio() -> None:
    """
    Ensure UTF-8 for stdout/stderr in a way that reuses underlying FDs.

    Call this early in your main entrypoint if you really need to enforce it.
    """
    # Avoid double-wrapping
    if getattr(sys.stdout, "_forced_utf8", False):
        return

    for name in ("stdout", "stderr"):
        stream = getattr(sys, name)
        # Skip if not a text stream with a buffer
        if not hasattr(stream, "buffer"):
            continue
        fd = stream.buffer.fileno()
        new_stream = open(
            fd,
            mode="w",
            encoding="utf-8",
            buffering=1,
            closefd=False,
        )
        setattr(new_stream, "_forced_utf8", True)
        setattr(sys, name, new_stream)


# Optional: call it here or from your app's entrypoint
# _force_utf8_stdio()


# ---- Error type enum -------------------------------------------------------

class LogErrorType(str, Enum):
    FILE = "FileError"
    MONGO = "MongoError"
    REDIS = "RedisError"
    MISSING_DATA = "MissingDataError"
    EMPTY_DATA = "EmptyDataError"
    KEY_INDEX = "KeyIndexError"
    VALUE = "ValueError"
    TYPE = "TypeError"
    ATTR = "AttributeError"
    CONNECTION = "ConnectionError"
    HTTP = "HttpError"
    PERMISSION = "PermissionError"
    OS = "OSError"
    REGEX = "RegexError"
    MISSING_DIR = "MissingDirectoryError"
    MISSING_FILE = "MissingFileError"
    CUSTOM = "CustomError"


# ---- Config dataclass ------------------------------------------------------

@dataclass(slots=True)
class LoggerConfig:
    name: str
    log_dir: Path = field(default_factory=lambda: Path.cwd())
    level: int = logging.INFO
    max_bytes: int = 5_242_880  # ~5 MB
    backup_count: int = 3
    to_stderr: bool = True
    propagate: bool = False
    fmt: str = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    datefmt: Optional[str] = None

    def log_file_path(self) -> Path:
        date = _dt.datetime.now().strftime("%d%m%y")
        filename = f"{self.name}_{date}.log"
        return self.log_dir / filename


# ---- Core logger helper ----------------------------------------------------

class LoggerBase:
    """
    Lightweight wrapper around logging.Logger with:

    - Per-class or per-name loggers
    - Rotating file handler
    - Optional stderr stream handler
    - Structured error-type helpers
    """

    _LOGGERS: Final[dict[str, Logger]] = {}

    def __init__(
        self,
        logger: Optional[Logger] = None,
        config: Optional[LoggerConfig] = None,
    ) -> None:
        cls_name = self.__class__.__name__
        if config is None:
            config = LoggerConfig(name=cls_name)

        self._config = config
        self._logger = logger or self._get_or_create_logger(config)

    # ---- Logger creation / caching ----------------------------------------

    @classmethod
    def _get_or_create_logger(cls, config: LoggerConfig) -> Logger:
        """
        Ensure there is only one logger instance per name in this process.
        This avoids unbounded handler accumulation and saves memory.
        """
        if config.name in cls._LOGGERS:
            return cls._LOGGERS[config.name]

        logger = logging.getLogger(config.name)
        logger.setLevel(config.level)
        logger.propagate = config.propagate

        if not logger.handlers:
            cls._attach_handlers(logger, config)

        cls._LOGGERS[config.name] = logger
        return logger

    @staticmethod
    def _attach_handlers(logger: Logger, config: LoggerConfig) -> None:
        formatter = logging.Formatter(config.fmt, datefmt=config.datefmt)

        if config.to_stderr:
            stream_handler = logging.StreamHandler(sys.stderr)
            stream_handler.setFormatter(formatter)
            stream_handler.setLevel(config.level)
            logger.addHandler(stream_handler)

        log_file = config.log_file_path()
        log_file.parent.mkdir(parents=True, exist_ok=True)

        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=config.max_bytes,
            backupCount=config.backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(config.level)
        logger.addHandler(file_handler)

    # ---- Properties --------------------------------------------------------

    @property
    def logger(self) -> Logger:
        return self._logger

    @property
    def name(self) -> str:
        return self._logger.name

    # ---- Generic logging with lazy formatting ------------------------------

    def log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None:
        """
        Thin wrapper over Logger.log with lazy formatting.
        Use: log(logging.INFO, "val=%s", expensive_repr)
        """
        self._logger.log(level, msg, *args, **kwargs)

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.debug(msg, *args, **kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.info(msg, *args, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.error(msg, *args, **kwargs)

    def critical(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.critical(msg, *args, **kwargs)

    # ---- Exception helpers -------------------------------------------------

    def log_exception(
        self,
        error_type: LogErrorType | str,
        msg: str,
        *args: Any,
        exc: BaseException | None = None,
        level: int = logging.ERROR,
        **kwargs: Any,
    ) -> None:
        """
        Log an error with a structured error type and optional exception info.

        Pass exc to include a traceback.
        """
        etype = error_type.value if isinstance(error_type, LogErrorType) else str(error_type)
        prefix = f"[{etype}] {msg}"
        if exc is not None:
            self._logger.log(level, prefix, *args, exc_info=exc, **kwargs)
        else:
            self._logger.log(level, prefix, *args, **kwargs)

    # Convenience typed helpers

    def file_error(self, msg: str, *args: Any, exc: BaseException | None = None) -> None:
        self.log_exception(LogErrorType.FILE, msg, *args, exc=exc)

    def mongo_error(self, msg: str, *args: Any, exc: BaseException | None = None) -> None:
        self.log_exception(LogErrorType.MONGO, msg, *args, exc=exc)

    def redis_error(self, msg: str, *args: Any, exc: BaseException | None = None) -> None:
        self.log_exception(LogErrorType.REDIS, msg, *args, exc=exc)

    def missing_data_error(self, msg: str, *args: Any, exc: BaseException | None = None) -> None:
        self.log_exception(LogErrorType.MISSING_DATA, msg, *args, exc=exc)

    def empty_data_error(self, msg: str, *args: Any, exc: BaseException | None = None) -> None:
        self.log_exception(LogErrorType.EMPTY_DATA, msg, *args, exc=exc)

    def key_index_error(self, msg: str, *args: Any, exc: BaseException | None = None) -> None:
        self.log_exception(LogErrorType.KEY_INDEX, msg, *args, exc=exc)

    def value_error(self, msg: str, *args: Any, exc: BaseException | None = None) -> None:
        self.log_exception(LogErrorType.VALUE, msg, *args, exc=exc)

    def type_error(self, msg: str, *args: Any, exc: BaseException | None = None) -> None:
        self.log_exception(LogErrorType.TYPE, msg, *args, exc=exc)

    def attr_error(self, msg: str, *args: Any, exc: BaseException | None = None) -> None:
        self.log_exception(LogErrorType.ATTR, msg, *args, exc=exc)

    def connection_error(self, msg: str, *args: Any, exc: BaseException | None = None) -> None:
        self.log_exception(LogErrorType.CONNECTION, msg, *args, exc=exc)

    def http_error(self, msg: str, *args: Any, exc: BaseException | None = None) -> None:
        self.log_exception(LogErrorType.HTTP, msg, *args, exc=exc)



    def permission_error(self, msg: str, *args: Any, exc: BaseException | None = None) -> None:
        self.log_exception(LogErrorType.PERMISSION, msg, *args, exc=exc)

    def os_error(self, msg: str, *args: Any, exc: BaseException | None = None) -> None:
        self.log_exception(LogErrorType.OS, msg, *args, exc=exc)

    def regex_error(self, msg: str, *args: Any, exc: BaseException | None = None) -> None:
        self.log_exception(LogErrorType.REGEX, msg, *args, exc=exc)

    def missing_dir_error(self, msg: str, *args: Any, exc: BaseException | None = None) -> None:
        self.log_exception(LogErrorType.MISSING_DIR, msg, *args, exc=exc)

    def missing_file_error(self, msg: str, *args: Any, exc: BaseException | None = None) -> None:
        self.log_exception(LogErrorType.MISSING_FILE, msg, *args, exc=exc)

    # ---- Context manager for scoped logging --------------------------------

    def scoped(self, scope_name: str) -> "ScopedLogger":
        """
        Create a scoped logger that automatically prefixes messages with a tag.

        Example:
            with logger.scoped("order_book") as log:
                log.info("Loaded snapshot id=%s", snapshot_id)
        """
        return ScopedLogger(self, scope_name)


class ScopedLogger:
    __slots__ = ("_base", "_scope")

    def __init__(self, base: LoggerBase, scope: str) -> None:
        self._base = base
        self._scope = scope

    def __enter__(self) -> "ScopedLogger":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc is not None:
            self._base.log_exception(
                LogErrorType.CUSTOM,
                f"Unhandled exception in scope {self._scope}",
                exc=exc,
            )

    def _prefix(self, msg: str) -> str:
        return f"[{self._scope}] {msg}"

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._base.debug(self._prefix(msg), *args, **kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._base.info(self._prefix(msg), *args, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._base.warning(self._prefix(msg), *args, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._base.error(self._prefix(msg), *args, **kwargs)

    def critical(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._base.critical(self._prefix(msg), *args, **kwargs)
