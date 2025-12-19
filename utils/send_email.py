"""Helper wrapper around existing mailer to provide a simple send_error_email
and an async decorator to capture screenshots, logs and send notifications.

This module reads `config.ini` (project root) for provider credentials and
respects send_emails toggle. It uses the existing `src.utils.mailer` where
possible to avoid duplicating SMTP logic.
"""
from __future__ import annotations
import asyncio
import configparser
import logging
import time
from pathlib import Path
from typing import Optional, Sequence

from . import mailer
from .error_handler import ValidationError

DEFAULT_CONFIG = Path("config.ini")
LOG_TAIL_DEFAULT = 200


def _read_ini(path: Optional[Path] = None) -> dict:
    p = Path(path or DEFAULT_CONFIG)
    cfg = {"send_emails": False}
    if not p.exists():
        return cfg
    parser = configparser.ConfigParser()
    parser.read(p)
    if "EMAIL" in parser:
        em = parser["EMAIL"]
        cfg.update({
            "provider": em.get("provider", "").strip(),
            "smtp_server": em.get("smtp_server", "").strip(),
            "smtp_port": em.get("smtp_port", "").strip(),
            "sender_email": em.get("sender_email", "").strip(),
            "sender_password": em.get("sender_password", "").strip(),
            "recipient_emails": [e.strip() for e in em.get("recipient_emails", "").split(",") if e.strip()],
            "cc_emails": [e.strip() for e in em.get("cc_emails", "").split(",") if e.strip()],
            "bcc_emails": [e.strip() for e in em.get("bcc_emails", "").split(",") if e.strip()],
            "send_emails": em.get("send_emails", "true").strip().lower() in ("1", "true", "yes"),
        })
    if "OPTIONS" in parser:
        opt = parser["OPTIONS"]
        cfg["log_tail_lines"] = int(opt.get("log_tail_lines", LOG_TAIL_DEFAULT))
    else:
        cfg["log_tail_lines"] = LOG_TAIL_DEFAULT
    return cfg


async def send_error_email(
    subject: str,
    body: str,
    *,
    screenshot_path: Optional[Path] = None,
    log_files: Optional[Sequence[Path]] = None,
    config_path: Optional[Path] = None,
    logger: Optional[logging.Logger] = None,
) -> None:
    """Send an error email using config.ini. Uses existing mailer.send_email_async.

    - Builds a minimal email_cfg expected by `src.utils.mailer`.
    - If send_emails is false in config.ini this is a no-op (but logs when logger provided).
    """
    cfg = _read_ini(config_path)
    if not cfg.get("send_emails"):
        if logger:
            logger.info("Email notifications are disabled in config.ini; skipping error email.")
        return

    # Build mailer-friendly dict
    email_cfg = {
        # keep keys similar to existing mailer structure
        "provider": cfg.get("provider"),
        "smtp_host": cfg.get("smtp_server"),
        "smtp_port": cfg.get("smtp_port"),
        "use_tls": True,
        "sender": {"email": cfg.get("sender_email"), "name": "RPA Notifier"},
        "auth": {"username": cfg.get("sender_email"), "password": cfg.get("sender_password")},
        "recipients": {
            "to": list(cfg.get("recipient_emails") or []),
            "cc": list(cfg.get("cc_emails") or []),
            "bcc": list(cfg.get("bcc_emails") or [])
        },
        # allow mailer to use defaults for retry/timeouts
    }

    # Create temp directory for email attachments if needed
    temp_attachments_dir = Path("data/outputs/email_attachments")
    temp_attachments_dir.mkdir(parents=True, exist_ok=True)
    
    attachments = []
    try:
        # Copy screenshot if exists
        if screenshot_path and Path(screenshot_path).exists():
            screenshot_copy = temp_attachments_dir / f"screenshot_{int(time.time())}_{Path(screenshot_path).name}"
            screenshot_copy.write_bytes(Path(screenshot_path).read_bytes())
            attachments.append(screenshot_copy)

        # Copy log files if exist
        if log_files:
            for log_path in log_files:
                if log_path and Path(log_path).exists():
                    log_copy = temp_attachments_dir / f"log_{int(time.time())}_{Path(log_path).name}"
                    log_copy.write_bytes(Path(log_path).read_bytes())
                    attachments.append(log_copy)

    except Exception as copy_err:
        if logger:
            logger.error(f"Failed to copy attachments: {copy_err}")

    # Use the existing mailer which expects async send
    try:
        await mailer.send_email_async(
            email_cfg,
            subject=subject,
            body=body,
            to=email_cfg["recipients"]["to"],
            cc=email_cfg["recipients"]["cc"],
            bcc=email_cfg["recipients"]["bcc"],
            attachments=attachments or None,
        )
    except Exception as e:
        if logger:
            logger.error(f"Failed to send error email: {e}")
    finally:
        # Clean up temporary copies after sending (or if sending fails)
        for temp_file in attachments:
            try:
                if temp_file.exists():
                    temp_file.unlink()
            except Exception as cleanup_err:
                if logger:
                    logger.debug(f"Failed to clean up temporary attachment {temp_file}: {cleanup_err}")


def _find_log_tail(logfile: Path, tail_lines: int = LOG_TAIL_DEFAULT) -> str:
    try:
        if not logfile.exists():
            return ""
        with logfile.open("rb") as fh:
            # read last ~10KB then splitlines
            fh.seek(0, 2)
            size = fh.tell()
            to_read = min(size, 100 * 1024)
            fh.seek(max(0, size - to_read))
            data = fh.read().decode(errors="replace").splitlines()
        return "\n".join(data[-tail_lines:])
    except Exception:
        return ""


def _extract_log_file_from_logger(logger: logging.Logger) -> Optional[Path]:
    # Look for a FileHandler and return its baseFilename
    if not logger:
        return None
    for h in getattr(logger, "handlers", []):
        try:
            base = getattr(h, "baseFilename", None)
            if base:
                return Path(base)
        except Exception:
            continue
    return None


def handle_process_errors(process_name: Optional[str] = None):
    """Decorator for async process functions to capture exceptions, screenshot and send email.

    The wrapped function is expected to accept a named parameter `logger` and `page` in args/kwargs.
    On exception it will:
      - try to save a screenshot to data/outputs/error_screenshots
      - include last log lines from the process logger
      - call send_error_email
      - re-raise the exception so callers can decide to stop or continue
    """
    def deco(func):
        if asyncio.iscoroutinefunction(func):
            async def wrapper(*args, **kwargs):
                ln = process_name or getattr(func, "__name__", "process")
                # try to retrieve page and logger from args/kwargs
                page = kwargs.get("page", None)
                logger = kwargs.get("logger", None)
                # also inspect positional args if not found
                if page is None or logger is None:
                    # try positional: common signature (page, df, ... , logger)
                    if len(args) >= 1 and page is None:
                        possible_page = args[0]
                        # crude check
                        if hasattr(possible_page, "screenshot"):
                            page = possible_page
                    if len(args) >= 4 and logger is None:
                        possible_logger = args[3]
                        if hasattr(possible_logger, "error"):
                            logger = possible_logger

                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    # Build error context
                    func_name = f"{func.__module__}.{func.__name__}"
                    err_msg = f"Unhandled exception in {ln} ({func_name}): {e}"
                    if logger:
                        logger.error(err_msg)
                    # screenshot (best-effort)
                    shot_path = None
                    try:
                        if page is not None:
                            dest = Path("data/outputs/error_screenshots")
                            dest.mkdir(parents=True, exist_ok=True)
                            shot_path = dest / f"{ln}_{func.__name__}_error.png"
                            # playwright page.screenshot is async
                            await page.screenshot(path=str(shot_path), full_page=True)
                            if logger:
                                logger.error(f"Saved error screenshot: {shot_path}")
                    except Exception as se:
                        if logger:
                            logger.error(f"Failed to capture screenshot for {ln}: {se}")

                    # extract log file tail
                    log_tail = ""
                    log_file = _extract_log_file_from_logger(logger) if logger else None
                    if log_file:
                        log_tail = _find_log_tail(log_file)

                    # prepare email
                    subj = f"Validation/Error in {ln} - {func.__name__}"
                    body = f"An unhandled exception occurred in process '{ln}'.\n\nException:\n{e}\n\nFunction: {func_name}\n\n"
                    if log_tail:
                        body += f"Recent log lines:\n{log_tail}\n\n"

                    # call async send (best-effort). If this is a ValidationError, it's
                    # likely already handled by the validation checker (which sends an email),
                    # so avoid duplicate notifications here.
                    try:
                        if not isinstance(e, ValidationError):
                            await send_error_email(subj, body, screenshot_path=shot_path, log_files=[log_file] if log_file else None, logger=logger)
                            if logger:
                                logger.error("Dispatched process-level error email.")
                        else:
                            if logger:
                                logger.debug("ValidationError raised; skipping duplicate process-level email (already sent by validator).")
                    except Exception as me:
                        if logger:
                            logger.error(f"Failed to dispatch process-level error email: {me}")

                    # re-raise so caller may stop only this process
                    raise

            return wrapper
        else:
            # sync functions: provide basic wrapper
            def wrapper(*args, **kwargs):
                ln = process_name or getattr(func, "__name__", "process")
                logger = kwargs.get("logger")
                try:
                    return func(*args, **kwargs)
                except Exception:
                    if logger:
                        logger.error(f"Unhandled exception in {ln} -> see logs/screenshot")
                    raise
            return wrapper
    return deco
