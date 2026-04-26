# ISC License
#
# Copyright (c) 2023-2026, Elia Rizzetto, Silvio Peroni
#
# Permission to use, copy, modify, and/or distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
# INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
# LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
# OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
# PERFORMANCE OF THIS SOFTWARE.

import logging
from typing import Optional

logger = logging.getLogger('oc_validator')


def configure_logging(verbose: bool = False, log_file: Optional[str] = None) -> None:
    """
    Configure logging for the oc_validator package.

    :param verbose: If True, set level to INFO; otherwise WARNING.
    :type verbose: bool
    :param log_file: If provided, log to this file path instead of the terminal.
    :type log_file: Optional[str]
    :rtype: None
    """
    level = logging.INFO if verbose else logging.WARNING
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler = logging.FileHandler(log_file) if log_file else logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(level)
