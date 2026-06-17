"""
dq_framework._explain
~~~~~~~~~~~~~~~~~~~~~~~
Thread-safe suppression of ``DataFrame.explain()`` stdout noise.

DQX's ``apply_checks_by_metadata`` calls ``DataFrame.explain()`` internally, which
prints a verbose physical plan to stdout. The obvious fix —
``contextlib.redirect_stdout`` — mutates ``sys.stdout`` *globally* and is therefore
**not** thread-safe: under a ``ThreadPoolExecutor`` one thread's redirect silences
every other thread's output at the same time.

Instead we monkeypatch ``DataFrame.explain`` to a no-op, guarded by a module-level
lock so only one thread patches/restores at a time. The lock window covers only the
lazy plan build (microseconds) — the actual Spark execution happens outside it.

This module is internal (underscore-prefixed); it is not part of the public API.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager

from pyspark.sql import DataFrame

# Serialises the patch/restore of the class-level DataFrame.explain attribute.
_explain_lock = threading.Lock()


@contextmanager
def suppress_explain() -> Iterator[None]:
    """
    Temporarily replace ``DataFrame.explain`` with a no-op, restoring it on exit.

    Holds :data:`_explain_lock` for the duration so concurrent workers never race
    on the shared class attribute. ``explain`` is always restored, even on error.

    Example
    -------
    >>> with suppress_explain():
    ...     result_df = dq_engine.apply_checks_by_metadata(input_df, rules)
    """
    with _explain_lock:
        original_explain = DataFrame.explain
        DataFrame.explain = lambda *args, **kwargs: None  # type: ignore[method-assign]
        try:
            yield
        finally:
            DataFrame.explain = original_explain  # type: ignore[method-assign]


__all__ = ["suppress_explain"]
