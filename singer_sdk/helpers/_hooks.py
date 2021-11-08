"""Helper functions for hooks."""
from contextlib import contextmanager
from typing import Any, Iterator


@contextmanager
def prepare_and_cleanup_hooks(name: str, obj: Any) -> Iterator[None]:
    """Calls `prepare_*` and `cleanup_*` methods around execution."""
    prepare_method = getattr(obj, "prepare_{}".format(name))
    cleanup_method = getattr(obj, "cleanup_{}".format(name))

    error = None
    try:
        prepare_method()
        yield
    except Exception as e:
        error = e
        raise e
    finally:
        cleanup_method(error)
