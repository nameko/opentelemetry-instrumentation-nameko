# -*- coding: utf-8 -*-
import collections.abc
import json
import logging
import sys
from importlib import import_module

import six


TRUNCATE_MAX_LENGTH = 200

logger = logging.getLogger(__name__)


def serialise_to_json(value):
    """Safely serialise ``value`` to JSON formatted string"""
    return json.dumps(safe_for_serialisation(value))


def serialise_to_string(value):
    """Safely serialise ``value`` to string representation"""
    return str(safe_for_serialisation(value))


def safe_for_serialisation(value):
    no_op_types = six.string_types + six.integer_types + (float, type(None))
    if isinstance(value, no_op_types):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8", "ignore")
    if isinstance(value, dict):
        return {
            safe_for_serialisation(key): safe_for_serialisation(val)
            for key, val in six.iteritems(value)
        }
    if isinstance(value, collections.abc.Iterable):
        return type(value)(map(safe_for_serialisation, value))
    try:
        return six.text_type(value)
    except Exception:
        logger.warning("failed to get string representation", exc_info=True)
        return "failed to get string representation"


def import_by_path(dotted_path):
    """
    Import a dotted module path and return the attribute/class designated by
    the last name in the path. Raise ImportError if the import failed.

    Borrowed from ``django.utils.module_loading.import_string``.

    """
    try:
        module_path, class_name = dotted_path.rsplit(".", 1)
    except ValueError:
        raise ImportError("{} doesn't look like a module path".format(dotted_path))

    module = import_module(module_path)

    try:
        return getattr(module, class_name)
    except AttributeError:
        raise ImportError(
            'Module "{}" does not define a "{}" attribute/class'.format(
                module_path, class_name
            )
        )


def truncate(value, max_len=None):
    if max_len is None:
        max_len = TRUNCATE_MAX_LENGTH
    if len(value) > max_len:
        return value[:max_len], True
    return value, False


def call_function_get_frame(
    func, *args, **kwargs
):  # pragma: no cover -- cannot be captured by coverage
    """
    Calls the function `func` with the specified arguments and keyword
    arguments and snatches its local frame before it actually executes.

    Borrowed from https://stackoverflow.com/questions/4214936
    """

    frame = None
    trace = sys.gettrace()

    def snatch_locals(_frame, name, arg):
        nonlocal frame
        if frame is None and name == "call":
            frame = _frame
            sys.settrace(trace)
        return trace

    sys.settrace(snatch_locals)
    try:
        result = func(*args, **kwargs)
    finally:
        sys.settrace(trace)
    return frame, result
