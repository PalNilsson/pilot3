#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

"""
Exceptions in pilot
"""

import traceback


class PilotException(Exception):
    """
    The basic exception class.
    The pilot error code can be defined here, where the pilot error code will
    be propageted to job server.
    """

    def __init__(self, *args, **kwargs):
        super(PilotException, self).__init__(args, kwargs)
        self._errorCode = 0
        self._message = "An unknown pilot exception occurred."
        self.args = args
        self.kwargs = kwargs
        self._error_string = None
        self._stack_trace = "%s" % traceback.format_exc()

    def __str__(self):
        try:
            self._error_string = "Error code: %s, message: %s" % (self._errorCode, self._message % self.kwargs)
        except Exception:
            # at least get the core message out if something happened
            self._error_string = "Error code: %s, message: %s" % (self._errorCode, self._message)

        if len(self.args) > 0:
            # If there is a non-kwarg parameter, assume it's the error
            # message or reason description and tack it on to the end
            # of the exception message
            # Convert all arguments into their string representations...
            args = ["%s" % arg for arg in self.args if arg]
            self._error_string = (self._error_string + "\nDetails: %s" % '\n'.join(args))
        self._error_string = self._error_string + "\nStacktrace: %s" % self._stack_trace
        return self._error_string.strip()

    def get_error_code(self):
        return self._errorCode


class NotImplemented(PilotException):
    """
    NotImplemented
    """
    def __init__(self, *args, **kwargs):
        super(NotImplemented, self).__init__(args, kwargs)
        self._message = "The class or function is not implemented."


class UnKnownException(PilotException):
    """
    UnKnownException
    """
    def __init__(self, *args, **kwargs):
        super(UnKnownException, self).__init__(args, kwargs)
        self._message = "An unknown pilot exception has occurred."


class NoLocalSpace(PilotException):
    """
    Not enough local space
    """
    def __init__(self, *args, **kwargs):
        super(NoLocalSpace, self).__init__(args, kwargs)
        self._errorCode = 1098
        self._message = "Not enough local space."


class StageInFailure(PilotException):
    """
    Failed to stage-in input files
    """
    def __init__(self, *args, **kwargs):
        super(StageInFailure, self).__init__(args, kwargs)
        self._errorCode = 1099
        self._message = "Failed to stage-in input files."


class StageOutFailure(PilotException):
    """
    Failed to stage-out output files
    """
    def __init__(self, *args, **kwargs):
        super(StageOutFailure, self).__init__(args, kwargs)
        self._errorCode = 1137
        self._message = "Failed to stage-out output files."


class SetupFailure(PilotException):
    """
    Failed to setup environment
    """
    def __init__(self, *args, **kwargs):
        super(SetupFailure, self).__init__(args, kwargs)
        self._errorCode = 1110
        self._message = "Failed to setup environment."


class RunPayloadFailure(PilotException):
    """
    Failed to execute payload.
    """
    def __init__(self, *args, **kwargs):
        super(RunPayloadFailure, self).__init__(args, kwargs)
        self._errorCode = 1111
        self._message = "Failed to execute payload."
