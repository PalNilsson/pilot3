#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Authors
# - Paul Nilsson, paul.nilsson@cern.ch, 2026

"""Regression tests for the transform download read time-out (PanDA ID 7439967099).

In that job, the HTTP connection to the transform server was established, but the socket read of
the response body timed out. ``urlopen()`` only wraps connection-phase failures in ``URLError``, so
the unwrapped ``TimeoutError`` raised by ``response.read()`` escaped ``download_file()``, bypassed
the three download trials and the backup base URLs in ``download_transform()``, and was caught by
``execute_payloads()``, which failed the job with the generic error 1310 (PAYLOADEXECUTIONEXCEPTION)
instead of retrying, or failing with 1149 (TRFDOWNLOADFAILURE).
"""

from __future__ import annotations

import http.client
import os
import shutil
import socket
import ssl
import tempfile
import unittest
import urllib.error
from unittest.mock import MagicMock, patch

from pilot.common.errorcodes import ErrorCodes
from pilot.util import https
from pilot.user.atlas import setup as atlas_setup

errors = ErrorCodes()
TRF_URL = "https://pandaserver.cern.ch:25443/trf/user/runGen-00-00-02"


def make_response(read_side_effect: BaseException | None = None, data: bytes = b"") -> MagicMock:
    """Build a fake urlopen() response usable as a context manager.

    Args:
        read_side_effect: exception raised by response.read(), or None.
        data: bytes returned by response.read() when no side effect is given.

    Returns:
        MagicMock: the fake response.
    """
    response = MagicMock()
    response.__enter__.return_value = response
    response.__exit__.return_value = False
    if read_side_effect is not None:
        response.read.side_effect = read_side_effect
    else:
        response.read.return_value = data
    return response


class TestDownloadFileReadFailures(unittest.TestCase):
    """download_file() must return an empty result, never raise, on any transport failure."""

    def _download(self, read_side_effect: BaseException | None = None,
                  urlopen_side_effect: BaseException | None = None) -> bytes | str:
        """Run download_file() with a patched urlopen().

        Args:
            read_side_effect: exception raised while reading the body.
            urlopen_side_effect: exception raised by urlopen() itself.

        Returns:
            The download_file() return value.
        """
        if urlopen_side_effect is not None:
            mock_urlopen = MagicMock(side_effect=urlopen_side_effect)
        else:
            mock_urlopen = MagicMock(return_value=make_response(read_side_effect))
        with patch.object(https.urllib.request, "urlopen", mock_urlopen):
            return https.download_file(TRF_URL)

    def test_read_timeout_is_caught(self):
        """The exact failure from PanDA ID 7439967099: TimeoutError from response.read()."""
        self.assertEqual(self._download(read_side_effect=TimeoutError("timed out")), "")

    def test_socket_timeout_is_caught(self):
        """socket.timeout (a distinct class on Python < 3.10) is caught."""
        self.assertEqual(self._download(read_side_effect=socket.timeout("timed out")), "")

    def test_connection_reset_is_caught(self):
        """A connection reset during the body read is caught."""
        self.assertEqual(self._download(read_side_effect=ConnectionResetError(104, "Connection reset by peer")), "")

    def test_incomplete_read_is_caught(self):
        """A short body (http.client.IncompleteRead) is caught."""
        self.assertEqual(self._download(read_side_effect=http.client.IncompleteRead(b"partial", 100)), "")

    def test_remote_disconnected_is_caught(self):
        """RemoteDisconnected is still caught (previously listed explicitly)."""
        self.assertEqual(self._download(urlopen_side_effect=http.client.RemoteDisconnected("closed")), "")

    def test_url_error_is_caught(self):
        """Connection-phase URLError is still caught (previous behaviour)."""
        self.assertEqual(self._download(urlopen_side_effect=urllib.error.URLError("timed out")), "")

    def test_ssl_error_is_caught(self):
        """ssl.SSLError is still caught (previous behaviour)."""
        self.assertEqual(self._download(urlopen_side_effect=ssl.SSLError("handshake failed")), "")

    def test_successful_download(self):
        """A successful read returns the body unchanged."""
        mock_urlopen = MagicMock(return_value=make_response(data=b"#!/bin/bash\n"))
        with patch.object(https.urllib.request, "urlopen", mock_urlopen):
            self.assertEqual(https.download_file(TRF_URL), b"#!/bin/bash\n")

    def test_programming_errors_are_not_swallowed(self):
        """Non-transport exceptions (e.g. a bug) still propagate."""
        with self.assertRaises(ValueError):
            self._download(read_side_effect=ValueError("bug"))


class TestDownloadTransformTimeout(unittest.TestCase):
    """download_transform()/get_analysis_trf() must retry and then fail with 1149, not 1310."""

    def setUp(self):
        """Create a work directory and make sure the harvester shortcut is not taken."""
        self.workdir = tempfile.mkdtemp()
        self._harvester = os.environ.pop("HARVESTER_WORKDIR", None)

    def tearDown(self):
        """Remove the work directory and restore the environment."""
        shutil.rmtree(self.workdir, ignore_errors=True)
        if self._harvester is not None:
            os.environ["HARVESTER_WORKDIR"] = self._harvester

    def test_transient_timeout_is_retried(self):
        """A read time-out on the first trial is followed by a successful second trial."""
        responses = [make_response(TimeoutError("timed out")), make_response(data=b"#!/bin/bash\n")]
        with patch.object(https.urllib.request, "urlopen", MagicMock(side_effect=responses)), \
                patch.object(atlas_setup, "sleep") as mock_sleep:
            status, diagnostics = atlas_setup.download_transform(TRF_URL, "runGen-00-00-02", self.workdir)

        self.assertTrue(status)
        self.assertEqual(diagnostics, "no data was downloaded from " + TRF_URL)  # left over from trial 1
        mock_sleep.assert_called_once_with(60)
        with open(os.path.join(self.workdir, "runGen-00-00-02"), "rb") as _file:
            self.assertEqual(_file.read(), b"#!/bin/bash\n")

    def test_persistent_timeout_exhausts_trials(self):
        """Persistent read time-outs use all three trials and return a failure instead of raising."""
        mock_urlopen = MagicMock(side_effect=lambda *args, **kwargs: make_response(TimeoutError("timed out")))
        with patch.object(https.urllib.request, "urlopen", mock_urlopen), \
                patch.object(atlas_setup, "sleep"):
            status, diagnostics = atlas_setup.download_transform(TRF_URL, "runGen-00-00-02", self.workdir)

        self.assertFalse(status)
        self.assertIn("no data was downloaded", diagnostics)
        self.assertEqual(mock_urlopen.call_count, 3)

    def test_get_analysis_trf_reports_trf_download_failure(self):
        """With all base URLs timing out, the job fails with TRFDOWNLOADFAILURE (1149), not 1310."""
        mock_urlopen = MagicMock(side_effect=lambda *args, **kwargs: make_response(TimeoutError("timed out")))
        with patch.object(https.urllib.request, "urlopen", mock_urlopen), \
                patch.object(atlas_setup, "sleep"):
            exit_code, diagnostics, trf_name = atlas_setup.get_analysis_trf(
                TRF_URL, self.workdir, ["pandaserver.cern.ch:25443"])

        self.assertEqual(exit_code, errors.TRFDOWNLOADFAILURE)
        self.assertEqual(exit_code, 1149)
        self.assertIn("no data was downloaded", diagnostics)
        self.assertEqual(trf_name, "")


if __name__ == "__main__":
    unittest.main()
