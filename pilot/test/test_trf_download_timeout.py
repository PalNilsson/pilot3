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

The same review found that ``download_transform()`` created the transform file before knowing
whether any data had arrived (an empty file was left behind after a failed download, and the
curl-based plugins left whatever the shell redirection produced), and that
``get_urlopen_output()`` had neither a socket timeout nor handling for unwrapped socket errors.
The curl-based plugins ran curl without ``-f``, so an HTTP error page was saved as the transform and
reported as a successful download, and ``upload_file()`` set its time-outs as attributes on the
request object, which ``urlopen()`` ignores.
"""

from __future__ import annotations

import http.client
import os
import shutil
import socket
import ssl
import tempfile
import threading
import unittest
import urllib.error
from http.server import BaseHTTPRequestHandler, HTTPServer
from io import BytesIO
from unittest.mock import MagicMock, patch

from pilot.common.errorcodes import ErrorCodes
from pilot.util import https
from pilot.user.atlas import setup as atlas_setup
from pilot.user.darkside import setup as darkside_setup
from pilot.user.epic import setup as epic_setup
from pilot.user.generic import setup as generic_setup
from pilot.user.rubin import setup as rubin_setup
from pilot.user.ska import setup as ska_setup
from pilot.user.sphenix import setup as sphenix_setup

errors = ErrorCodes()
TRF_URL = "https://pandaserver.cern.ch:25443/trf/user/runGen-00-00-02"
TRF_NAME = "runGen-00-00-02"
# VO plugins whose download_transform() uses download_file() / curl with a shell redirection
URLLIB_PLUGINS = (atlas_setup, generic_setup, ska_setup, darkside_setup)
CURL_PLUGINS = (epic_setup, rubin_setup, sphenix_setup)


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
        self.assertEqual(diagnostics, "")  # no stale diagnostics from the failed first trial
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

    def test_failed_download_leaves_no_file(self):
        """After a failed download no (empty) transform is left in the work directory, in any plugin."""
        for module in URLLIB_PLUGINS:
            with self.subTest(plugin=module.__name__):
                mock_urlopen = MagicMock(side_effect=lambda *args, **kwargs: make_response(TimeoutError("timed out")))
                with patch.object(https.urllib.request, "urlopen", mock_urlopen), patch.object(module, "sleep"):
                    status, _ = module.download_transform(TRF_URL, TRF_NAME, self.workdir)

                self.assertFalse(status)
                self.assertFalse(os.path.exists(os.path.join(self.workdir, TRF_NAME)))

    def test_retry_recovers_in_all_plugins(self):
        """A transient read time-out is retried and recovers, in every download_file()-based plugin."""
        for module in URLLIB_PLUGINS:
            with self.subTest(plugin=module.__name__):
                responses = [make_response(TimeoutError("timed out")), make_response(data=b"#!/bin/bash\n")]
                with patch.object(https.urllib.request, "urlopen", MagicMock(side_effect=responses)), \
                        patch.object(module, "sleep"):
                    status, diagnostics = module.download_transform(TRF_URL, TRF_NAME, self.workdir)

                self.assertTrue(status)
                self.assertEqual(diagnostics, "")
                os.remove(os.path.join(self.workdir, TRF_NAME))

    def test_write_failure_is_not_retried(self):
        """A local write failure returns at once with a clear diagnostic (downloading again will not help)."""
        mock_urlopen = MagicMock(return_value=make_response(data=b"#!/bin/bash\n"))
        with patch.object(https.urllib.request, "urlopen", mock_urlopen), \
                patch.object(atlas_setup, "sleep") as mock_sleep, \
                patch("builtins.open", side_effect=OSError(28, "No space left on device")):
            status, diagnostics = atlas_setup.download_transform(TRF_URL, TRF_NAME, self.workdir)

        self.assertFalse(status)
        self.assertIn("failed to write transform", diagnostics)
        self.assertIn("No space left on device", diagnostics)
        self.assertEqual(mock_urlopen.call_count, 1)
        mock_sleep.assert_not_called()

    def test_partial_write_is_removed(self):
        """A write that fails after the file was created leaves no truncated transform behind."""
        path = os.path.join(self.workdir, TRF_NAME)
        real_open = open

        def failing_open(file, *args, **kwargs):
            """Create the real file, then fail on write (like a full disk)."""
            handle = real_open(file, *args, **kwargs)  # pylint: disable=consider-using-with  # closed by save_transform()
            if file == path:
                handle.write(b"#!/bin/ba")
                handle.flush()
                handle.write = MagicMock(side_effect=OSError(28, "No space left on device"))
            return handle

        mock_urlopen = MagicMock(return_value=make_response(data=b"#!/bin/bash\n"))
        with patch.object(https.urllib.request, "urlopen", mock_urlopen), patch.object(atlas_setup, "sleep"), \
                patch("builtins.open", side_effect=failing_open):
            status, diagnostics = atlas_setup.download_transform(TRF_URL, TRF_NAME, self.workdir)

        self.assertFalse(status)
        self.assertIn("No space left on device", diagnostics)
        self.assertFalse(os.path.exists(path))


class TestCurlDownloadTransformCleanup(unittest.TestCase):
    """The curl-based plugins must not leave the redirection's output behind after a failed download."""

    def setUp(self):
        """Create a work directory and make sure the harvester shortcut is not taken."""
        self.workdir = tempfile.mkdtemp()
        self._harvester = os.environ.pop("HARVESTER_WORKDIR", None)

    def tearDown(self):
        """Remove the work directory and restore the environment."""
        shutil.rmtree(self.workdir, ignore_errors=True)
        if self._harvester is not None:
            os.environ["HARVESTER_WORKDIR"] = self._harvester

    def _fake_execute(self, exit_code: int):
        """Return a fake execute() that mimics 'curl ... > path' (the redirection always creates the file).

        Args:
            exit_code: curl exit code to return.

        Returns:
            Callable: the fake execute().
        """
        def fake_execute(*args, **kwargs):  # pylint: disable=unused-argument
            with open(os.path.join(self.workdir, TRF_NAME), "wb") as _file:
                _file.write(b"" if exit_code else b"#!/bin/bash\n")
            return exit_code, "", "" if not exit_code else "curl: (28) Operation timed out"
        return fake_execute

    def test_failed_curl_leaves_no_file(self):
        """After all curl trials fail, the file created by the redirection is removed."""
        for module in CURL_PLUGINS:
            with self.subTest(plugin=module.__name__):
                with patch.object(module, "execute", side_effect=self._fake_execute(28)), patch.object(module, "sleep"):
                    status, diagnostics = module.download_transform(TRF_URL, TRF_NAME, self.workdir)

                self.assertFalse(status)
                self.assertIn("curl command failed", diagnostics)
                self.assertFalse(os.path.exists(os.path.join(self.workdir, TRF_NAME)))

    def test_successful_curl_keeps_file(self):
        """A successful curl download keeps the file."""
        for module in CURL_PLUGINS:
            with self.subTest(plugin=module.__name__):
                with patch.object(module, "execute", side_effect=self._fake_execute(0)), patch.object(module, "sleep"):
                    status, _ = module.download_transform(TRF_URL, TRF_NAME, self.workdir)

                self.assertTrue(status)
                self.assertTrue(os.path.exists(os.path.join(self.workdir, TRF_NAME)))
                os.remove(os.path.join(self.workdir, TRF_NAME))


class TestGetUrlopenOutput(unittest.TestCase):
    """get_urlopen_output() must apply a timeout and never raise on a transport failure."""

    def _open(self, side_effect: BaseException | None = None, return_value: MagicMock | None = None, **kwargs):
        """Call get_urlopen_output() with a patched urlopen().

        Args:
            side_effect: exception raised by urlopen(), or None.
            return_value: response returned by urlopen() when no side effect is given.
            **kwargs: extra arguments for get_urlopen_output().

        Returns:
            tuple: (get_urlopen_output() result, the urlopen mock).
        """
        mock_urlopen = MagicMock(side_effect=side_effect, return_value=return_value)
        with patch.object(https.urllib.request, "urlopen", mock_urlopen):
            result = https.get_urlopen_output(urllib.request.Request(TRF_URL), None, **kwargs)
        return result, mock_urlopen

    def test_default_timeout_from_config(self):
        """Without an explicit timeout, config.Pilot.http_maxtime is used (previously no timeout at all)."""
        response = MagicMock()
        (exitcode, output), mock_urlopen = self._open(return_value=response)
        self.assertEqual((exitcode, output), (0, response))
        self.assertEqual(mock_urlopen.call_args.kwargs["timeout"], int(https.config.Pilot.http_maxtime))

    def test_explicit_timeout(self):
        """An explicit timeout is passed through to urlopen()."""
        (exitcode, _), mock_urlopen = self._open(return_value=MagicMock(), timeout=7)
        self.assertEqual(exitcode, 0)
        self.assertEqual(mock_urlopen.call_args.kwargs["timeout"], 7)

    def test_transport_failures_are_caught(self):
        """Time-outs, resets and protocol errors return (-1, '') instead of raising."""
        for exc in (TimeoutError("timed out"), socket.timeout("timed out"), ConnectionResetError(104, "reset"),
                    http.client.RemoteDisconnected("closed"), http.client.IncompleteRead(b"", 10),
                    urllib.error.URLError("timed out"), ssl.SSLError("handshake failed")):
            with self.subTest(exc=type(exc).__name__):
                (exitcode, output), _ = self._open(side_effect=exc)
                self.assertEqual((exitcode, output), (-1, ""))

    def test_http_error(self):
        """An HTTP error returns (-1, '') and logs the body."""
        exc = urllib.error.HTTPError(TRF_URL, 503, "Service Unavailable", {}, BytesIO(b"busy"))
        with self.assertLogs(https.logger, level="WARNING") as logs:
            (exitcode, output), _ = self._open(side_effect=exc)
        self.assertEqual((exitcode, output), (-1, ""))
        self.assertTrue(any("server error (503)" in line and "busy" in line for line in logs.output))

    def test_http_error_with_unreadable_body(self):
        """An HTTP error whose body read times out is still handled."""
        body = MagicMock()
        body.read.side_effect = TimeoutError("timed out")
        exc = urllib.error.HTTPError(TRF_URL, 503, "Service Unavailable", {}, body)
        (exitcode, output), _ = self._open(side_effect=exc)
        self.assertEqual((exitcode, output), (-1, ""))


class TrfHandler(BaseHTTPRequestHandler):
    """Serve the transform at /trf/ok, and 404 for anything else."""

    def do_GET(self):  # pylint: disable=invalid-name
        """Answer a GET request."""
        if self.path.endswith("/ok"):
            body, code = b"#!/bin/bash\n", 200
        else:
            body, code = b"<html><body>404 Not Found</body></html>", 404
        self.send_response(code)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):  # noqa: A002
        """Silence the request log."""


@unittest.skipIf(shutil.which("curl") is None, "curl is not available")
class TestCurlHttpError(unittest.TestCase):
    """With real curl against a local server: an HTTP error must fail the download, not save the error page."""

    @classmethod
    def setUpClass(cls):
        """Start a local HTTP server."""
        cls.server = HTTPServer(("127.0.0.1", 0), TrfHandler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.server.server_address[1]}/trf"

    @classmethod
    def tearDownClass(cls):
        """Stop the local HTTP server."""
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self):
        """Create a work directory and use IPv4 curl in the rubin plugin (the server listens on 127.0.0.1)."""
        self.workdir = tempfile.mkdtemp()
        self._harvester = os.environ.pop("HARVESTER_WORKDIR", None)
        self._ip_version = os.environ.get("PILOT_IP_VERSION")
        os.environ["PILOT_IP_VERSION"] = "IPv4"

    def tearDown(self):
        """Remove the work directory and restore the environment."""
        shutil.rmtree(self.workdir, ignore_errors=True)
        if self._harvester is not None:
            os.environ["HARVESTER_WORKDIR"] = self._harvester
        if self._ip_version is None:
            os.environ.pop("PILOT_IP_VERSION", None)
        else:
            os.environ["PILOT_IP_VERSION"] = self._ip_version

    def test_http_404_fails_the_download(self):
        """A 404 makes curl exit 22; no error page is left behind as the transform."""
        for module in CURL_PLUGINS:
            with self.subTest(plugin=module.__name__):
                with patch.object(module, "sleep") as mock_sleep:
                    status, diagnostics = module.download_transform(f"{self.base}/missing", TRF_NAME, self.workdir)

                self.assertFalse(status)
                self.assertIn("curl command failed: 22", diagnostics)
                self.assertEqual(mock_sleep.call_count, 2)  # three trials
                self.assertFalse(os.path.exists(os.path.join(self.workdir, TRF_NAME)))

    def test_http_200_saves_the_transform(self):
        """A successful download saves the transform unchanged."""
        for module in CURL_PLUGINS:
            with self.subTest(plugin=module.__name__):
                with patch.object(module, "sleep"):
                    status, _ = module.download_transform(f"{self.base}/ok", TRF_NAME, self.workdir)

                path = os.path.join(self.workdir, TRF_NAME)
                self.assertTrue(status)
                with open(path, "rb") as _file:
                    self.assertEqual(_file.read(), b"#!/bin/bash\n")
                os.remove(path)


class TestUploadFile(unittest.TestCase):
    """upload_file() must apply its timeout and never raise on a transport failure."""

    def setUp(self):
        """Create a JSON file to upload."""
        handle, self.path = tempfile.mkstemp(suffix=".json")
        with os.fdopen(handle, "wb") as _file:
            _file.write(b'{"maxRSS": 1}')

    def tearDown(self):
        """Remove the JSON file."""
        os.remove(self.path)

    def _upload(self, read_side_effect: BaseException | None = None, data: bytes = b"ok",
                urlopen_side_effect: BaseException | None = None, **kwargs):
        """Call upload_file() with a patched urlopen().

        Args:
            read_side_effect: exception raised while reading the response.
            data: response body.
            urlopen_side_effect: exception raised by urlopen() itself.
            **kwargs: extra arguments for upload_file().

        Returns:
            tuple: (upload_file() result, the urlopen mock).
        """
        if urlopen_side_effect is not None:
            mock_urlopen = MagicMock(side_effect=urlopen_side_effect)
        else:
            mock_urlopen = MagicMock(return_value=make_response(read_side_effect, data=data))
        with patch.object(https.urllib.request, "urlopen", mock_urlopen):
            status = https.upload_file("https://pilot.atlas-ml.org", self.path, **kwargs)
        return status, mock_urlopen

    def test_success_and_timeout(self):
        """An 'ok' response succeeds, and the timeout is passed to urlopen() (it was ignored before)."""
        status, mock_urlopen = self._upload()
        self.assertTrue(status)
        self.assertEqual(mock_urlopen.call_args.kwargs["timeout"], 20)
        self.assertEqual(mock_urlopen.call_args.args[0].data, b'{"maxRSS": 1}')

    def test_explicit_timeout(self):
        """An explicit timeout is passed through to urlopen()."""
        _, mock_urlopen = self._upload(timeout=5)
        self.assertEqual(mock_urlopen.call_args.kwargs["timeout"], 5)

    def test_not_ok_response(self):
        """Any other response is a failure."""
        self.assertFalse(self._upload(data=b"error")[0])

    def test_undecodable_response(self):
        """A non-UTF-8 response is a failure, not an exception."""
        self.assertFalse(self._upload(data=b"\xff\xfe")[0])

    def test_transport_failures_are_caught(self):
        """Read time-outs, resets, protocol errors and HTTP errors return False instead of raising."""
        for kwargs in ({"read_side_effect": TimeoutError("timed out")},
                       {"read_side_effect": socket.timeout("timed out")},
                       {"read_side_effect": ConnectionResetError(104, "reset")},
                       {"read_side_effect": http.client.IncompleteRead(b"", 2)},
                       {"urlopen_side_effect": http.client.RemoteDisconnected("closed")},
                       {"urlopen_side_effect": urllib.error.HTTPError("u", 500, "err", {}, BytesIO(b""))},
                       {"urlopen_side_effect": urllib.error.URLError("timed out")}):
            with self.subTest(**{k: type(v).__name__ for k, v in kwargs.items()}):
                self.assertFalse(self._upload(**kwargs)[0])


if __name__ == "__main__":
    unittest.main()
