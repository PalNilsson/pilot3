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

"""Tests for the temporary X.509 acceptance probe.

X509-PROBE-TEMPORARY: delete this file together with pilot/util/x509probe.py.
"""

import base64
import json
import time
import unittest
from unittest.mock import patch

from pilot.util import https, x509probe

URL = "https://aipanda092.cern.ch:25443/api/v1/pilot/update_job"


def make_jwt(exp: float) -> str:
    """Build an unsigned JWT carrying the given expiry.

    Args:
        exp: Expiry (epoch seconds).

    Returns:
        The token string.
    """
    def _segment(obj: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).decode().rstrip("=")

    return f"{_segment({'alg': 'RS256'})}.{_segment({'sub': 'pilot', 'exp': int(exp)})}.signature"


class ProbeTestCase(unittest.TestCase):
    """Base class: a token and a proxy are configured."""

    def setUp(self):
        """Reset the probe and configure a token and a proxy."""
        x509probe.reset()
        self.patches = [
            patch.object(https, "get_local_oidc_token_info", return_value=("panda_token", "atlas.pilot")),
            patch.object(https, "x509_failover_available", return_value=True),
        ]
        for item in self.patches:
            item.start()

    def tearDown(self):
        """Stop the patches and reset the probe."""
        for item in self.patches:
            item.stop()
        x509probe.reset()


class TestProbeUpdateJob(ProbeTestCase):
    """The first update_job of the pilot is sent with X.509."""

    def test_accepted_response_is_used(self):
        """An accepted X.509 update is returned and logged as accepted."""
        with patch.object(https, "send_x509_request", return_value=(True, '{"success": true, "data": {}}')) as send, \
                self.assertLogs(x509probe.logger, level="INFO") as logs:
            res = x509probe.probe_update_job(URL, "api/v1/pilot/update_job", {"job_id": "7367714395"})
        self.assertEqual(res, {"success": True, "data": {}})
        self.assertIn("ACCEPTED", "\n".join(logs.output))
        self.assertEqual(send.call_args[0][3], "POST")
        self.assertNotIn("Authorization", send.call_args[0][2])

    def test_rejected_falls_back_to_token(self):
        """A rejected X.509 update returns None so that the token is used."""
        with patch.object(https, "send_x509_request", return_value=(False, "HTTP Error 403: Forbidden")), \
                self.assertLogs(x509probe.logger, level="INFO") as logs:
            res = x509probe.probe_update_job(URL, "api/v1/pilot/update_job", {"job_id": "7367714395"})
        self.assertIsNone(res)
        self.assertIn("NOT ACCEPTED", "\n".join(logs.output))

    def test_non_dict_response_falls_back_to_token(self):
        """An unparsable response returns None."""
        with patch.object(https, "send_x509_request", return_value=(True, "ERROR : something")), \
                self.assertLogs(x509probe.logger, level="WARNING") as logs:
            self.assertIsNone(x509probe.probe_update_job(URL, "api/v1/pilot/update_job", {}))
        self.assertIn("unexpected response (ERROR : something)", "\n".join(logs.output))

    def test_runs_once(self):
        """Only the first update_job is probed."""
        with patch.object(https, "send_x509_request", return_value=(False, "denied")) as send:
            x509probe.probe_update_job(URL, "api/v1/pilot/update_job", {})
            self.assertIsNone(x509probe.probe_update_job(URL, "api/v1/pilot/update_job", {}))
        send.assert_called_once()

    def test_other_endpoints_not_probed(self):
        """Other endpoints are not probed and do not use up the probe."""
        with patch.object(https, "send_x509_request") as send:
            self.assertIsNone(x509probe.probe_update_job(URL, "api/v1/pilot/update_worker_status", {}))
        send.assert_not_called()
        with patch.object(https, "send_x509_request", return_value=(False, "denied")) as send:
            x509probe.probe_update_job(URL, "api/v1/pilot/update_job", {})
        send.assert_called_once()

    def test_skipped_without_token(self):
        """Without a token, nothing is probed."""
        with patch.object(https, "get_local_oidc_token_info", return_value=(None, None)), \
                patch.object(https, "send_x509_request") as send:
            self.assertIsNone(x509probe.probe_update_job(URL, "api/v1/pilot/update_job", {}))
        send.assert_not_called()

    def test_exception_is_contained(self):
        """An exception never reaches the caller."""
        with patch.object(https, "send_x509_request", side_effect=RuntimeError("boom")):
            self.assertIsNone(x509probe.probe_update_job(URL, "api/v1/pilot/update_job", {}))


class TestProbeHook(ProbeTestCase):
    """The update_job probe is reached through the hook in pilot.util.https."""

    def test_not_installed_by_default(self):
        """Without installation, send_request() does not probe."""
        self.assertIsNone(https.UPDATE_JOB_PROBE["function"])

    def test_installed_probe_answers_send_request(self):
        """Once installed, an accepted probe response is used and request2() is not called."""
        x509probe.install_update_job_probe()
        with patch.object(https, "send_x509_request", return_value=(True, '{"success": true, "data": {}}')), \
                patch.object(https, "request2") as request2:
            res = https.send_request("https://pandaserver.cern.ch:25443", "api/v1/pilot/update_job", {}, None, "IPv6")
        self.assertEqual(res, {"success": True, "data": {}})
        request2.assert_not_called()

    def test_rejected_probe_falls_back_to_request2(self):
        """A rejected probe leaves the update to request2() with the token."""
        x509probe.install_update_job_probe()
        with patch.object(https, "send_x509_request", return_value=(False, "denied")), \
                patch.object(https, "request2", return_value={"success": True}) as request2:
            res = https.send_request("https://pandaserver.cern.ch:25443", "api/v1/pilot/update_job", {}, None, "IPv6")
        self.assertEqual(res, {"success": True})
        request2.assert_called_once()


class TestProbeTokenDownload(ProbeTestCase):
    """At startup, a token is requested with X.509."""

    def test_accepted_logs_expiry_not_token(self):
        """An accepted request logs the expiry, never the token."""
        token = make_jwt(time.time() + 4 * 86400)
        body = json.dumps({"success": True, "data": {"access_token": token}})
        with patch.object(https, "get_token_refresh_command", return_value="https://server/get_access_token"), \
                patch.object(https, "send_x509_request", return_value=(True, body)), \
                self.assertLogs(x509probe.logger, level="INFO") as logs:
            self.assertTrue(x509probe.probe_token_download("https://server", 25443))
        output = "\n".join(logs.output)
        self.assertIn("ACCEPTED", output)
        self.assertIn("remaining lifetime", output)
        self.assertNotIn(token, output)

    def test_rejected(self):
        """A rejected request is logged as not accepted."""
        with patch.object(https, "get_token_refresh_command", return_value="https://server/get_access_token"), \
                patch.object(https, "send_x509_request", return_value=(False, "HTTP Error 403")), \
                self.assertLogs(x509probe.logger, level="INFO") as logs:
            self.assertFalse(x509probe.probe_token_download("https://server", 25443))
        self.assertIn("NOT ACCEPTED", "\n".join(logs.output))

    def test_no_token_in_response(self):
        """A response without a token is described as such."""
        self.assertEqual(x509probe._describe_token('{"success": false, "message": "x"}'), "no token in the response")

    def test_undecodable_token(self):
        """A token that is not a JWT has an unknown expiry."""
        self.assertIn("expiry unknown", x509probe._describe_token('{"success": true, "data": {"access_token": "abc"}}'))

    def test_skipped_without_key(self):
        """Without a token key, the probe does not run."""
        with patch.object(https, "get_token_refresh_command", return_value=""), \
                patch.object(https, "send_x509_request") as send:
            self.assertIsNone(x509probe.probe_token_download("https://server", 25443))
        send.assert_not_called()

    def test_skipped_without_proxy(self):
        """Without a proxy, the probe does not run."""
        with patch.object(https, "x509_failover_available", return_value=False), \
                patch.object(https, "send_x509_request") as send:
            self.assertIsNone(x509probe.probe_token_download("https://server", 25443))
        send.assert_not_called()

    def test_exception_is_contained(self):
        """An exception never reaches the caller."""
        with patch.object(https, "get_token_refresh_command", side_effect=RuntimeError("boom")):
            self.assertIsNone(x509probe.probe_token_download("https://server", 25443))


if __name__ == "__main__":
    unittest.main()
