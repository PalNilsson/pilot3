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
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2026

"""TEMPORARY probe: does the PanDA server accept X.509 proxy authentication from a token-configured pilot?

X509-PROBE-TEMPORARY - REMOVE AFTER VERIFICATION
=================================================
The OIDC token failover (see :func:`pilot.util.https.request2` and
:func:`pilot.util.https.refresh_oidc_token`) relies on the server accepting the pilot's X.509 proxy
on ``api/v1/creds/get_access_token`` and ``api/v1/pilot/update_job``. This module tests both in
production, independently of any token failure:

- at pilot startup, a new token is requested with X.509 instead of the token (the result is only
  logged, the token is not stored);
- the first ``update_job`` of the pilot is sent with X.509 instead of the token; if it is not
  accepted, the update is sent normally with the token, so the job is unaffected.

All log lines start with ``[X509-PROBE]``. Once the result is known, delete this module and the
code marked ``X509-PROBE-TEMPORARY`` in ``pilot.py`` and ``pilot/util/https.py`` (the ``UPDATE_JOB_PROBE``
hook and its use in ``_send_request2()``).
"""

from __future__ import annotations

import logging
from time import time
from typing import Any

from pilot.util import https

logger = logging.getLogger(__name__)

PROBE_TAG = "[X509-PROBE]"

# the update_job probe runs once per pilot
_STATE = {"update_job_probed": False}


def reset() -> None:
    """Reset the probe state and uninstall the update_job hook (for tests)."""
    _STATE["update_job_probed"] = False
    https.UPDATE_JOB_PROBE["function"] = None


def install_update_job_probe() -> None:
    """Install :func:`probe_update_job` as the update_job hook in :mod:`pilot.util.https`."""
    https.UPDATE_JOB_PROBE["function"] = probe_update_job


def _is_applicable() -> bool:
    """Return ``True`` if the probe makes sense, i.e. a token is configured and an X.509 proxy is available.

    Returns:
        ``True`` if the probe should run.
    """
    auth_token, auth_origin = https.get_local_oidc_token_info()
    if not (auth_token and auth_origin):
        logger.info(f"{PROBE_TAG} skipped: no OIDC token configured")
        return False
    if not https.x509_failover_available():
        logger.info(f"{PROBE_TAG} skipped: no X.509 proxy available")
        return False

    return True


def _describe_token(text: str) -> str:
    """Describe the token in a get_access_token response without revealing it.

    Args:
        text: Response body.

    Returns:
        A short description (expiry and remaining lifetime when available).
    """
    token = https._parse_token_response(text)  # pylint: disable=protected-access
    if not token:
        return "no token in the response"
    try:
        payload = https.decode_jwt_payload(token, return_times=False)
        expiry = int(float(payload["exp"]))
    except (ValueError, KeyError, TypeError) as exc:
        return f"token received, expiry unknown ({exc})"

    return f"token received, expires at {https._format_epoch(expiry)}, remaining lifetime {expiry - int(time())} s"  # pylint: disable=protected-access


def probe_token_download(url: str, port: int) -> bool | None:
    """Request a new token with X.509 authentication instead of the token, and log the outcome.

    The downloaded token is not stored.

    Args:
        url: PanDA server base URL.
        port: PanDA server port.

    Returns:
        ``True`` if the server accepted X.509, ``False`` if not, ``None`` if the probe did not run.
    """
    try:
        if not _is_applicable():
            return None
        server_command = https.get_token_refresh_command(url, port)
        if not server_command:
            logger.info(f"{PROBE_TAG} skipped: token key not available")
            return None

        ok, text = https.send_x509_request(server_command, headers=https.get_headers(False, content_type=None))
        if ok:
            logger.info(f"{PROBE_TAG} get_access_token with X.509 authentication: ACCEPTED ({_describe_token(text)})")
        else:
            logger.warning(f"{PROBE_TAG} get_access_token with X.509 authentication: NOT ACCEPTED ({text})")
        return ok
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.warning(f"{PROBE_TAG} get_access_token probe failed: {exc}")
        return None


def probe_update_job(path: str, update_function: str, data: dict) -> dict[str, Any] | None:
    """Send the first update_job of the pilot with X.509 authentication instead of the token.

    Args:
        path: Full server URL of the endpoint.
        update_function: Endpoint path, e.g. ``'api/v1/pilot/update_job'``.
        data: Payload dict.

    Returns:
        The server response if the server accepted X.509, otherwise ``None`` (the caller then
        sends the update with the token as usual).
    """
    if _STATE["update_job_probed"] or not update_function.endswith("update_job"):
        return None
    _STATE["update_job_probed"] = True

    try:
        if not _is_applicable():
            return None

        headers = https.get_headers(False, accept=True)
        https._ensure_numeric_fields(data)  # pylint: disable=protected-access
        data_bytes = https._prepare_body_and_headers(path, data, True, headers)  # pylint: disable=protected-access
        logger.info(f"{PROBE_TAG} sending this update_job with X.509 authentication instead of the token")
        ok, text = https.send_x509_request(path, data_bytes, headers, "POST")
        if not ok:
            logger.warning(f"{PROBE_TAG} update_job with X.509 authentication: NOT ACCEPTED ({text}) "
                           f"- will send the update with the token instead")
            return None

        res = https._parse_response_text(text)  # pylint: disable=protected-access
        if not isinstance(res, dict):
            logger.warning(f"{PROBE_TAG} update_job with X.509 authentication: unexpected response ({str(res)[:256]}) "
                           f"- will send the update with the token instead")
            return None

        logger.info(f"{PROBE_TAG} update_job with X.509 authentication: ACCEPTED (success={res.get('success')})")
        return res
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.warning(f"{PROBE_TAG} update_job probe failed: {exc} - will send the update with the token instead")
        return None
