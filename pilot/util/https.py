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
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Mario Lassnig, mario.lassnig@cern.ch, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-23

"""Functions for https interactions."""

import json
import logging
import os
import pipes
import platform
import random
import socket
import ssl
import sys
import urllib.request
import urllib.error
import urllib.parse
from collections import namedtuple
from re import findall
from time import sleep, time
from typing import Callable, Any

from .config import config
from .constants import get_pilot_version
from .container import execute
from .filehandling import write_file, read_file
from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import FileHandlingFailure

logger = logging.getLogger(__name__)
errors = ErrorCodes()

_ctx = namedtuple('_ctx', 'ssl_context user_agent capath cacert')
_ctx.ssl_context = None
_ctx.user_agent = None
_ctx.capath = None
_ctx.cacert = None

# anisyonk: public copy of `_ctx` to avoid logic break since ssl_context is reset inside the request() -- FIXME
# anisyonk: public instance, should be properly initialized by `https_setup()`
# anisyonk: use lightweight class definition instead of namedtuple since tuple is immutable and we don't need/use any tuple features here
ctx = type('ctx', (object,), dict(ssl_context=None, user_agent='Pilot3 client', capath=None, cacert=None))


def _tester(func: Callable[..., Any], *args: Any) -> Any:
    """
    Test function ``func`` on the given arguments and return the first positive.

    >>> _tester(lambda x: x%3 == 0, 1, 2, 3, 4, 5, 6)
    3
    >>> _tester(lambda x: x%3 == 0, 1, 2)
    None

    :param func: the function to be tested (Callable)
    :param args: other arguments (Any)
    :return: something or none (Any).
    """
    for arg in args:
        if arg is not None and func(arg):
            return arg

    return None


def capath(args: Any = None) -> Any:
    """
    Try to get :abbr:`CA (Certification Authority)` path with certificates.

    Tries
    1. :option:`--capath` from arguments
    2. :envvar:`X509_CERT_DIR` from env
    3. Path ``/etc/grid-security/certificates``

    :param args: arguments, parsed by argparse (Any)
    :returns: directory path (str), or None.
    """
    return _tester(os.path.isdir,
                   args and args.capath,
                   os.environ.get('X509_CERT_DIR'),
                   '/etc/grid-security/certificates')


def cacert_default_location() -> Any:
    """
    Try to get current user ID through `os.getuid`, and get the posix path for x509 certificate.

    :returns: `str` -- posix default x509 path, or `None`
    """
    try:
        return f'/tmp/x509up_u{os.getuid()}'
    except AttributeError:
        logger.warning('no UID available? System not POSIX-compatible... trying to continue')
        pass

    return None


def cacert(args: Any = None) -> Any:
    """
    Try to get :abbr:`CA (Certification Authority)` certificate or X509.

    Checks that it is a regular file.
    Tries
    1. :option:`--cacert` from arguments
    2. :envvar:`X509_USER_PROXY` from env
    3. Path ``/tmp/x509up_uXXX``, where ``XXX`` refers to ``UID``

    :param args: arguments, parsed by argparse (Any)
    :returns: `str` -- certificate file path, or `None` (Any).
    """
    return _tester(os.path.isfile,
                   args and args.cacert,
                   os.environ.get('X509_USER_PROXY'),
                   cacert_default_location())


def https_setup(args: Any = None, version: str = ""):
    """
    Set up the context for HTTPS requests.

    1. Selects the certificate paths
    2. Sets up :mailheader:`User-Agent`
    3. Tries to create `ssl.SSLContext` for future use (falls back to :command:`curl` if fails)

    :param args: arguments, parsed by argparse (Any)
    :param version: pilot version string (for :mailheader:`User-Agent`) (str).
    """
    version = version or get_pilot_version()

    _ctx.user_agent = f'pilot/{version} (Python {sys.version.split()[0]}; {platform.system()} {platform.machine()})'
    _ctx.capath = capath(args)
    _ctx.cacert = cacert(args)

    try:
        _ctx.ssl_context = ssl.create_default_context(capath=_ctx.capath,
                                                      cafile=_ctx.cacert)
    except Exception as exc:
        logger.warning(f'SSL communication is impossible due to SSL error: {exc} -- falling back to curl')
        _ctx.ssl_context = None

    # anisyonk: clone `_ctx` to avoid logic break since ssl_context is reset inside the request() -- FIXME
    ctx.capath = _ctx.capath
    ctx.cacert = _ctx.cacert
    ctx.user_agent = _ctx.user_agent

    try:
        ctx.ssl_context = ssl.create_default_context(capath=ctx.capath, cafile=ctx.cacert)
        ctx.ssl_context.load_cert_chain(ctx.cacert)
    except Exception as exc:
        logger.warning(f'Failed to initialize SSL context .. skipped, error: {exc}')


def request(url: str, data: dict = {}, plain: bool = False, secure: bool = True, ipv: str = 'IPv6') -> Any:
    """
    Send a request using HTTPS.

    Sends :mailheader:`User-Agent` and certificates previously being set up by `https_setup`.
    If `ssl.SSLContext` is available, uses `urllib2` as a request processor. Otherwise uses :command:`curl`.

    If ``data`` is provided, encodes it as a URL form data and sends it to the server.

    Treats the request as JSON unless a parameter ``plain`` is `True`.
    If JSON is expected, sends ``Accept: application/json`` header.

    Usage:

    .. code-block:: python
        :emphasize-lines: 2

        https_setup(args, PILOT_VERSION)  # sets up ssl and other stuff
        response = request('https://some.url', {'some':'data'})

    :param url: the URL of the resource (str)
    :param data: data to send (dict)
    :param plain: if true, treats the response as a plain text (bool)
    :param secure: default: True, i.e. use certificates (bool)
    :param ipv: internet protocol version (str).
    :returns:
        - :keyword:`dict` -- if everything went OK
        - `str` -- if ``plain`` parameter is `True`
        - `None` -- if something went wrong
    """
    _ctx.ssl_context = None  # certificates are not available on the grid, use curl

    # note that X509_USER_PROXY might change during running (in the case of proxy downloads), so
    # we might have to update _ctx
    update_ctx()

    logger.debug(f'server update dictionary = \n{data}')

    # get the filename and strdata for the curl config file
    filename, strdata = get_vars(url, data)
    # write the strdata to file
    try:
        writestatus = write_file(filename, strdata)
    except FileHandlingFailure:
        writestatus = None

    # get the config option for the curl command
    dat = get_curl_config_option(writestatus, url, data, filename)

    # loop over internet protocol versions since proper value might be known yet (ie before downloading queuedata)
    ipvs = ['IPv6', 'IPv4'] if ipv == 'IPv6' else ['IPv4']
    if _ctx.ssl_context is None and secure:
        failed = False
        for _ipv in ipvs:
            req, obscure = get_curl_command(plain, dat, _ipv)
            if not req:
                logger.warning('failed to construct valid curl command')
                failed = True
                break
            try:
                status, output, stderr = execute(req, obscure=obscure, timeout=130)
            except Exception as exc:
                logger.warning(f'exception: {exc}')
                failed = True
                break
            else:
                if status == 0:
                    break
                else:
                    logger.warning(f'request failed for IPv={_ipv} ({status}): stdout={output}, stderr={stderr}')
                    continue
        if failed:
            return None

        # return output if plain otherwise return json.loads(output)
        if plain:
            return output
        else:
            try:
                ret = json.loads(output)
            except Exception as exc:
                logger.warning(f'json.loads() failed to parse output={output}: {exc}')
                return None
            else:
                return ret
    else:
        req = execute_urllib(url, data, plain, secure)
        context = _ctx.ssl_context if secure else None

        ec, output = get_urlopen_output(req, context)
        if ec:
            return None

        return output.read() if plain else json.load(output)


def update_ctx():
    """Update the ctx object in case X509_USER_PROXY has been updated."""
    x509 = os.environ.get('X509_USER_PROXY', _ctx.cacert)
    if x509 != _ctx.cacert and os.path.exists(x509):
        _ctx.cacert = x509
    certdir = os.environ.get('X509_CERT_DIR', _ctx.capath)
    if certdir != _ctx.capath and os.path.exists(certdir):
        _ctx.capath = certdir


def get_curl_command(plain: bool, dat: str, ipv: str) -> (Any, str):
    """
    Get the curl command.

    :param plain: if true, treats the response as a plain text (bool)
    :param dat: curl config option (str)
    :param ipv: internet protocol version (str)
    :return: curl command (str or None), sensitive string to be obscured before dumping to log (str).
    """
    auth_token_content = ''
    auth_token = os.environ.get('OIDC_AUTH_TOKEN', os.environ.get('PANDA_AUTH_TOKEN', None))  # file name of the token
    auth_origin = os.environ.get('OIDC_AUTH_ORIGIN', os.environ.get('PANDA_AUTH_ORIGIN', None))  # origin of the token (panda_dev.pilot)

    command = 'curl'
    if ipv == 'IPv4':
        command += ' -4'
    if auth_token and auth_origin:
        # curl --silent --capath
        # /cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/etc/grid-security-emi/certificates --compressed
        # -H "Authorization: Bearer <contents of PANDA_AUTH_TOKEN>" -H "Origin: <PANDA_AUTH_VO>"
        path = locate_token(auth_token)
        auth_token_content = ""
        if os.path.exists(path):
            auth_token_content = read_file(path)
            if not auth_token_content:
                logger.warning(f'failed to read file {path}')
                return None, ''
        else:
            logger.warning(f'path does not exist: {path}')
            return None, ''
        if not auth_token_content:
            logger.warning('OIDC_AUTH_TOKEN/PANDA_AUTH_TOKEN content could not be read')
            return None, ''
        req = f'{command} -sS --compressed --connect-timeout {config.Pilot.http_connect_timeout} ' \
              f'--max-time {config.Pilot.http_maxtime} '\
              f'--capath {pipes.quote(_ctx.capath or "")} ' \
              f'-H "Authorization: Bearer {pipes.quote(auth_token_content)}" ' \
              f'-H {pipes.quote("Accept: application/json") if not plain else ""} ' \
              f'-H "Origin: {pipes.quote(auth_origin)}" {dat}'
    else:
        req = f'{command} -sS --compressed --connect-timeout {config.Pilot.http_connect_timeout} ' \
              f'--max-time {config.Pilot.http_maxtime} '\
              f'--capath {pipes.quote(_ctx.capath or "")} ' \
              f'--cert {pipes.quote(_ctx.cacert or "")} ' \
              f'--cacert {pipes.quote(_ctx.cacert or "")} ' \
              f'--key {pipes.quote(_ctx.cacert or "")} '\
              f'-H {pipes.quote(f"User-Agent: {_ctx.user_agent}")} ' \
              f'-H {pipes.quote("Accept: application/json") if not plain else ""} {dat}'

    #logger.info('request: %s', req)
    return req, auth_token_content


def locate_token(auth_token: str) -> str:
    """
    Locate the token file.

    :param auth_token: file name of token (str)
    :return: path to token (str).
    """
    _primary = os.path.dirname(os.environ.get('OIDC_AUTH_DIR', os.environ.get('PANDA_AUTH_DIR', os.environ.get('X509_USER_PROXY', ''))))
    paths = [os.path.join(_primary, auth_token),
             os.path.join(os.environ.get('PILOT_SOURCE_DIR', ''), auth_token),
             os.path.join(os.environ.get('PILOT_WORK_DIR', ''), auth_token)]
    path = ""
    for _path in paths:
        logger.debug(f'looking for {_path}')
        if os.path.exists(_path):
            path = _path
            break

    if path == "":
        logger.info(f'did not find any local token file ({auth_token}) in paths={paths}')

    return path


def get_vars(url: str, data: dict) -> (str, str):
    """
    Get the filename and strdata for the curl config file.

    :param url: URL (str)
    :param data: data to be written to file (dict)
    :return: filename (str), strdata (str).
    """
    strdata = ""
    for key in data:
        strdata += f'data="{urllib.parse.urlencode({key: data[key]})}"\n'
    jobid = f"_{data['jobId']}" if 'jobId' in list(data.keys()) else ""

    # write data to temporary config file
    filename = f"{os.getenv('PILOT_HOME')}/curl_{os.path.basename(url)}{jobid}.config"

    return filename, strdata


def get_curl_config_option(writestatus: bool, url: str, data: dict, filename: str) -> str:
    """
    Get the curl config option.

    :param writestatus: status of write_file call (bool)
    :param url: URL (str)
    :param data: data structure (dict)
    :param filename: file name of config file (str)
    :return: config option (str).
    """
    if not writestatus:
        logger.warning('failed to create curl config file (will attempt to urlencode data directly)')
        dat = pipes.quote(url + '?' + urllib.parse.urlencode(data) if data else '')
    else:
        dat = f'--config {filename} {url}'

    return dat


def execute_urllib(url: str, data: dict, plain: bool, secure: bool) -> Any:
    """
    Execute the request using urllib.

    :param url: URL (str)
    :param data: data structure (dict)
    :param plain: if true, treats the response as a plain text (bool)
    :param secure: default: True, i.e. use certificates (bool)
    :return: urllib request structure (Any).
    """
    req = urllib.request.Request(url, urllib.parse.urlencode(data))
    if not plain:
        req.add_header('Accept', 'application/json')
    if secure:
        req.add_header('User-Agent', _ctx.user_agent)

    return req


def get_urlopen_output(req: Any, context: Any) -> (int, str):
    """
    Get the output from the urlopen request.

    :param req: urllib request structure (Any)
    :param context: ssl context (Any)
    :return: exit code (int), output (str).
    """
    exitcode = -1
    output = ""
    try:
        output = urllib.request.urlopen(req, context=context)
    except urllib.error.HTTPError as exc:
        logger.warning(f'server error ({exc.code}): {exc.read()}')
    except urllib.error.URLError as exc:
        logger.warning(f'connection error: {exc.reason}')
    else:
        exitcode = 0

    return exitcode, output


def send_update(update_function: str, data: dict, url: str, port: str, job: Any = None, ipv: str = 'IPv6') -> dict:
    """
    Send the update to the server using the given function and data.

    :param update_function: 'updateJob' or 'updateWorkerPilotStatus' (str)
    :param data: data (dict)
    :param url: server url (str)
    :param port: server port (str)
    :param job: job object (Any)
    :param ipv: internet protocol version, IPv4 or IPv6 (str)
    :return: server response (dict).
    """
    time_before = int(time())
    max_attempts = 10
    attempt = 0
    done = False
    res = None

    if os.environ.get('REACHED_MAXTIME', None) and update_function == 'updateJob':
        data['state'] = 'failed'
        if job:
            job.state = 'failed'
            job.completed = True
            msg = 'the max batch system time limit has been reached'
            logger.warning(msg)
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.REACHEDMAXTIME, msg=msg)
            add_error_codes(data, job)

    # do not allow any delayed heartbeat messages for running state, if the job has completed (ie another call to this
    # function was already made by another thread for finished/failed state)
    if job:  # ignore for updateWorkerPilotStatus calls
        if job.completed and (job.state == 'running' or job.state == 'starting'):
            logger.warning(f'will not send job update for {job.state} state since the job has already completed')
            return None  # should be ignored

    while attempt < max_attempts and not done:
        logger.info(f'server update attempt {attempt + 1}/{max_attempts}')

        # get the URL for the PanDA server from pilot options or from config
        try:
            pandaserver = get_panda_server(url, port)
        except Exception as exc:
            logger.warning(f'exception caught in get_panda_server(): {exc}')
            sleep(5)
            attempt += 1
            continue
        # send the heartbeat
        try:
            res = request(f'{pandaserver}/server/panda/{update_function}', data=data, ipv=ipv)
        except Exception as exc:
            logger.warning(f'exception caught in https.request(): {exc}')
        else:
            if res is not None:
                done = True
            txt = f'server {update_function} request completed in {int(time()) - time_before}s'
            if job:
                txt += f' for job {job.jobid}'
            logger.info(txt)
            # hide sensitive info
            pilotsecrets = ''
            if res and 'pilotSecrets' in res:
                pilotsecrets = res['pilotSecrets']
                res['pilotSecrets'] = '********'
            logger.info(f'server responded with: res = {res}')
            if pilotsecrets:
                res['pilotSecrets'] = pilotsecrets

        attempt += 1
        if not done:
            sleep(config.Pilot.update_sleep)

    return res


def get_panda_server(url: str, port: str, update_server: bool = True) -> str:
    """
    Get the URL for the PanDA server.

    The URL will be randomized if the server can be contacted (otherwise fixed).

    :param url: URL string, if set in pilot option (port not included) (str)
    :param port: port number, if set in pilot option (str)
    :param update_server: True if the server can be contacted, False otherwise (bool)
    :return: full URL (either from pilot options or from config file) (str).
    """
    if url != '':
        parsedurl = url.split('://')
        scheme = None
        if len(parsedurl) == 2:
            scheme = parsedurl[0]
            loc = parsedurl[1]
        else:
            loc = parsedurl[0]

        parsedloc = loc.split(':')
        loc = parsedloc[0]

        # if a port is provided in the url, then override the port argument
        if len(parsedloc) == 2:
            port = parsedloc[1]
        # default scheme to https
        if not scheme:
            scheme = "https"
        portstr = f":{port}" if port else ""
        pandaserver = f"{scheme}://{loc}{portstr}"
    else:
        pandaserver = config.Pilot.pandaserver
        if not pandaserver.startswith('http'):
            pandaserver = 'https://' + pandaserver

    if not update_server:
        return pandaserver

    # add randomization for PanDA server
    default = 'pandaserver.cern.ch'
    if default in pandaserver:
        rnd = random.choice([socket.getfqdn(vv) for vv in set([v[-1][0] for v in socket.getaddrinfo(default, 25443, socket.AF_INET)])])
        pandaserver = pandaserver.replace(default, rnd)
        logger.debug(f'updated {default} to {pandaserver}')

    return pandaserver


def add_error_codes(data: dict, job: Any):
    """
    Add error codes to data structure.

    :param data: data dictionary (dict)
    :param job: job object (Any).
    """
    # error codes
    pilot_error_code = job.piloterrorcode
    pilot_error_codes = job.piloterrorcodes
    if pilot_error_codes != []:
        logger.warning(f'pilotErrorCodes = {pilot_error_codes} (will report primary/first error code)')
        data['pilotErrorCode'] = pilot_error_codes[0]
    else:
        data['pilotErrorCode'] = pilot_error_code

    # add error info
    pilot_error_diag = job.piloterrordiag
    pilot_error_diags = job.piloterrordiags
    if pilot_error_diags != []:
        logger.warning(f'pilotErrorDiags = {pilot_error_diags} (will report primary/first error diag)')
        data['pilotErrorDiag'] = pilot_error_diags[0]
    else:
        data['pilotErrorDiag'] = pilot_error_diag
    data['transExitCode'] = job.transexitcode
    data['exeErrorCode'] = job.exeerrorcode
    data['exeErrorDiag'] = job.exeerrordiag


def get_server_command(url: str, port: str, cmd: str = 'getJob') -> str:
    """
    Prepare the getJob server command.

    :param url: PanDA server URL (str)
    :param port: PanDA server port (str)
    :return: full server command (str).
    """
    if url != "":
        port_pattern = '.:([0-9]+)'
        if not findall(port_pattern, url):
            url = url + f':{port}'
        else:
            logger.debug(f'URL already contains port: {url}')
    else:
        url = config.Pilot.pandaserver
    if url == "":
        logger.fatal('PanDA server url not set (either as pilot option or in config file)')
    elif not url.startswith("http"):
        url = 'https://' + url
        logger.warning('detected missing protocol in server url (added)')

    # randomize server name
    url = get_panda_server(url, port)
    return f'{url}/server/panda/{cmd}'
