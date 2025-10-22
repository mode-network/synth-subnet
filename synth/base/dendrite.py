import sys
import traceback
from typing import List, Optional, Tuple, Type, Union
import asyncio
import uuid
import aiohttp


import bittensor as bt
from bittensor_wallet import Keypair, Wallet
import httpx
import pydantic
import uvloop


from synth.protocol import Simulation


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

_ERROR_MAPPINGS: List[Tuple[Type[Exception], Tuple[Union[str, None], str]]] = [
    # aiohttp server‐side connection issues
    (aiohttp.ServerTimeoutError, ("504", "Server timeout error")),
    (aiohttp.ServerDisconnectedError, ("503", "Service disconnected")),
    (aiohttp.ServerConnectionError, ("503", "Service connection error")),
    # timeouts (asyncio + httpx)
    (asyncio.TimeoutError, ("408", "Request timeout")),
    (httpx.ReadTimeout, ("408", "Read timeout")),
    (httpx.WriteTimeout, ("408", "Write timeout")),
    (httpx.ConnectTimeout, ("408", "Request timeout")),
    # httpx connection issues
    (httpx.ConnectError, ("503", "Service unavailable")),
    (httpx.PoolTimeout, ("503", "Connection pool timeout")),
    (httpx.ReadError, ("503", "Read error")),
    # aiohttp payload & response parsing
    (aiohttp.ClientPayloadError, ("400", "Payload error")),
    (aiohttp.ClientResponseError, (None, "Client response error")),
    # httpx protocol errors
    (httpx.RemoteProtocolError, ("502", "Protocol error")),
    (httpx.ProtocolError, ("502", "Protocol error")),
    (httpx.UnsupportedProtocol, ("400", "Unsupported protocol")),
    # httpx decoding & status
    (httpx.DecodingError, ("400", "Response decoding error")),
    (httpx.HTTPStatusError, (None, "Client response error")),
    # catch‐alls (aiohttp first, then httpx)
    (aiohttp.ClientConnectorError, ("503", "Service unavailable")),
    (aiohttp.ClientError, ("500", "Client error")),
    (httpx.RequestError, ("500", "Request error")),
    (httpx.HTTPError, (None, "HTTP error")),
]

_DENDRITE_DEFAULT_ERROR: Tuple[str, str] = ("422", "Failed to parse response")


def process_error_message(
    synapse: Simulation,
    request_name: str,
    exception: Exception,
):
    log_exception(exception)

    status_code, status_message = None, str(type(exception))
    for exc_type, (code, default_msg) in _ERROR_MAPPINGS:
        if isinstance(exception, exc_type):
            status_code, status_message = code, default_msg
            break

    if status_code is None:
        if isinstance(exception, aiohttp.ClientResponseError):
            status_code = str(exception.status)
        elif isinstance(exception, httpx.HTTPStatusError):
            status_code = str(exception.response.status_code)
        else:
            # last‐ditch fallback
            status_code = _DENDRITE_DEFAULT_ERROR[0]

    if isinstance(
        exception, (aiohttp.ClientConnectorError, httpx.HTTPStatusError)
    ):
        host = getattr(synapse.axon, "ip", "<unknown>")
        port = getattr(synapse.axon, "port", "<unknown>")
        message = f"{status_message} at {host}:{port}/{request_name}"
    elif isinstance(exception, (asyncio.TimeoutError, httpx.ReadTimeout)):
        timeout = getattr(synapse, "timeout", "<unknown>")
        message = f"{status_message} after {timeout} seconds"
    else:
        message = f"{status_message}: {exception}"

    synapse.dendrite.status_code = status_code
    synapse.dendrite.status_message = message

    return synapse


class SynthDendrite(bt.Dendrite):
    def __init__(self, wallet: Optional[Union[Wallet, Keypair]] = None):
        super().__init__(wallet=wallet)


def log_exception(exception: Exception):
    """
    Logs an exception with a unique identifier.

    This method generates a unique UUID for the error, extracts the error type,
    and logs the error message using Bittensor's logging system.

    Args:
        exception (Exception): The exception object to be logged.

    Returns:
        None
    """
    error_id = str(uuid.uuid4())
    error_type = exception.__class__.__name__
    if isinstance(exception, pydantic.ValidationError):
        return  # Skip logging for validation errors
    if isinstance(
        exception,
        (
            aiohttp.ClientOSError,
            asyncio.TimeoutError,
            httpx.ConnectError,
            httpx.ReadError,
            httpx.HTTPStatusError,
            httpx.ReadTimeout,
            httpx.ConnectTimeout,
        ),
    ):
        bt.logging.debug(f"{error_type}#{error_id}: {exception}")
    else:
        bt.logging.error(f"{error_type}#{error_id}: {exception}")
        traceback.print_exc(file=sys.stderr)
