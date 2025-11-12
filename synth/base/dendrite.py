import sys
import traceback
from typing import List, Optional, Tuple, Type, Union
import time
import asyncio
import uuid
import aiohttp


import bittensor as bt
from bittensor_wallet import Keypair, Wallet
import httpx
from pydantic import ValidationError
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

    async def forward(
        self,
        axons: Union[
            list[Union[bt.AxonInfo, bt.Axon]], Union[bt.AxonInfo, bt.Axon]
        ],
        synapse: Simulation,
        timeout: float = 12,
        run_async: bool = True,
    ) -> list[Simulation]:
        is_list = True
        # If a single axon is provided, wrap it in a list for uniform processing
        if not isinstance(axons, list):
            is_list = False
            axons = [axons]

        async def query_all_axons():
            async def single_axon_response(
                target_axon: Union[bt.AxonInfo, bt.Axon],
            ) -> Simulation:
                async with httpx.AsyncClient(
                    http2=True,
                    limits=httpx.Limits(
                        max_connections=None, max_keepalive_connections=25
                    ),
                    timeout=timeout,
                ) as client:
                    return await self.call_http2(
                        client=client,
                        target_axon=target_axon,
                        synapse=synapse.model_copy(),  # type: ignore
                        timeout=timeout,
                    )

            # If run_async flag is False, get responses one by one.
            # If run_async flag is True, get responses concurrently using asyncio.gather().
            if not run_async:
                return [
                    await single_axon_response(target_axon)
                    for target_axon in axons
                ]  # type: ignore

            return await asyncio.gather(
                *(single_axon_response(target_axon) for target_axon in axons)
            )  # type: ignore

        # Get responses for all axons.
        responses = await query_all_axons()
        # Return the single response if only one axon was targeted, else return all responses
        return responses[0] if len(responses) == 1 and not is_list else responses  # type: ignore

    async def call_http2(
        self,
        client: httpx.AsyncClient,
        target_axon: Union[bt.AxonInfo, bt.Axon],
        synapse: Simulation,
        timeout: float = 12.0,
    ) -> Simulation:
        # Record start time
        start_time = time.time()
        target_axon = (
            target_axon.info()
            if isinstance(target_axon, bt.Axon)
            else target_axon
        )

        # Build request endpoint from the synapse class
        request_name = synapse.__class__.__name__
        url = self._get_endpoint_url(target_axon, request_name=request_name)

        # Preprocess synapse for making a request
        synapse = self.preprocess_synapse_for_request(
            target_axon, synapse, timeout
        )

        try:
            # Log outgoing request
            self._log_outgoing_request(synapse)

            # Make the HTTP POST request
            response = await client.post(
                url=url,
                headers=synapse.to_headers(),
                json=synapse.model_dump(),
                timeout=timeout,
            )
            response.raise_for_status()
            # Extract the JSON response from the server
            json_response = response.json()
            # Process the server response and fill synapse
            status = response.status_code
            headers = response.headers
            self.process_server_response(
                status, headers, json_response, synapse
            )

            # Set process time and log the response
            synapse.dendrite.process_time = str(time.time() - start_time)  # type: ignore

        except Exception as e:
            synapse = self.process_error_message(synapse, request_name, e)

        finally:
            self._log_incoming_response(synapse)

            # Return the updated synapse object after deserializing if requested
            return synapse

    def process_server_response(
        self, status, _, json_response: dict, local_synapse: Simulation
    ):
        # Check if the server responded with a successful status code
        if status == 200:
            # If the response is successful, overwrite local synapse state with
            # server's state only if the protocol allows mutation. To prevent overwrites,
            # the protocol must set Frozen = True
            server_synapse = local_synapse.__class__(**json_response)
            for key in local_synapse.model_dump().keys():
                try:
                    # Set the attribute in the local synapse from the corresponding
                    # attribute in the server synapse
                    setattr(local_synapse, key, getattr(server_synapse, key))
                except Exception:
                    # Ignore errors during attribute setting
                    pass
        else:
            # If the server responded with an error, update the local synapse state
            if local_synapse.axon is None:
                local_synapse.axon = bt.TerminalInfo()
            local_synapse.axon.status_code = status
            local_synapse.axon.status_message = json_response.get("message")

        # Update the status code and status message of the dendrite to match the axon
        local_synapse.dendrite.status_code = local_synapse.axon.status_code  # type: ignore
        local_synapse.dendrite.status_message = local_synapse.axon.status_message  # type: ignore

    def log_exception(self, exception: Exception):
        log_exception(exception)


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
            httpx.RemoteProtocolError,
            ValidationError,
        ),
    ):
        bt.logging.debug(f"{error_type}#{error_id}: {exception}")
    else:
        bt.logging.error(f"{error_type}#{error_id}: {exception}")
        traceback.print_exc(file=sys.stderr)
