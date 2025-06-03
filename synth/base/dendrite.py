import sys
import traceback
from typing import Optional, Union
import time
import asyncio
import uuid
import aiohttp
import concurrent.futures


import bittensor as bt
from bittensor_wallet import Keypair, Wallet
import httpx
import uvloop


from synth.protocol import Simulation


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class SynthDendrite(bt.Dendrite):
    def __init__(self, wallet: Optional[Union[Wallet, Keypair]] = None):
        super().__init__(wallet=wallet)
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=16
        )

    async def forward(
        self,
        axons: Union[
            list[Union[bt.AxonInfo, bt.Axon]], Union[bt.AxonInfo, bt.Axon]
        ],
        synapse: Simulation,
        timeout: float = 12,
        deserialize: bool = True,
        run_async: bool = True,
        streaming: bool = False,
        use_thread_pool: bool = False,
    ) -> list[Simulation]:
        """
        Asynchronously sends requests to one or multiple Axons and collates their responses.

        This function acts as a bridge for sending multiple requests concurrently or sequentially
        based on the provided parameters. It checks the type of the target Axons, preprocesses
        the requests, and then sends them off. After getting the responses, it processes and
        collates them into a unified format.

        When querying an Axon that sends a single response, this function returns a Synapse object
        containing the response data. If multiple Axons are queried, a list of Synapse objects is
        returned, each containing the response from the corresponding Axon.

        For example::

            ...
            import bittensor
            wallet = bittensor.Wallet()                     # Initialize a wallet
            synapse = bittensor.Synapse(...)                # Create a synapse object that contains query data
            dendrite = bittensor.Dendrite(wallet = wallet)  # Initialize a dendrite instance
            netuid = ...                                    # Provide subnet ID
            metagraph = bittensor.Metagraph(netuid)         # Initialize a metagraph instance
            axons = metagraph.axons                         # Create a list of axons to query
            responses = await dendrite(axons, synapse)      # Send the query to all axons and await the responses

        When querying an Axon that sends back data in chunks using the Dendrite, this function
        returns an AsyncGenerator that yields each chunk as it is received. The generator can be
        iterated over to process each chunk individually.

        For example::

            ...
            dendrite = bittensor.Dendrite(wallet = wallet)
            async for chunk in dendrite.forward(axons, synapse, timeout, deserialize, run_async, streaming):
                # Process each chunk here
                print(chunk)

        Args:
            axons (Union[list[Union[bittensor.core.chain_data.axon_info.AxonInfo, bittensor.core.axon.Axon]],
                Union[bittensor.core.chain_data.axon_info.AxonInfo, bittensor.core.axon.Axon]]): The target Axons to
                send requests to. Can be a single Axon or a list of Axons.
            synapse (bittensor.core.synapse.Synapse): The Synapse object encapsulating the data. Defaults to a new
                :func:`Synapse` instance.
            timeout (float): Maximum duration to wait for a response from an Axon in seconds. Defaults to ``12.0``.
            deserialize (bool): Determines if the received response should be deserialized. Defaults to ``True``.
            run_async (bool): If ``True``, sends requests concurrently. Otherwise, sends requests sequentially.
                Defaults to ``True``.
            streaming (bool): Indicates if the response is expected to be in streaming format. Defaults to ``False``.

        Returns:
            Union[AsyncGenerator, bittensor.core.synapse.Synapse, list[bittensor.core.synapse.Synapse]]: If a single
                `Axon` is targeted, returns its response.
            If multiple Axons are targeted, returns a list of their responses.
        """
        is_list = True
        # If a single axon is provided, wrap it in a list for uniform processing
        if not isinstance(axons, list):
            is_list = False
            axons = [axons]

        # Check if synapse is an instance of the StreamingSynapse class or if streaming flag is set.
        is_streaming_subclass = issubclass(
            synapse.__class__, bt.StreamingSynapse
        )
        if streaming != is_streaming_subclass:
            bt.logging.warning(
                f"Argument streaming is {streaming} while issubclass(synapse, StreamingSynapse) is {synapse.__class__.__name__}. This may cause unexpected behavior."
            )
        streaming = is_streaming_subclass or streaming

        async def query_all_axons(
            is_stream: bool,
        ):
            """
            Handles the processing of requests to all targeted axons, accommodating both streaming and non-streaming responses.

            This function manages the concurrent or sequential dispatch of requests to a list of axons.
            It utilizes the ``is_stream`` parameter to determine the mode of response handling (streaming
            or non-streaming). For each axon, it calls ``single_axon_response`` and aggregates the responses.

            Args:
                is_stream (bool): Flag indicating whether the axon responses are expected to be streamed.
                If ``True``, responses are handled in streaming mode.

            Returns:
                list[Union[AsyncGenerator, bittensor.core.synapse.Synapse, bittensor.core.stream.StreamingSynapse]]:
                    A list containing the responses from each axon. The type of each response depends on the streaming
                    mode and the type of synapse used.
            """

            async def single_axon_response(
                target_axon: Union[bt.AxonInfo, bt.Axon],
            ) -> Simulation:
                """
                Manages the request and response process for a single axon, supporting both streaming and non-streaming modes.

                This function is responsible for initiating a request to a single axon. Depending on the ``is_stream``
                flag, it either uses ``call_stream`` for streaming responses or ``call`` for standard responses. The
                function handles the response processing, catering to the specifics of streaming or non-streaming data.

                Args:
                    target_axon (Union[bittensor.core.chain_data.axon_info.AxonInfo, bittensor.core.axon.Axon): The
                        target axon object to which the request is to be sent. This object contains the necessary
                        information like IP address and port to formulate the request.

                Returns:
                    Union[AsyncGenerator, bittensor.core.synapse.Synapse, bittensor.core.stream.StreamingSynapse]: The
                        response from the targeted axon. In streaming mode, an AsyncGenerator is returned, yielding data
                        chunks. In non-streaming mode, a Synapse or StreamingSynapse object is returned containing the
                        response.
                """
                if is_stream:
                    # If in streaming mode, return the async_generator
                    return self.call_stream(
                        target_axon=target_axon,
                        synapse=synapse.model_copy(),  # type: ignore
                        timeout=timeout,
                        deserialize=deserialize,
                    )
                else:
                    # If not in streaming mode, simply call the axon and get the response.
                    return await self.call(
                        target_axon=target_axon,
                        synapse=synapse.model_copy(),  # type: ignore
                        timeout=timeout,
                        deserialize=deserialize,
                    )

            # If run_async flag is False, get responses one by one.
            # If run_async flag is True, get responses concurrently using asyncio.gather().
            if not run_async:
                return [
                    await single_axon_response(target_axon)
                    for target_axon in axons
                ]  # type: ignore

            if use_thread_pool:
                with self.thread_pool as executor:
                    loop = asyncio.get_event_loop()
                    tasks = [
                        loop.run_in_executor(
                            executor, lambda: single_axon_response(target_axon)
                        )
                        for target_axon in axons
                    ]
                    results = await asyncio.gather(*tasks)

                return results

            return await asyncio.gather(
                *(single_axon_response(target_axon) for target_axon in axons)
            )  # type: ignore

        # Get responses for all axons.
        responses = await query_all_axons(streaming)
        # Return the single response if only one axon was targeted, else return all responses
        return responses[0] if len(responses) == 1 and not is_list else responses  # type: ignore

    async def call(
        self,
        target_axon: Union[bt.AxonInfo, bt.Axon],
        synapse: Simulation,
        timeout: float = 12.0,
        deserialize: bool = True,
    ) -> Simulation:
        """
        Asynchronously sends a request to a specified Axon and processes the response.

        This function establishes a connection with a specified Axon, sends the encapsulated data through the Synapse
        object, waits for a response, processes it, and then returns the updated Synapse object.

        Args:
            target_axon (Union[bittensor.core.chain_data.axon_info.AxonInfo, bittensor.core.axon.Axon]): The target Axon
                to send the request to.
            synapse (bittensor.core.synapse.Synapse): The Synapse object encapsulating the data. Defaults to a new
                :func:`Synapse` instance.
            timeout (float): Maximum duration to wait for a response from the Axon in seconds. Defaults to ``12.0``.
            deserialize (bool): Determines if the received response should be deserialized. Defaults to ``True``.

        Returns:
            bittensor.core.synapse.Synapse: The Synapse object, updated with the response data from the Axon.
        """

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

            async with httpx.AsyncClient(
                http2=True, limits=httpx.Limits(max_connections=None)
            ) as client:
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
        self, status, headers, json_response: dict, local_synapse: Simulation
    ):
        """
        Processes the server response, updates the local synapse state with the server's state and merges headers set
        by the server.

        Args:
            json_response (dict): The parsed JSON response from the server.
            local_synapse (bittensor.core.synapse.Synapse): The local synapse object to be updated.

        Raises:
            None: But errors in attribute setting are silently ignored.
        """
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

        # Extract server headers and overwrite None values in local synapse headers
        server_headers = bt.Synapse.from_headers(headers)  # type: ignore

        # Merge dendrite headers
        local_synapse.dendrite.__dict__.update(
            {
                **local_synapse.dendrite.model_dump(exclude_none=True),  # type: ignore
                **server_headers.dendrite.model_dump(exclude_none=True),  # type: ignore
            }
        )

        # Merge axon headers
        local_synapse.axon.__dict__.update(
            {
                **local_synapse.axon.model_dump(exclude_none=True),  # type: ignore
                **server_headers.axon.model_dump(exclude_none=True),  # type: ignore
            }
        )

        # Update the status code and status message of the dendrite to match the axon
        local_synapse.dendrite.status_code = local_synapse.axon.status_code  # type: ignore
        local_synapse.dendrite.status_message = local_synapse.axon.status_message  # type: ignore

    def log_exception(self, exception: Exception):
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
            ),
        ):
            bt.logging.debug(f"{error_type}#{error_id}: {exception}")
        else:
            bt.logging.error(f"{error_type}#{error_id}: {exception}")
            traceback.print_exc(file=sys.stderr)
