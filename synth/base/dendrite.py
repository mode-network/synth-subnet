from typing import Optional, Union
import aiohttp
import concurrent.futures


import bittensor as bt
from bittensor_wallet import Keypair, Wallet


class SynthDendrite(bt.Dendrite):
    def __init__(self, wallet: Optional[Union[Wallet, Keypair]] = None):
        super().__init__(wallet=wallet)
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=16
        )

    async def forward(
        self,
        axons: Union[
            list[Union["AxonInfo", "Axon"]], Union["AxonInfo", "Axon"]
        ],
        synapse: "Synapse" = Synapse(),
        timeout: float = 12,
        deserialize: bool = True,
        run_async: bool = True,
        streaming: bool = False,
    ) -> list[
        Union["AsyncGenerator[Any, Any]", "Synapse", "StreamingSynapse"]
    ]:
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
        is_streaming_subclass = issubclass(synapse.__class__, StreamingSynapse)
        if streaming != is_streaming_subclass:
            logging.warning(
                f"Argument streaming is {streaming} while issubclass(synapse, StreamingSynapse) is {synapse.__class__.__name__}. This may cause unexpected behavior."
            )
        streaming = is_streaming_subclass or streaming

        async def query_all_axons(
            is_stream: bool,
        ) -> Union["AsyncGenerator[Any, Any]", "Synapse", "StreamingSynapse"]:
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
                target_axon: Union["AxonInfo", "Axon"],
            ) -> Union[
                "AsyncGenerator[Any, Any]", "Synapse", "StreamingSynapse"
            ]:
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
            if not run_async:
                return [
                    await single_axon_response(target_axon)
                    for target_axon in axons
                ]  # type: ignore
            # If run_async flag is True, get responses concurrently using asyncio.gather().
            return await asyncio.gather(
                *(single_axon_response(target_axon) for target_axon in axons)
            )  # type: ignore

        # Get responses for all axons.
        responses = await query_all_axons(streaming)
        # Return the single response if only one axon was targeted, else return all responses
        return responses[0] if len(responses) == 1 and not is_list else responses  # type: ignore

    async def call(
        self,
        target_axon: Union["AxonInfo", "Axon"],
        synapse: "Synapse" = Synapse(),
        timeout: float = 12.0,
        deserialize: bool = True,
    ) -> "Synapse":
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
            if isinstance(target_axon, Axon)
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
            async with (await self.session).post(
                url=url,
                headers=synapse.to_headers(),
                json=synapse.model_dump(),
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as response:
                # Extract the JSON response from the server
                json_response = await response.json()
                # Process the server response and fill synapse
                self.process_server_response(response, json_response, synapse)

            # Set process time and log the response
            synapse.dendrite.process_time = str(time.time() - start_time)  # type: ignore

        except Exception as e:
            synapse = self.process_error_message(synapse, request_name, e)

        finally:
            self._log_incoming_response(synapse)

            # Log synapse event history
            self.synapse_history.append(
                Synapse.from_headers(synapse.to_headers())
            )

            # Return the updated synapse object after deserializing if requested
            return synapse.deserialize() if deserialize else synapse

    def process_server_response(
        self,
        server_response: aiohttp.ClientResponse,
        json_response: dict,
        local_synapse: bt.Synapse,
    ):
        bt.logging.trace("skipping dendrite processing for synth dendrite")
