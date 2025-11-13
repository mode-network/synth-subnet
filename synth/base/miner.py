# The MIT License (MIT)
# Copyright © 2023 Yuma Rao

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
# documentation files (the “Software”), to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies or substantial portions of
# the Software.

# THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
# THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

import time
import asyncio
import threading
import argparse
import traceback
from typing import Union


import bittensor as bt
from bittensor.core.axon import V_7_2_0
from bittensor.core.errors import SynapseDendriteNoneException
from bittensor_wallet import Keypair
from bittensor.utils.axon_utils import (
    allowed_nonce_window_ns,
    calculate_diff_seconds,
)


from synth.base.neuron import BaseNeuron
from synth.utils.config import add_miner_args


class BaseMinerNeuron(BaseNeuron):
    """
    Base class for Bittensor miners.
    """

    neuron_type: str = "MinerNeuron"

    @classmethod
    def add_args(cls, parser: argparse.ArgumentParser):
        super().add_args(parser)
        add_miner_args(cls, parser)

    def __init__(self, config=None):
        super().__init__(config=config)

        # Warn if allowing incoming requests from anyone.
        if not self.config.blacklist.force_validator_permit:
            bt.logging.warning(
                "You are allowing non-validators to send requests to your miner. This is a security risk."
            )
        if self.config.blacklist.allow_non_registered:
            bt.logging.warning(
                "You are allowing non-registered entities to send requests to your miner. This is a security risk."
            )
        # The axon handles request processing, allowing validators to send this miner requests.
        self.axon = bt.axon(
            wallet=self.wallet,
            config=self.config() if callable(self.config) else self.config,
        )

        # Attach determiners which functions are called when servicing a request.
        bt.logging.info("Attaching forward function to miner axon.")
        self.axon.attach(
            forward_fn=self.forward_miner,
            blacklist_fn=self.blacklist,
            priority_fn=self.priority,
            verify_fn=self.verify,
        )
        bt.logging.info(f"Axon created: {self.axon}")

        # Instantiate runners
        self.should_exit = False
        self.is_running = False
        self.thread: Union[threading.Thread, None] = None
        self.lock = asyncio.Lock()

    async def verify(self, synapse: bt.Synapse):
        """
        This method is used to verify the authenticity of a received message using a digital signature.

        It ensures that the message was not tampered with and was sent by the expected sender.

        The :func:`default_verify` method in the Bittensor framework is a critical security function within the
        Axon server. It is designed to authenticate incoming messages by verifying their digital
        signatures. This verification ensures the integrity of the message and confirms that it was
        indeed sent by the claimed sender. The method plays a pivotal role in maintaining the trustworthiness
        and reliability of the communication within the Bittensor network.

        Key Features
            Security Assurance
                The default_verify method is crucial for ensuring the security of the Bittensor network. By verifying
                digital signatures, it guards against unauthorized access and data manipulation.

            Preventing Replay Attacks
                The method checks for increasing nonce values, which is a vital
                step in preventing replay attacks. A replay attack involves an adversary reusing or
                delaying the transmission of a valid data transmission to deceive the receiver.
                The first time a nonce is seen, it is checked for freshness by ensuring it is
                within an acceptable delta time range.

            Authenticity and Integrity Checks
                By verifying that the message's digital signature matches
                its content, the method ensures the message's authenticity (it comes from the claimed
                sender) and integrity (it hasn't been altered during transmission).

            Trust in Communication
                This method fosters trust in the network communication. Neurons
                (nodes in the Bittensor network) can confidently interact, knowing that the messages they
                receive are genuine and have not been tampered with.

            Cryptographic Techniques
                The method's reliance on asymmetric encryption techniques is a
                cornerstone of modern cryptographic security, ensuring that only entities with the correct
                cryptographic keys can participate in secure communication.

        Args:
            synapse(bittensor.core.synapse.Synapse): bittensor request synapse.

        Raises:
            Exception: If the ``receiver_hotkey`` doesn't match with ``self.receiver_hotkey``.
            Exception: If the nonce is not larger than the previous nonce for the same endpoint key.
            Exception: If the signature verification fails.

        After successful verification, the nonce for the given endpoint key is updated.

        Note:
            The verification process assumes the use of an asymmetric encryption algorithm,
            where the sender signs the message with their private key and the receiver verifies the
            signature using the sender's public key.
        """
        # Build the keypair from the dendrite_hotkey
        if synapse.dendrite is not None:
            keypair = Keypair(ss58_address=synapse.dendrite.hotkey)

            # Build the signature messages.
            message = f"{synapse.dendrite.nonce}.{synapse.dendrite.hotkey}.{self.wallet.hotkey.ss58_address}.{synapse.dendrite.uuid}.{synapse.computed_body_hash}"

            # Build the unique endpoint key.
            endpoint_key = f"{synapse.dendrite.hotkey}:{synapse.dendrite.uuid}"

            # Requests must have nonces to be safe from replays
            if synapse.dendrite.nonce is None:
                raise Exception("Missing Nonce")

            # Newer nonce structure post v7.2
            if (
                synapse.dendrite.version is not None
                and synapse.dendrite.version >= V_7_2_0
            ):
                # If we don't have a nonce stored, ensure that the nonce falls within
                # a reasonable delta.
                current_time_ns = time.time_ns()
                allowed_window_ns = allowed_nonce_window_ns(
                    current_time_ns, synapse.timeout
                )

                if (
                    self.nonces.get(endpoint_key) is None
                    and synapse.dendrite.nonce <= allowed_window_ns
                ):
                    diff_seconds, allowed_delta_seconds = (
                        calculate_diff_seconds(
                            current_time_ns,
                            synapse.timeout,
                            synapse.dendrite.nonce,
                        )
                    )
                    raise Exception(
                        f"Nonce is too old: acceptable delta is {allowed_delta_seconds:.2f} seconds but request was {diff_seconds:.2f} seconds old"
                    )

                # If a nonce is stored, ensure the new nonce
                # is greater or equal to than the previous nonce
                if (
                    self.nonces.get(endpoint_key) is not None
                    and synapse.dendrite.nonce < self.nonces[endpoint_key]
                ):
                    raise Exception(
                        "Nonce is too old, a newer one was last processed"
                    )
            # Older nonce structure pre v7.2
            else:
                if (
                    self.nonces.get(endpoint_key) is not None
                    and synapse.dendrite.nonce < self.nonces[endpoint_key]
                ):
                    raise Exception(
                        "Nonce is too old, a newer one was last processed"
                    )

            if synapse.dendrite.signature and not keypair.verify(
                message, synapse.dendrite.signature
            ):
                raise Exception(
                    f"Signature mismatch with {message} and {synapse.dendrite.signature}"
                )

            # Success
            self.nonces[endpoint_key] = synapse.dendrite.nonce  # type: ignore
        else:
            raise SynapseDendriteNoneException(synapse=synapse)

    def run(self):
        """
        Initiates and manages the main loop for the miner on the Bittensor network. The main loop handles graceful shutdown on keyboard interrupts and logs unforeseen errors.

        This function performs the following primary tasks:
        1. Check for registration on the Bittensor network.
        2. Starts the miner's axon, making it active on the network.
        3. Periodically resynchronizes with the chain; updating the metagraph with the latest network state and setting weights.

        The miner continues its operations until `should_exit` is set to True or an external interruption occurs.
        During each epoch of its operation, the miner waits for new blocks on the Bittensor network, updates its
        knowledge of the network (metagraph), and sets its weights. This process ensures the miner remains active
        and up-to-date with the network's latest state.

        Note:
            - The function leverages the global configurations set during the initialization of the miner.
            - The miner's axon serves as its interface to the Bittensor network, handling incoming and outgoing requests.

        Raises:
            KeyboardInterrupt: If the miner is stopped by a manual interruption.
            Exception: For unforeseen errors during the miner's operation, which are logged for diagnosis.
        """

        # Check that miner is registered on the network.
        self.sync()

        # Serve passes the axon information to the network + netuid we are hosting on.
        # This will auto-update if the axon port of external ip have changed.
        bt.logging.info(
            f"Serving miner axon {self.axon} on network: {self.config.subtensor.chain_endpoint} with netuid: {self.config.netuid}"
        )
        self.axon.serve(netuid=self.config.netuid, subtensor=self.subtensor)

        # Start  starts the miner's axon, making it active on the network.
        self.axon.start()

        bt.logging.info(f"Miner starting at block: {self.block}")

        # This loop maintains the miner's operations until intentionally stopped.
        try:
            while not self.should_exit:
                while (
                    self.block - self.metagraph.last_update[self.uid]
                    < self.config.neuron.epoch_length
                ):
                    # Wait before checking again.
                    time.sleep(1)

                    # Check if we should exit.
                    if self.should_exit:
                        break

                # Sync metagraph and potentially set weights.
                self.sync()
                self.step += 1

        # If someone intentionally stops the miner, it'll safely terminate operations.
        except KeyboardInterrupt:
            self.axon.stop()
            bt.logging.success("Miner killed by keyboard interrupt.")
            exit()

        # In case of unforeseen errors, the miner will log the error and continue operations.
        except Exception:
            bt.logging.error(traceback.format_exc())

    def run_in_background_thread(self):
        """
        Starts the miner's operations in a separate background thread.
        This is useful for non-blocking operations.
        """
        if not self.is_running:
            bt.logging.debug("Starting miner in background thread.")
            self.should_exit = False
            self.thread = threading.Thread(target=self.run, daemon=True)
            self.thread.start()
            self.is_running = True
            bt.logging.debug("Started")

    def stop_run_thread(self):
        """
        Stops the miner's operations that are running in the background thread.
        """
        if self.is_running:
            bt.logging.debug("Stopping miner in background thread.")
            self.should_exit = True
            if self.thread is not None:
                self.thread.join(5)
            self.is_running = False
            bt.logging.debug("Stopped")

    def __enter__(self):
        """
        Starts the miner's operations in a background thread upon entering the context.
        This method facilitates the use of the miner in a 'with' statement.
        """
        self.run_in_background_thread()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Stops the miner's background operations upon exiting the context.
        This method facilitates the use of the miner in a 'with' statement.

        Args:
            exc_type: The type of the exception that caused the context to be exited.
                      None if the context was exited without an exception.
            exc_value: The instance of the exception that caused the context to be exited.
                       None if the context was exited without an exception.
            traceback: A traceback object encoding the stack trace.
                       None if the context was exited without an exception.
        """
        self.stop_run_thread()

    def resync_metagraph(self):
        """Resyncs the metagraph and updates the hotkeys and moving averages based on the new metagraph."""
        # bt.logging.info("resync_metagraph()")

        # Sync the metagraph.
        self.metagraph.sync(subtensor=self.subtensor)
