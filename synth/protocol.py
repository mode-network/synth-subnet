# The MIT License (MIT)
# Copyright © 2023 Yuma Rao
# TODO(developer): Set your name
# Copyright © 2023 <your name>

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

import base64
import json
import typing

import bittensor as bt


from synth.simulation_input import SimulationInput

# This is the protocol for the miner and validator interaction.
# It is a simple request-response protocol where the validator sends a request
# to the miner, and the miner responds with a simulation_output response.


class Simulation(bt.Synapse):
    """
    A synth protocol representation which uses bt.Synapse as its base.
    This protocol helps in handling simulation_input request and response communication between
    the miner and the validator.

    Attributes:
    - simulation_input: An integer value representing the input request sent by the validator.
    - simulation_output: An optional integer value which, when filled, represents the response from the miner.
    """

    # Required request input, filled by sending dendrite caller.
    simulation_input: SimulationInput

    # Optional request output, filled by receiving axon.
    simulation_output: typing.Optional[
        typing.List[typing.List[typing.Dict[str, typing.Union[str, float]]]]
    ] = None

    def deserialize(self) -> typing.Optional[list]:
        """
        Deserialize simulation output. This method retrieves the response from
        the miner in the form of simulation_output, deserializes it and returns it
        as the output of the dendrite.query() call.
        """
        return self.simulation_output

    def parse_headers_to_inputs(cls, headers: dict) -> dict:
        """
        Interprets and transforms a given dictionary of headers into a structured dictionary, facilitating the
        reconstruction of Synapse objects.

        This method is essential for parsing network-transmitted
        data back into a Synapse instance, ensuring data consistency and integrity.

        Process:

        1. Separates headers into categories based on prefixes (``axon``, ``dendrite``, etc.).
        2. Decodes and deserializes ``input_obj`` headers into their original objects.
        3. Assigns simple fields directly from the headers to the input dictionary.

        Example::

            received_headers = {
                'bt_header_axon_address': '127.0.0.1',
                'bt_header_dendrite_port': '8080',
                # Other headers...
            }
            inputs = Synapse.parse_headers_to_inputs(received_headers)
            # inputs now contains a structured representation of Synapse properties based on the headers

        Note:
            This is handled automatically when calling :func:`Synapse.from_headers(headers)` and does not need to be
                called directly.

        Args:
            headers (dict): The headers dictionary to parse.

        Returns:
            dict: A structured dictionary representing the inputs for constructing a Synapse instance.
        """

        # Initialize the input dictionary with empty sub-dictionaries for 'axon' and 'dendrite'
        inputs_dict: dict[str, typing.Union[dict, typing.Optional[str]]] = {
            "axon": {},
            "dendrite": {},
        }

        # Iterate over each item in the headers
        for key, value in headers.items():
            # Handle 'axon' headers
            if "bt_header_axon_" in key:
                try:
                    new_key = key.split("bt_header_axon_")[1]
                    axon_dict = typing.cast(dict, inputs_dict["axon"])
                    axon_dict[new_key] = value
                except Exception as e:
                    bt.logging.error(
                        f"Error while parsing 'axon' header {key}: {str(e)}"
                    )
                    continue
            # Handle 'dendrite' headers
            elif "bt_header_dendrite_" in key:
                try:
                    new_key = key.split("bt_header_dendrite_")[1]
                    dendrite_dict = typing.cast(dict, inputs_dict["dendrite"])
                    dendrite_dict[new_key] = value
                except Exception as e:
                    bt.logging.error(
                        f"Error while parsing 'dendrite' header {key}: {e}"
                    )
                    continue
            # Handle 'input_obj' headers
            elif "bt_header_input_obj" in key:
                try:
                    new_key = key.split("bt_header_input_obj_")[1]
                    # Skip if the key already exists in the dictionary
                    if new_key in inputs_dict:
                        continue
                    # Decode and load the serialized object
                    inputs_dict[new_key] = json.loads(
                        base64.b64decode(value.encode()).decode("utf-8")
                    )
                except json.JSONDecodeError as e:
                    bt.logging.error(
                        f"Error while json decoding 'input_obj' header {key}: {e}"
                    )
                    continue
                except Exception as e:
                    bt.logging.error(
                        f"Error while parsing 'input_obj' header {key}: {e}"
                    )
                    continue

        # Assign the remaining known headers directly
        inputs_dict["timeout"] = headers.get("timeout", None)
        inputs_dict["name"] = headers.get("name", None)
        inputs_dict["header_size"] = headers.get("header_size", None)
        inputs_dict["total_size"] = headers.get("total_size", None)
        inputs_dict["computed_body_hash"] = headers.get(
            "computed_body_hash", None
        )

        return inputs_dict
