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

import typing

import bittensor as bt
import numpy as np


from simulation.simulation_input import SimulationInput

# This is the protocol for the simulation miner and validator.
# It is a simple request-response protocol where the validator sends a request
# to the miner, and the miner responds with a simulation_output response.

# ---- miner ----
# Example usage:
#   def simulation( synapse: Simulation ) -> Simulation:
#       synapse.simulation_output = synapse.simulation_input
#       return synapse
#   axon = bt.axon().attach( simulation ).serve(netuid=...).start()

# ---- validator ---
# Example usage:
#   dendrite = bt.Dendrite()
#   simulation_output = dendrite.query( Simulation( simulation_input = SimulationInput(time_increment=60, time_length=3600, num_simulations=10) ) )
#   assert simulation_output == np.array([[1.1, 2.2], [3.3, 4.4]], dtype=np.float64)


class Simulation(bt.Synapse):
    """
    A simulation protocol representation which uses bt.Synapse as its base.
    This protocol helps in handling simulation_input request and response communication between
    the miner and the validator.

    Attributes:
    - simulation_input: An integer value representing the input request sent by the validator.
    - simulation_output: An optional integer value which, when filled, represents the response from the miner.
    """

    # Required request input, filled by sending dendrite caller.
    simulation_input: SimulationInput

    # Optional request output, filled by receiving axon.
    simulation_output: typing.Optional[typing.List[typing.Dict[str, typing.Union[str, float]]]] = None

    def deserialize(self) -> []:
        """
        Deserialize the simulation output. This method retrieves the response from
        the miner in the form of simulation_output, deserializes it and returns it
        as the output of the dendrite.query() call.

        Returns:
        - ndarray[Any, dtype]: The deserialized response, which in this case is the value of simulation_output.

        Example:
        Assuming a Simulation instance has a simulation_output value of 5:
        >>> simulation_instance = Simulation(simulation_input=SimulationInput(time_increment=60, time_length=3600, num_simulations=10))
        >>> simulation_instance.simulation_output = np.array([[1.1, 2.2], [3.3, 4.4]], dtype=np.float64)
        >>> simulation_instance.deserialize()
        [[1.1 2.2]
        [3.3 4.4]]
        """
        return self.simulation_output
