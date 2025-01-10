from datetime import datetime

from pydantic import BaseModel, Field


class SimulationInput(BaseModel):
    asset: str = Field(default="BTC", description="The asset to simulate.")
    start_time: str = Field(
        default=datetime.now().isoformat(),
        description="The start time of the simulation.",
    )
    time_increment: int = Field(
        default=300, description="Time increment in seconds."
    )
    time_length: int = Field(
        default=86400, description="Total time length in seconds."
    )
    num_simulations: int = Field(
        default=1, description="Number of simulation runs."
    )

    class Config:
        arbitrary_types_allowed = True
