from synth.miner.simulations import generate_simulations


def test_generate_simulations():
    result = generate_simulations(
        asset="BTC",
        start_time="2025-02-04T00:00:00+00:00",
        time_increment=300,
        time_length=86400,
        num_simulations=100,
    )

    assert isinstance(result, list)
    assert len(result) == 100
    assert all(
        isinstance(sub_array, list) and len(sub_array) == 289
        for sub_array in result
    )
