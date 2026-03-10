import pandas as pd

from synth.validator.moving_average import apply_per_asset_coefficients


def test_zero_coefficients_no_nan():
    """When all assets have coefficient 0, scores should be 0, not NaN."""
    df = pd.DataFrame(
        {
            "asset": ["SPYX", "NVDAX", "SPYX"],
            "prompt_score_v3": [0.5, 0.3, 0.7],
        }
    )
    result = apply_per_asset_coefficients(df)
    assert not result.isna().any(), f"Got NaN: {result.tolist()}"
    assert (result == 0.0).all()


def test_normal_coefficients_unchanged():
    """Normal assets with non-zero coefficients still work."""
    df = pd.DataFrame(
        {
            "asset": ["BTC", "ETH"],
            "prompt_score_v3": [0.5, 0.3],
        }
    )
    result = apply_per_asset_coefficients(df)
    assert not result.isna().any()
    assert result.sum() > 0


def test_mixed_zero_and_nonzero_coefficients():
    """Mix of zero-coef and nonzero-coef assets."""
    df = pd.DataFrame(
        {
            "asset": ["BTC", "SPYX"],
            "prompt_score_v3": [0.5, 0.3],
        }
    )
    result = apply_per_asset_coefficients(df)
    assert not result.isna().any()
    # BTC has non-zero coef so total should be > 0
    assert result.sum() > 0
