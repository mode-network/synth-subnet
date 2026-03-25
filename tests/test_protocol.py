from pydantic import ValidationError

from synth.protocol import invalid_to_none


def mock_handler(v):
    """Simulates Pydantic validation that succeeds."""
    if not isinstance(v, tuple):
        raise ValidationError.from_exception_data(
            title="test",
            line_errors=[],
        )
    return v


class TestInvalidToNone:
    def test_valid_data_passes_through(self):
        """Valid data should be returned as-is."""
        valid = (1, [1.0, 2.0], [3.0, 4.0])
        result = invalid_to_none(valid, mock_handler)
        assert result == valid

    def test_invalid_data_returns_none(self):
        """Invalid data should return None, not the raw invalid value.

        This was the bug: previously returned the raw invalid value `v`,
        allowing malformed miner responses to bypass validation.

        Example: a miner sends "garbage_string" as simulation_output.
        Before fix: invalid_to_none returned "garbage_string"
        After fix:  invalid_to_none returns None
        """
        result = invalid_to_none("not_a_tuple", mock_handler)
        assert result is None, (
            "invalid_to_none must return None for invalid data, "
            "not the raw value"
        )

    def test_invalid_dict_returns_none(self):
        """Malformed dict input should return None."""
        result = invalid_to_none({"malicious": "data"}, mock_handler)
        assert result is None

    def test_invalid_list_returns_none(self):
        """List instead of tuple should return None."""
        result = invalid_to_none([1, 2, 3], mock_handler)
        assert result is None

    def test_none_input_returns_none(self):
        """None input that fails validation should return None."""
        result = invalid_to_none(None, mock_handler)
        assert result is None
