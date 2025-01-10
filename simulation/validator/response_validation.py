def validate_responses(
    response
) -> bool:
    """
    Validate responses from miners.

    Return False if response is incorrect.
    """
    if response is None or len(response) == 0:
        return False

    return True
