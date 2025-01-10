def validate_responses(
    response
) -> bool:
    return response is None or len(response) == 0
