"""Dependency-free numeric contracts shared by Module 1 ingestion and output."""


INT32_MAX = 2**31 - 1


def validate_cart_position_range(minimum: int, maximum: int) -> tuple[int, int]:
    """Reject cart-position bounds that cannot be represented as positive int32."""
    if (
        type(minimum) is not int
        or type(maximum) is not int
        or minimum < 1
        or maximum < minimum
        or maximum > INT32_MAX
    ):
        raise ValueError("add_to_cart_order values must be positive int32 integers")
    return minimum, maximum
