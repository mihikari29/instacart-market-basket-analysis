"""Dependency-free numeric contracts shared by Module 1 ingestion and output."""


INT32_MAX = 2**31 - 1

#: Parquet codec for the durable Module 1 artifacts.
#:
#: Zstandard compresses the generated event feed about 40% smaller than
#: Snappy on the full dataset (measured on the 33.8M event feed) at roughly
#: the same write cost, which is paid once per feed. Spark reads Zstandard
#: Parquet natively, so the read path is unchanged. The codec is recorded in
#: the staging receipt's row counts and hashes, so a feed and its receipt
#: always describe the same bytes.
PARQUET_COMPRESSION = "zstd"


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
