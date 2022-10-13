"""Helper functions for conforming identifiers."""
import re
from string import ascii_lowercase, digits


def snakecase(string: str) -> str:
    """Convert string into snake case.

    Args:
        string: String to convert.

    Returns:
        string: Snake cased string.
    """
    string = re.sub(r"[\-\.\s]", "_", string)
    string = (
        string[0].lower()
        + re.sub(
            r"[A-Z]", lambda matched: "_" + str(matched.group(0).lower()), string[1:]
        )
        if string
        else string
    )
    return re.sub(r"_{2,}", "_", string).rstrip("_")


def replace_leading_digit(string: str) -> str:
    """Replace leading numeric character with equivalent letter.

    Args:
        string: String to process.

    Returns:
        A modified string if original starts with a number,
        else the unmodified original.
    """
    if string[0] in digits:
        letters = list(ascii_lowercase)
        numbers = [int(d) for d in digits]
        digit_map = {n: letters[n] for n in numbers}
        return digit_map[int(string[0])] + string[1:]
    return string
