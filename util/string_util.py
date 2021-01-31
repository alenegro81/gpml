def strip(string: str) -> str:
    return ''.join([c if 0 < ord(c) < 128 else ' ' for c in string])
