def strip(string): return ''.join([c if 0 < ord(c) < 128 else ' ' for c in string])
