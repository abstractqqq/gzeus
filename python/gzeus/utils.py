from __future__ import annotations

from pathlib import Path
from enum import IntEnum

class CompressionMethod(IntEnum):
    UNK = 0 # Unknown or no compression 
    GZ = 1 # GZ


def _get_compression_method(b: bytes | str) -> CompressionMethod:

    if isinstance(b, str):
        _bytes = b.encode() # UTF-8
    else:
        _bytes = b

    if len(_bytes) < 2:
        return CompressionMethod.UNK
    else:
        if _bytes[:2] == b'\x1f\x8b':
            return CompressionMethod.GZ
        else:
            return CompressionMethod.UNK


def get_compression_method_local(path: str | Path) -> CompressionMethod:
    file = open(path, "rb")
    ctype = _get_compression_method(file.read(2))
    file.close()
    return ctype
    

