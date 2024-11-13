from __future__ import annotations

from pathlib import Path
from enum import IntEnum

class CompressionMethod(IntEnum):
    UNK = 0 # Unknown or no compression 
    GZ = 1 # GZ

def check_compression(path: str | Path) -> CompressionMethod:

    file = open(path, "rb")
    if file.read(2) == b'\x1f\x8b':
        ctype = CompressionMethod.GZ
    else:
        ctype = CompressionMethod.UNK

    file.close()
    return ctype
    

