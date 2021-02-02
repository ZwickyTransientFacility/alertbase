from typing import List, Tuple, Optional, Any
import numpy as np

def order2nside(order: int) -> int: ...

def vec2pix(nside: int, x: float, y: float, z: float, nest: bool=False) -> int: ...

def query_disc(nside: int,
               vec: Tuple[float, float, float],
               radius: float,
               inclusive: bool = False,
               fact: int = 4,
               nest: bool = False,
               buff: Optional[np.ndarray]=None
) -> np.ndarray: ...
