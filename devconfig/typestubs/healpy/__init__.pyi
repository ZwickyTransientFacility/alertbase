from typing import List, Tuple, Optional
import numpy as np

def order2nside(order: int) -> int: ...

def query_disc(nside: int,
               vec: Tuple[float, float, float],
               radius: float,
               inclusive: bool = False,
               fact: int = 4,
               nest: bool = False,
               buff: Optional[np.ndarray[np.int64]]=None
) -> np.ndarray[np.int64]: ...
