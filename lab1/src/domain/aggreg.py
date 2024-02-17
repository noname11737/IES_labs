from dataclasses import dataclass
from datetime import datetime

from domain.accel import Accel
from domain.gps import Gps

@dataclass
class AggData:
    accel:Accel
    gps:Gps
    time:datetime