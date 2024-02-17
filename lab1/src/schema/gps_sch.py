from marshmallow import Schema,fields
from domain.gps import Gps

class GpsSch(Schema):
    long=fields.Number()
    lat=fields.Number()