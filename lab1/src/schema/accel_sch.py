from marshmallow import Schema,fields
from domain.accel import Accel

class AccelSch(Schema):
    x=fields.Int()
    y=fields.Int()
    z=fields.Int()