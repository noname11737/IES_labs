from marshmallow import Schema,fields
from schema.accel_sch import AccelSch
from schema.gps_sch import GpsSch
from domain.aggreg import AggData

class AggSch(Schema):
    accel = fields.Nested(AccelSch)
    gps = fields.Nested(GpsSch)
    time = fields.DateTime('iso')