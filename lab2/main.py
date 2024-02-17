import asyncio
import json
from typing import Set, Dict, List, Any
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Body
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Float,
    DateTime,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select

#personal additions
from sqlalchemy import insert,delete,update
from sqlalchemy.sql.expression import func

from datetime import datetime
from pydantic import BaseModel, field_validator
from config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)

# FastAPI app setup
app = FastAPI()
# SQLAlchemy setup
DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
metadata = MetaData()
# Define the ProcessedAgentData table
processed_agent_data = Table(
    "processed_agent_data",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("road_state", String),
    Column("user_id", Integer),
    Column("x", Float),
    Column("y", Float),
    Column("z", Float),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("timestamp", DateTime),
)

# SQLAlchemy model
class ProcessedAgentDataInDB(BaseModel):
    id: int
    road_state: str
    user_id: int
    x: float
    y: float
    z: float
    latitude: float
    longitude: float
    timestamp: datetime

class DataRow:
    pass

SessionLocal = sessionmaker(bind=engine)

# FastAPI models
class AccelerometerData(BaseModel):
    x: float
    y: float
    z: float


class GpsData(BaseModel):
    latitude: float
    longitude: float


class AgentData(BaseModel):
    user_id: int
    accelerometer: AccelerometerData
    gps: GpsData
    timestamp: datetime

    @classmethod
    @field_validator("timestamp", mode="before")
    def check_timestamp(cls, value):
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value)
        except (TypeError, ValueError):
            raise ValueError(
                "Invalid timestamp format. Expected ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ)."
            )


class ProcessedAgentData(BaseModel):
    road_state: str
    agent_data: AgentData




# WebSocket subscriptions
subscriptions: Dict[int, Set[WebSocket]] = {}


# FastAPI WebSocket endpoint
@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int):
    await websocket.accept()
    if user_id not in subscriptions:
        subscriptions[user_id] = set()
    subscriptions[user_id].add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        subscriptions[user_id].remove(websocket)


# Function to send data to subscribed users
async def send_data_to_subscribers(user_id: int, data):
    if user_id in subscriptions:
        for websocket in subscriptions[user_id]:
            await websocket.send_json(json.dumps(data))


# FastAPI CRUDL endpoints
def convert_base_to_db_entity(base:ProcessedAgentData,id:int)->ProcessedAgentDataInDB:
    res = ProcessedAgentDataInDB(id=id,
                                 road_state = base.road_state,
    user_id = base.agent_data.user_id,
    x = base.agent_data.accelerometer.x,
    y = base.agent_data.accelerometer.y,
    z = base.agent_data.accelerometer.z,
    latitude = base.agent_data.gps.latitude,
    longitude = base.agent_data.gps.longitude,
    timestamp = base.agent_data.timestamp)
    return res


@app.post("/processed_agent_data/")
async def create_processed_agent_data(data: List[ProcessedAgentData]):
    # Insert data to database
    c_session = SessionLocal()  
    for row in data:
        max_ind = c_session.query(func.max(processed_agent_data.c.id))
        max_ind = list(c_session.execute(max_ind))[0][0]+1
        db_item = convert_base_to_db_entity(ProcessedAgentData(**row.model_dump()),max_ind)
        db_item = insert(processed_agent_data).values(**db_item.model_dump())
        c_session.execute(db_item)
        c_session.commit()
        # Send data to subscribers
        await send_data_to_subscribers(row.agent_data.user_id,db_item)
    return data


@app.get(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def read_processed_agent_data(processed_agent_data_id: int):
    c_session = SessionLocal()
    db_item = select(processed_agent_data).filter(processed_agent_data.c.id == processed_agent_data_id)
    if db_item is None:
        raise HTTPException(status_code=404, detail="Item not found")
    db_item = list(c_session.execute(db_item))[0]
    print(db_item)
    field_names = ProcessedAgentDataInDB.model_fields.keys()
    kwargs = dict(zip(field_names, db_item))
    print(kwargs)
    return ProcessedAgentDataInDB.model_validate(kwargs)


@app.get("/processed_agent_data/", response_model=list[ProcessedAgentDataInDB])
def list_processed_agent_data():
    c_session = SessionLocal()
    # Get list of data 
    return c_session.query(processed_agent_data).all()


@app.put(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def update_processed_agent_data(processed_agent_data_id: int, data: ProcessedAgentData):
    # Update data
    c_session = SessionLocal()
    data = convert_base_to_db_entity(data,processed_agent_data_id)
    db_item = update(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id).values(data.model_dump()).returning(processed_agent_data.c)
    db_item = list(c_session.execute(db_item))[0]
    c_session.commit()
    return db_item


@app.delete(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def delete_processed_agent_data(processed_agent_data_id: int):
    # Delete by id
    c_session = SessionLocal()
    db_item = delete(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id).returning(processed_agent_data.c)
    db_item = list(c_session.execute(db_item))[0]
    c_session.commit()
    return db_item


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
