from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy import create_engine, Column, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
from typing import Optional
from datetime import datetime

# Database Setup
DATABASE_URL = "postgresql://user:password@localhost/dbname"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# 1. Database Model (SQLAlchemy)
class FlightDB(Base):
    __tablename__ = "flights"
    gufi = Column(String, primary_key=True)
    callsign = Column(String)
    tail_number = Column(String)
    operator = Column(String)
    major = Column(String)
    aircraft_type = Column(String)
    origin = Column(String)
    destination = Column(String)
    original_etd = Column(DateTime)
    updated_etd = Column(DateTime)
    original_eta = Column(DateTime)
    updated_eta = Column(DateTime)
    flight_status = Column(String)

# 2. Response Schema (Pydantic)
# This maps your DB columns to your requested API field names
class FlightResponse(BaseModel):
    flight_id: str
    registration: Optional[str]
    operator: Optional[str]
    major: Optional[str]
    dep_airport: Optional[str]
    arr_airport: Optional[str]
    aircraft_type: Optional[str]
    latest_status: Optional[str]
    latest_etd: Optional[datetime]
    latest_eta: Optional[datetime]
    original_etd: Optional[datetime]
    original_eta: Optional[datetime]

    class Config:
        from_attributes = True

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

app = FastAPI(title="Flight Tracking API")

# 3. The Endpoint
@app.get("/flights/{callsign}", response_model=FlightResponse)
def get_flight_by_callsign(callsign: str, db: Session = Depends(get_db)):
    # Query the DB using the indexed callsign column
    flight = db.query(FlightDB).filter(FlightDB.callsign == callsign.upper()).first()
    
    if not flight:
        raise HTTPException(status_code=404, detail="Flight not found")

    # Manually map the DB fields to the API fields
    return FlightResponse(
        flight_id=flight.gufi,
        registration=flight.tail_number,
        operator=flight.operator,
        major=flight.major,
        dep_airport=flight.origin,
        arr_airport=flight.destination,
        aircraft_type=flight.aircraft_type,
        latest_status=flight.flight_status,
        latest_etd=flight.updated_etd,
        latest_eta=flight.updated_eta,
        original_etd=flight.original_etd,
        original_eta=flight.original_eta
    )