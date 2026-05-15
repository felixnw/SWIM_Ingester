from fastapi import FastAPI, HTTPException, Depends, Body
from sqlalchemy import (
    create_engine,
    Column,
    String,
    DateTime,
    desc,
    case,
    or_
)
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List, Dict
from datetime import datetime, timedelta

# =========================================================
# Database Setup
# =========================================================

DATABASE_URL = "postgresql://user:password@localhost/dbname"

engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

Base = declarative_base()


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


# =========================================================
# Elasticsearch-Compatible Request Models
# =========================================================

class TermQuery(BaseModel):
    flight_id: str


class TermsQuery(BaseModel):
    latest_status: List[str]


class RangeLatestETD(BaseModel):
    gte: Optional[str] = None


class RangeQuery(BaseModel):
    latest_etd: RangeLatestETD


class MustNotQuery(BaseModel):
    range: RangeQuery


class InnerBoolQuery(BaseModel):
    must_not: MustNotQuery


class FilterItem(BaseModel):
    term: Optional[TermQuery] = None
    terms: Optional[TermsQuery] = None
    bool: Optional[InnerBoolQuery] = None


class BoolQuery(BaseModel):
    filter: List[FilterItem]


class QueryModel(BaseModel):
    bool: BoolQuery


class SortOrder(BaseModel):
    order: str = "desc"


class SearchRequest(BaseModel):
    size: Optional[int] = 10
    sort: Optional[List[Dict[str, SortOrder]]] = None
    query: QueryModel


# =========================================================
# Elasticsearch-Compatible Response Models
# =========================================================

class FlightSource(BaseModel):
    flight_id: str

    registration: Optional[str] = None

    operator: Optional[str] = None
    major: Optional[str] = None

    dep_airport: Optional[str] = None
    arr_airport: Optional[str] = None

    aircraft_type: Optional[str] = None

    latest_status: Optional[str] = None

    latest_etd: Optional[datetime] = None
    latest_eta: Optional[datetime] = None

    original_etd: Optional[datetime] = None
    original_eta: Optional[datetime] = None

    model_config = ConfigDict(
        from_attributes=True
    )


class FlightHit(BaseModel):
    source: FlightSource = Field(alias="_source")

    model_config = ConfigDict(
        populate_by_name=True
    )


class HitsContainer(BaseModel):
    hits: List[FlightHit]


class ESResponse(BaseModel):
    hits: HitsContainer


# =========================================================
# DB Dependency
# =========================================================

def get_db():
    db = SessionLocal()

    try:
        yield db
    finally:
        db.close()


# =========================================================
# App
# =========================================================

app = FastAPI(title="SWIM-Compatible Flight API")


# =========================================================
# Endpoint
# =========================================================

@app.post(
    "/swim-combined-flights/_search",
    response_model=ESResponse,
    response_model_by_alias=True
)
def search_flights(
    body: SearchRequest = Body(...),
    db: Session = Depends(get_db)
):
    """
    Elasticsearch-style compatible endpoint.

    Supports:
    - term.flight_id
    - terms.latest_status
    - must_not.range.latest_etd.gte
    - size
    - multi-result responses
    """

    # -----------------------------------------------------
    # Extract flight_id
    # -----------------------------------------------------

    flight_id = None

    for f in body.query.bool.filter:
        if f.term and f.term.flight_id:
            flight_id = f.term.flight_id.upper()
            break

    if not flight_id:
        raise HTTPException(
            status_code=400,
            detail="Missing flight_id"
        )

    # -----------------------------------------------------
    # Extract statuses
    # -----------------------------------------------------

    statuses = ["ACTIVE", "PLANNED", "PROPOSED"]

    for f in body.query.bool.filter:
        if f.terms and f.terms.latest_status:
            statuses = f.terms.latest_status
            break

    # -----------------------------------------------------
    # Parse dynamic ETD exclusion
    # Supports:
    # now+6h
    # now+2h
    # etc.
    # -----------------------------------------------------

    etd_cutoff = None

    for f in body.query.bool.filter:

        if (
            f.bool
            and f.bool.must_not
            and f.bool.must_not.range
            and f.bool.must_not.range.latest_etd
            and f.bool.must_not.range.latest_etd.gte
        ):

            gte_value = (
                f.bool.must_not.range.latest_etd.gte
            )

            if (
                gte_value.startswith("now+")
                and gte_value.endswith("h")
            ):

                hours = int(
                    gte_value
                    .replace("now+", "")
                    .replace("h", "")
                )

                etd_cutoff = (
                    datetime.utcnow()
                    + timedelta(hours=hours)
                )

            break

    # Default fallback
    if etd_cutoff is None:
        etd_cutoff = (
            datetime.utcnow()
            + timedelta(hours=6)
        )

    # -----------------------------------------------------
    # Base query
    # -----------------------------------------------------

    query = db.query(FlightDB).filter(
        FlightDB.callsign == flight_id,
        FlightDB.flight_status.in_(statuses),
        or_(
            FlightDB.updated_etd.is_(None),
            FlightDB.updated_etd < etd_cutoff
        )
    )

    # -----------------------------------------------------
    # Sorting
    #
    # ACTIVE + ENROUTE first
    # then newest ETD
    # -----------------------------------------------------

    priority_status = case(
        (
            FlightDB.flight_status.in_([
                "ACTIVE",
                "ENROUTE"
            ]),
            1
        ),
        else_=0
    )

    query = query.order_by(
        desc(priority_status),
        desc(FlightDB.updated_etd)
    )

    # -----------------------------------------------------
    # Size
    # -----------------------------------------------------

    size = body.size or 10

    query = query.limit(size)

    flights = query.all()

    # -----------------------------------------------------
    # Build ES-style response
    # -----------------------------------------------------

    formatted_hits = []

    for f in flights:

        source_data = FlightSource(
            flight_id=f.callsign,
            registration=f.tail_number,
            operator=f.operator,
            major=f.major,
            dep_airport=f.origin,
            arr_airport=f.destination,
            aircraft_type=f.aircraft_type,
            latest_status=f.flight_status,
            latest_etd=f.updated_etd,
            latest_eta=f.updated_eta,
            original_etd=f.original_etd,
            original_eta=f.original_eta
        )

        formatted_hits.append(
            FlightHit(
                source=source_data
            )
        )

    return ESResponse(
        hits=HitsContainer(
            hits=formatted_hits
        )
    )