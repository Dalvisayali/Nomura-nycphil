from db_utils import Base

from sqlalchemy import Column, Integer, String, Date, ForeignKey
from sqlalchemy.orm import relationship

class Nycphil(Base):
    __tablename__ = 'nycphil'

    id = Column(String, primary_key=True)
    season = Column(String)
    orchestra = Column(String)
    program_id = Column(Integer)

class Concert(Base):
    __tablename__ = 'concerts'

    concert_id = Column(Integer, primary_key=True)
    concert_date = Column(String)
    concert_event_type = Column(String)
    concert_venue = Column(String)
    concert_location = Column(String)
    concert_time = Column(String)
    nycphil_id = Column(String, ForeignKey('nycphil.id'))

class Work(Base):
    __tablename__ = 'works'

    work_id = Column(Integer, primary_key=True)
    work_work_title = Column(String)
    work_conductor_name = Column(String)
    work_composer_name = Column(String)
    work_movement = Column(String)
    work_interval = Column(String)
    nycphil_id = Column(String, ForeignKey('nycphil.id'))

class Soloist(Base):
    __tablename__ = 'soloists'

    soloist_id = Column(Integer, primary_key=True)
    soloist_name = Column(String)
    soloist_roles = Column(String)
    soloist_instrument = Column(String)
