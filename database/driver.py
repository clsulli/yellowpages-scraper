from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from settings.config import Config

engine = create_engine(Config.MSSQL_DATABASE_URI, echo=True)
Base = declarative_base(engine)


class Business(Base):
    __tablename__ = 'business'
    __table_args__ = {'autoload': True}


class BusinessLocation(Base):
    __tablename__ = 'business_location'
    __table_args__ = {'autoload': True}


class Location(Base):
    __tablename__ = 'location'
    __table_args__ = {'autoload': True}

    def __init__(self, city, state):
        self.city = city
        self.state = state


class BusinessCategory(Base):
    __tablename__ = 'business_category'
    __table_args__ = {'autoload': True}


class Category(Base):
    __tablename__ = 'category'
    __table_args__ = {'autoload': True}


def load_session():
    metadata = Base.metadata
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


def insert_location(city, state):
    session = load_session()
    location = Location(city=city, state=state)
    session.add(location)
    session.commit()
    session.close()


if __name__ == '__main__':
    session = load_session()
    res = session.query(Business).all()
