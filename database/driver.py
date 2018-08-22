from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from settings.config import Config

engine = create_engine(Config.MSSQL_DATABASE_URI)
Base = declarative_base(engine)


class Business(Base):
    __tablename__ = 'business'
    __table_args__ = {'autoload': True}

    def __init__(self, name, phone, website, email):
        self.name = name
        self.phone = phone
        self.website = website
        self.email = email


class BusinessLocation(Base):
    __tablename__ = 'business_location'
    __table_args__ = {'autoload': True}

    def __init__(self, business_id, location_id):
        self.business_id = business_id
        self.location_id = location_id


class Location(Base):
    __tablename__ = 'location'
    __table_args__ = {'autoload': True}

    def __init__(self, city, state):
        self.city = city
        self.state = state


class BusinessCategory(Base):
    __tablename__ = 'business_category'
    __table_args__ = {'autoload': True}

    def __init__(self, business_id, category_id):
        self.business_id = business_id
        self.category_id = category_id


class Category(Base):
    __tablename__ = 'category'
    __table_args__ = {'autoload': True}

    def __init__(self, category):
        self.category = category


def load_session():
    metadata = Base.metadata
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


def insert_business_entry(session, name, phone, website, email, category, location):
    location = location.split(',')
    city, state = location[0], location[1].split(' ')[1]
    if not business_exists(session, phone=phone, city=city, state=state):
        business = Business(name=name, phone=phone, website=website, email=email)
        session.add(business)
        session.flush()
        location_id = session.query(Location).filter_by(city=city, state=state).first().location_id
        business_id = business.business_id
        business_location = BusinessLocation(business_id=business_id, location_id=location_id)
        session.add(business_location)
        category_id = session.query(Category).filter_by(category=category).first().category_id
        business_category = BusinessCategory(business_id=business_id, category_id=category_id)
        session.add(business_category)
        session.commit()


def business_exists(session, phone, city, state):
    businesses_matched = session.query(Business).filter_by(phone=phone).all()
    existing_locations = []
    for business in businesses_matched:
        locations = session.query(BusinessLocation).filter_by(business_id=business.business_id).all()
        for location in locations:
            existing_location = session.query(Location).filter_by(location_id=location.location_id).first()
            existing_locations.append(existing_location)
    for location in existing_locations:
        if location.city == city and location.state == state:
            #: duplicate entry, return True
            return True
    return False




def insert_location(city, state):
    session = load_session()
    location = Location(city=city, state=state)
    session.add(location)
    session.commit()
    session.close()


def insert_category(category):
    session = load_session()
    category = Category(category=category)
    session.add(category)
    session.commit()
    session.close()


def get_categories(session):
    categories = session.query(Category).all()
    return categories


def get_locations(session):
    locations = session.query(Location).all()
    return locations


if __name__ == '__main__':
    session = load_session()
    res = session.query(Business).all()
