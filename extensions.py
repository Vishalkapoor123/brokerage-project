from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine('sqlite:///market.db')
Session = sessionmaker(bind=engine)
session = Session()
