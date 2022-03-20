import logging
from model import Base
from extensions import engine


logging.getLogger().setLevel(logging.INFO)

# If you have modified models:
# - Delete or rename existing "market.db" file
# - Run this script

if __name__ == '__main__':
    logging.info('Creating the database schema..')
    Base.metadata.create_all(engine)
