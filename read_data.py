import logging
from model import Example
from extensions import session


logging.getLogger().setLevel(logging.INFO)

# This is a sample how to read from the database.

if __name__ == '__main__':
    rows = session.query(Example).all()
    logging.info(f'We have {len(rows)} rows in Example table')

