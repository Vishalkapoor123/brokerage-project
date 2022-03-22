# brokerage-project
Python project to get stock details

Initial-setup:
Change directory to project location root level and install modules mentioned in requirements.txt
You can do the same using - pip install -r requirements.txt

Database-setup:
Make sure market.db file is present at the root level
Run create_schema.py file to create sqlite schemas - You can do the same by hitting this command at root level - python create_schema.py

Run the Program:
In terminal run - python load.py --symbol=<symbol> --currency=<currency> --start=<start date> --end=<end date>

Run Tests:
In terminal run - python -m pytest test.py

##################################
This program accepts stock symbol, currency symbol, start date and end date as input params as command line arguments and returns the closing price details for that stock converted into desired currency for all the dates from start date and end date both inclusive.


