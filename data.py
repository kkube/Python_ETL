from sqlalchemy import create_engine, MetaData, Table, func
from sqlalchemy.sql import select, desc
import os


# move to directory containing database file
os.chdir('C:/Users/Barb/Desktop/SQLite ETL')

# create an engine and connect to the database
engine = create_engine('sqlite:///census.sqlite')
connection = engine.connect()

"""
# print the names of the tables in your database
print(engine.table_names())
"""
# store metadata about a table
metadata = MetaData()
census = Table('census', metadata, autoload=True, autoload_with=engine)

"""
# print column names along with data types
print(repr(census))

# print strictly column names
print(census.columns.keys())
"""
# SQL statement and storing data as a python object
"""
stmt = 'SELECT * FROM census'
result_proxy = connection.execute(stmt)
results = result_proxy.fetchall()

# inspect that data loaded
first_row = results[0]
print(first_row)
print(first_row['state'])

stmt = select([census])
stmt = stmt.where(census.columns.state == 'Texas')
results = connection.execute(stmt).fetchall()


# print the difference in population from 2000 and 2008 by age and sex for Texas
for result in results:
    print(result.state, result.sex, result.age, result.pop2000 - result.pop2008)
"""

"""
# prints the sum of all of the total population in Texas for 2008
stmt = select([func.sum(census.columns.pop2008)])
results = connection.execute(stmt).scalar()
print(results)
"""

"""
# print total 2008 population by sex in Texas
stmt = select([census])
stmt = stmt.where(census.columns.state == 'Texas')
stmt = select([census.columns.state, census.columns.sex, func.sum(census.columns.pop2008)])
stmt = stmt.group_by(census.columns.sex)
results = connection.execute(stmt).fetchall()
print(results)
"""
"""
# print total 2008 population by state

pop2008_sum = func.sum(census.columns.pop2008).label('population')
stmt = select([census.columns.state, pop2008_sum])
stmt = stmt.group_by(census.columns.state)
results = connection.execute(stmt).fetchall()
print(results)
print(results[0].keys())

"""

# Create a population change column and return the top 5 results

stmt = select([census.columns.age, (census.columns.pop2008 - census.columns.pop2000).label('pop_change')])
stmt = stmt.group_by(census.columns.age)
stmt = stmt.order_by(desc('pop_change'))
stmt = stmt.limit(5)
results = connection.execute(stmt).fetchall()
print(results)
