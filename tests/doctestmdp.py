"""
Start going through the docs for SQ with Monetdb backend...

  >>> import logging, sys
  >>> logging.basicConfig(filename=".doctest.log")
  >>> logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)
  >>> class WrapStdOut(object):
  ...     def __getattr__(self, name):
  ...         return getattr(sys.stdout, name)
  >>> #logging.getLogger('sqlalchemy.engine').addHandler(WrapStdOut())
  >>> import sqlalchemy as sa
  >>> engine = sa.create_engine("monetdb://host=localhost")


Define and create tables 
-------------------------
(note mdb comes with a "users" table)


  >>> metadata = sa.MetaData()
  >>> users = sa.Table('testusers', metadata,
  ...     sa.Column('id', sa.Integer, primary_key=True),
  ...     sa.Column('name', sa.String(40)),
  ...     sa.Column('fullname', sa.String(100)),
  ... )


  >>> addresses = sa.Table('testaddresses', metadata, 
  ...   sa.Column('id', sa.Integer, primary_key=True),
  ...   sa.Column('user_id', None, sa.ForeignKey('testusers.id')),
  ...   sa.Column('email_address', sa.String(50), nullable=False)
  ...  )


  >>> #dir(addresses)
  >>> create = addresses.create

  >>> metadata.create_all(engine)

Insert expressions
------------------

  >>> ins = users.insert()
  >>> str(ins)
  'INSERT INTO testusers (id, name, fullname) VALUES (:id, :name, :fullname)'
  >>> ins = users.insert(values={'name':'jack', 'fullname':'Jack Jones'})
  >>> str(ins)
  'INSERT INTO testusers (name, fullname) VALUES (:name, :fullname)'
  >>> ins.compile().params 
  ClauseParameters:{'fullname': 'Jack Jones', 'name': 'jack'}

Executing
---------

  >>> conn = engine.connect()
  >>> conn #doctest: +ELLIPSIS
  <sqlalchemy.engine.base.Connection object at 0x...>

  >>> result = conn.execute(ins)

  INSERT INTO users (name, fullname) VALUES (?, ?)
  ['jack', 'Jack Jones']
  COMMIT


Executing multiple statements
-----------------------------

  >>> ins = users.insert()
  >>> conn.execute(ins, id=2, name='wendy', fullname='Wendy Williams') #doctest: +ELLIPSIS
  <sqlalchemy.engine.base.ResultProxy object at 0x...>


Implicit inserting
------------------

  >>> import pdb
  >>> #pdb.set_trace()
  >>> engine.execute(users.insert(), name='fred', fullname="Fred Flintstone") #doct3est: +ELLIPSIS
  <sqlalchemy.engine.base.ResultProxy object at 0x...>


  >>> metadata.bind = engine
  >>> result = users.insert().execute(name="mary", fullname="Mary Contrary")
  >>> metadata.bind = None


Selecting
---------

  >>> s = sa.select([users])
  >>> result = conn.execute(s)
  >>> for row in result:
  ...     print row

dictionary access

  >>> result = conn.execute(s)
  >>> row = result.fetchone()
  >>> print "name:", row['name'], "; fullname:", row['fullname']
  name: jack ; fullname: Jack Jones

integer indexes
  >>> row = result.fetchone()
  >>> print "name:", row[1], "; fullname:", row[2]
  name: wendy ; fullname: Wendy Williams

Use columns directly
  >>> for row in conn.execute(s):
  ...     print "name:", row[users.c.name], "; fullname:", row[users.c.fullname]
  name: jack ; fullname: Jack Jones
  name: wendy ; fullname: Wendy Williams
  name: fred ; fullname: Fred Flintstone
  name: mary ; fullname: Mary Contrary

Close the result set
  >>> result.close()

Drop tables
------------

  >>> metadata.drop_all(engine)

"""

import doctest
doctest.testmod()
#print globals()
s = doctest.script_from_examples(globals()["__doc__"])
fout = open("/tmp/debug.py",'w')
fout.write(s)
