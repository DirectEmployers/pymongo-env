import ssl
from contextlib import contextmanager
from pymongo import MongoClient

try:
    import settings
except ImportError:
    # Provide some meaningless base settings for compatibility purposes.
    import base_settings as settings

if hasattr(settings, 'MONGO_CN'):
    mongo_cn = settings.MONGO_CN
else:
    mongo_cn = settings.MONGO_HOST
mongo_dbname = settings.MONGO_DBNAME
mongo_ssl = getattr(settings, 'MONGO_SSL', False)


def change_db(cn, dbname, ssl=False):
    """
    Switch database connection information process wide.

    DbAccess objects created subsequent to this call will use the new
    connection info.
    """
    global mongo_cn, mongo_dbname, mongo_ssl
    mongo_cn = cn
    mongo_dbname = dbname
    mongo_ssl = ssl
    settings.MONGO_SSL = ssl
    settings.MONGO_HOST = cn
    settings.MONGO_DBNAME = dbname


def connect_db():
    """
    Connect to a MongoDB instance.

    Uses local settings to connect to MongoDB instances.

    For code running in production: db = connect_db().db

    This takes care of redirecting test data to a separate database if
    the development environment is configured correctly.

    """
    global mongo_cn, mongo_dbname, mongo_ssl

    if mongo_ssl:
        ssl_req = ssl.CERT_NONE
    else:
        ssl_req = None

    client = MongoClient(
        mongo_cn,
        w='majority',
        connect=True,
        ssl_cert_reqs=ssl_req)

    db = client[mongo_dbname]

    # Fail now if we can't do this.
    db.command('ping', check=True)

    # Allow common access pattern
    return DbAccess(client, db)


class DbAccess(object):
    """Holds a MongoDB client and database reference."""
    def __init__(self, client, db):
        self.client = client
        self.db = db

    def __str__(self):
        return "<DbAccess %r / %r>" % (self.client.host, self.db.name)


@contextmanager
def production_access():
    """
    Access the production servers in a limited scope.

    i.e.
        with production_access() as access:
            useful_data = access.db.analytics.find({...})

    """
    with db_access('PRODUCTION', True) as access:
        yield access


@contextmanager
def db_access(prefix, ssl=False):
    """
    Access servers for a specified environment in a limited scope.

    i.e.
        with db_access() as access:
            useful_data = access.db.analytics.find({...})

    :param prefix: The settings prefix of the environment you want to access.
    :param ssl: Boolean for if the environment requires a secure connection.

    """
    if hasattr(settings, 'MONGO_CN'):
        old_host = settings.MONGO_CN
    else:
        old_host = settings.MONGO_HOST
    old_dbname = settings.MONGO_DBNAME
    if hasattr(settings, prefix + '_MONGO_CN'):
        new_cn = getattr(settings, prefix + '_MONGO_CN')
    else:
        new_cn = getattr(settings, prefix + '_MONGO_HOST')
    new_dbname = getattr(settings, prefix + '_MONGO_DBNAME')

    try:
        change_db(new_cn, new_dbname, ssl)
        db_access = connect_db()
        yield db_access
    finally:
        # Note that this does not protect the db from future operations.
        db_access.client.close()
        db_access.client = None
        db_access.db = None
        change_db(old_host, old_dbname, ssl)


@contextmanager
def production_db():
    """
    Access just the production database object in a limited scope.

    i.e.
        with production_db() as prod_db:
            useful_data = prod_db.analytics.find({...})
        del prod_db

    """
    with production_access() as db_access:
        yield db_access.db


@contextmanager
def context_db(prefix, ssl=False):
    with db_access(prefix, ssl) as access:
        yield access.db
