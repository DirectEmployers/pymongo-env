import ssl
from contextlib import contextmanager
from pymongo import MongoClient

try:
    import secrets
except ImportError:
    # Provide some meaningless base secrets for compatibility purposes.
    import base_secrets as secrets

mongo_cn = secrets.MONGO_CN
mongo_dbname = secrets.MONGO_DBNAME
mongo_ssl = getattr(secrets, 'MONGO_SSL', False)


def change_db(cn, dbname, ssl_setting):
    """
    Switch database connection information process wide.

    DbAccess objects created subsequent to this call will use the new
    connection info.
    """
    global mongo_cn, mongo_dbname, mongo_ssl

    mongo_cn = cn
    mongo_dbname = dbname
    mongo_ssl = ssl_setting

    secrets.MONGO_SSL = ssl_setting
    secrets.MONGO_CN = cn
    secrets.MONGO_DBNAME = dbname


def connect_db():
    """
    Connect to a MongoDB instance.

    Uses local secrets to connect to MongoDB instances.

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

    def __del__(self):
        self.client.close()
        self.client = None
        self.db = None


@contextmanager
def production_access():
    """
    Access the production servers in a limited scope.

    i.e.
        with production_access() as access:
            useful_data = access.db.analytics.find({...})

    """
    with db_access('PRODUCTION') as access:
        yield access


@contextmanager
def db_access(prefix):
    """
    Access servers for a specified environment in a limited scope.

    i.e.
        with db_access() as access:
            useful_data = access.db.analytics.find({...})

    :param prefix: The secrets prefix of the environment you want to access.

    """
    old_host = secrets.MONGO_CN
    old_dbname = secrets.MONGO_DBNAME
    old_ssl = secrets.MONGO_SSL

    new_cn = getattr(secrets, prefix + '_MONGO_CN')
    new_dbname = getattr(secrets, prefix + '_MONGO_DBNAME')
    new_ssl = getattr(secrets, prefix + '_MONGO_SSL', False)

    try:
        change_db(new_cn, new_dbname, new_ssl)
        db_access = connect_db()
        yield db_access
    finally:
        change_db(old_host, old_dbname, old_ssl)


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
def context_db(prefix):
    with db_access(prefix) as access:
        yield access.db
