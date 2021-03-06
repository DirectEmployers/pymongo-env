from mock import patch
from unittest import TestCase

import base_secrets


class TestMongoClient(object):
    def __init__(self, mongo_cn, w='majority', connect=True,
                 ssl_cert_reqs=None):
        self.mongo_cn = mongo_cn
        self.w = w
        self.connect = connect
        self.ssl_cert_reqs = ssl_cert_reqs


class MongoEnvTestCase(TestCase):
    def setUp(self):
        """
        Do all the initial configuration of the settings since base_secrets
        is just an empty file.

        """
        self.initial_cn = 'TEST_CN'
        self.initial_dbname = 'TEST_DBNAME'
        self.initial_ssl = False
        self.prefix = 'APREFIX'
        self.prod_prefix = 'PRODUCTION'

        setattr(base_secrets, 'MONGO_CN', self.initial_cn)
        setattr(base_secrets, 'MONGO_DBNAME', self.initial_dbname)
        setattr(base_secrets, 'MONGO_SSL', self.initial_ssl)

    def tearDown(self):
        """
        Remove all the custom configuration that has been done to confirm that
        base_secrets aren't carried over between tests.

        """
        set_attrs = ['MONGO_CN', 'MONGO_DBNAME', 'MONGO_SSL']
        for attr in set_attrs:
            if hasattr(base_secrets, attr):
                delattr(base_secrets, attr)
            for prefix in [self.prefix, self.prod_prefix]:
                prefixed_attr = "%s_%s" % (prefix, attr)
                if hasattr(base_secrets, prefixed_attr):
                    delattr(base_secrets, prefixed_attr)

    @staticmethod
    def import_and_reload_pymongoenv():
        """
        Imports pymongoenv and ensures that it's been reloaded so it's
        pulling the globals from the most recent version of the base_secrets
        file.

        """
        import pymongoenv
        reload(pymongoenv)
        return pymongoenv

    def test_init(self):
        """
        Settings should be properly initialized to reflect defaults in the
        settings.py/base_secrets.py files.

        """
        pymongoenv = self.import_and_reload_pymongoenv()

        self.assertEqual(pymongoenv.mongo_cn, self.initial_cn)
        self.assertEqual(pymongoenv.mongo_dbname, self.initial_dbname)
        self.assertEqual(pymongoenv.mongo_ssl, self.initial_ssl)

    def test_change_db_cluster(self):
        """
        change_db should be able to successfully move from one cluster/host
        to another.

        """
        pymongoenv = self.import_and_reload_pymongoenv()
        new_cn_name = 'NEW_CN'

        # Confirm that the setting is expected before switching.
        self.assertEqual(pymongoenv.mongo_cn, self.initial_cn)

        pymongoenv.change_db(new_cn_name, self.initial_dbname, self.initial_ssl)

        self.assertEqual(pymongoenv.mongo_cn, new_cn_name)
        # dbname and ssl settings should not have changed
        self.assertEqual(pymongoenv.mongo_dbname, self.initial_dbname)
        self.assertEqual(pymongoenv.mongo_ssl, self.initial_ssl)

    def test_change_db_database(self):
        """
        change_db should be able to successfully move from one database
        to another.

        """
        pymongoenv = self.import_and_reload_pymongoenv()
        new_dbname = 'NEW_DBNAME'

        # Confirm that the setting is expected before switching.
        self.assertEqual(pymongoenv.mongo_cn, self.initial_cn)

        pymongoenv.change_db(self.initial_cn, new_dbname, self.initial_ssl)

        self.assertEqual(pymongoenv.mongo_dbname, new_dbname)
        # cn and ssl settings should not have changed
        self.assertEqual(pymongoenv.mongo_cn, self.initial_cn)
        self.assertEqual(pymongoenv.mongo_ssl, self.initial_ssl)

    def test_change_db_ssl(self):
        """
        change_db should be able to successfully move between requiring
        and not requiring ssl.

        """
        pymongoenv = self.import_and_reload_pymongoenv()
        new_ssl = not self.initial_ssl

        # Confirm that the setting is expected before switching.
        self.assertEqual(pymongoenv.mongo_ssl, self.initial_ssl)
        self.assertNotEqual(pymongoenv.mongo_ssl, new_ssl)

        pymongoenv.change_db(self.initial_cn, self.initial_dbname, new_ssl)

        self.assertEqual(pymongoenv.mongo_ssl, new_ssl)
        # cn and dbname settings should not have changed
        self.assertEqual(pymongoenv.mongo_cn, self.initial_cn)
        self.assertEqual(pymongoenv.mongo_dbname, self.initial_dbname)

    @patch('pymongo.database.Database.command')
    def test_db_access(self, mock_command):
        """
        Accessing mongo via a specific prefix should
        return the correct client and database specified by the
        prefix, regardless of the default, un-prefixed settings.

        Mock pymongo.database.Database.command so there are no issues with
        the fact that the hosts/clusters shouldn't actually exist.
        """
        pymongoenv = self.import_and_reload_pymongoenv()

        prefixed_cn = 'prefixed_cn'
        prefixed_dbname = 'prefixed_dbname'

        setattr(base_secrets, '%s_%s' % (self.prefix, 'MONGO_CN'), prefixed_cn)
        setattr(base_secrets, '%s_%s' % (self.prefix, 'MONGO_DBNAME'),
                prefixed_dbname)

        with pymongoenv.db_access(self.prefix) as access:
            # This is weird, but it's the way MongoClient handles __repr__()
            # so it's probably the best way to get this info for now.
            for host, port in access.client._topology_settings.seeds:
                self.assertEqual(host, prefixed_cn)
            self.assertEqual(access.db._Database__name, prefixed_dbname)

            self.assertEqual(pymongoenv.mongo_cn, prefixed_cn)
            self.assertEqual(pymongoenv.mongo_dbname, prefixed_dbname)

        # Outside the context manager everything should be back to normal.
        self.assertEqual(pymongoenv.mongo_cn, self.initial_cn)
        self.assertEqual(pymongoenv.mongo_dbname, self.initial_dbname)

    @patch('pymongo.database.Database.command')
    def test_production_access(self, mock_command):
        """
        Accessing mongo via production_access should provide the correct
        client and database specified by the PRODUCTION prefix,
        regardless of the default, un-prefixed settings.

        Mock pymongo.database.Database.command so there are no issues with
        the fact that the hosts/clusters shouldn't actually exist.
        """
        pymongoenv = self.import_and_reload_pymongoenv()

        production_cn = 'production_cn'
        production_dbname = 'production_dbname'

        setattr(base_secrets, '%s_%s' % (self.prod_prefix, 'MONGO_CN'),
                production_cn)
        setattr(base_secrets, '%s_%s' % (self.prod_prefix, 'MONGO_DBNAME'),
                production_dbname)

        with pymongoenv.production_access() as access:
            # This is weird, but it's the way MongoClient handles __repr__()
            # so it's probably the best way to get this info for now.
            for host, port in access.client._topology_settings.seeds:
                self.assertEqual(host, production_cn)
            self.assertEqual(access.db._Database__name, production_dbname)

            self.assertEqual(pymongoenv.mongo_cn, production_cn)
            self.assertEqual(pymongoenv.mongo_dbname, production_dbname)

        # Outside the context manager everything should be back to normal.
        self.assertEqual(pymongoenv.mongo_cn, self.initial_cn)
        self.assertEqual(pymongoenv.mongo_dbname, self.initial_dbname)

    @patch('pymongo.database.Database.command')
    def test_production_db(self, mock_command):
        """
        Accessing mongo via production_access should provide the correct
        database specified by the PRODUCTION prefix,
        regardless of the default, un-prefixed settings.

        Mock pymongo.database.Database.command so there are no issues with
        the fact that the hosts/clusters shouldn't actually exist.
        """
        pymongoenv = self.import_and_reload_pymongoenv()

        production_cn = 'production_cn'
        production_dbname = 'production_dbname'

        setattr(base_secrets, '%s_%s' % (self.prod_prefix, 'MONGO_CN'),
                production_cn)
        setattr(base_secrets, '%s_%s' % (self.prod_prefix, 'MONGO_DBNAME'),
                production_dbname)

        with pymongoenv.production_db() as db:
            self.assertEqual(db._Database__name, production_dbname)

            self.assertEqual(pymongoenv.mongo_cn, production_cn)
            self.assertEqual(pymongoenv.mongo_dbname, production_dbname)

        # Outside the context manager everything should be back to normal.
        self.assertEqual(pymongoenv.mongo_cn, self.initial_cn)
        self.assertEqual(pymongoenv.mongo_dbname, self.initial_dbname)

    @patch('pymongo.database.Database.command')
    def test_context_db(self, mock_command):
        """
        Accessing mongo via a specific prefix should
        return the correct database specified by the prefix, regardless
        of the default, un-prefixed settings.

        Mock pymongo.database.Database.command so there are no issues with
        the fact that the hosts/clusters shouldn't actually exist.
        """
        pymongoenv = self.import_and_reload_pymongoenv()

        prefixed_cn = 'context_prefixed_cn'
        prefixed_dbname = 'context_prefixed_dbname'

        setattr(base_secrets, '%s_%s' % (self.prefix, 'MONGO_CN'), prefixed_cn)
        setattr(base_secrets, '%s_%s' % (self.prefix, 'MONGO_DBNAME'),
                prefixed_dbname)

        with pymongoenv.db_access(self.prefix) as access:
            self.assertEqual(access.db._Database__name, prefixed_dbname)

            self.assertEqual(pymongoenv.mongo_cn, prefixed_cn)
            self.assertEqual(pymongoenv.mongo_dbname, prefixed_dbname)

        # Outside the context manager everything should be back to normal.
        self.assertEqual(pymongoenv.mongo_cn, self.initial_cn)
        self.assertEqual(pymongoenv.mongo_dbname, self.initial_dbname)
