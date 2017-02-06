from pymongoenv import connect_db, change_db

try:
    import secrets
except ImportError:
    # Provide some meaningless base secrets for compatibility purposes.
    import base_secrets as secrets


class MongoTestMixin(object):
    collection_names = []

    def setUp(self):
        change_db(secrets.TEST_MONGO_CN, secrets.TEST_MONGO_DBNAME,
                  secrets.TEST_MONGO_SSL)
        self.db_access = connect_db()
        self.all_collections = []
        self.db = self.db_access.db
        for collection_name in self.collection_names:
            setattr(self, collection_name, self.db[collection_name])
            self.all_collections.append(self.db[collection_name])

        counts = {
            c.full_name: c.count()
            for c in self.all_collections
            if c.count() > 0
        }

        if len(counts):
            message = (
                "Mongo server at {db_access} contains "
                "documents; is this a production "
                "instance? doc counts: {counts}").format(
                    db_access=self.db_access,
                    counts=counts)

            self.fail(message)

    def tearDown(self):
        for collection in self.all_collections:
            collection.delete_many({})
        del self.db_access
