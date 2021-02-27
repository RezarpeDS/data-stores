import pymongo
from bson import ObjectId
from . import defaults


class Storage:
    """
    A storage wraps over a mongodb connection and some default
    settings. It has convenience functions to create/update,
    read, list, and delete.
    """

    DEFAULT_SETTINGS = {
        "_": defaults.translate
    }

    def __init__(self, username, password, host, port, **kwargs):
        self._settings = self.DEFAULT_SETTINGS.copy()
        self._settings.update(**kwargs)
        self._client = pymongo.MongoClient("mongodb://%s:%s@%s:%s" % (username, password, host, port))

    @property
    def settings(self):
        return self._settings

    def _wrap_oid(self, key):
        return ObjectId(key) if not isinstance(key, ObjectId) else key

    def _unwrap_oid(self, key):
        return str(key) if isinstance(key, ObjectId) else key

    def update_by_key(self, database, collection, key, data):
        """
        Fully replaces the document's data by its key in a particular database/collection.
        :param database: The database to affect.
        :param collection: The collection to affect.
        :param key: The key to use for the replacement.
        :param data: The new data.
        :return: An UpdateResult.
        """

        return self._client[database][collection].replace_one({"_id": self._wrap_oid(key)}, data)

    def delete_by_key(self, database, collection, key):
        """
        Deletes a document by its key in a particular database/collection.
        :param database: The database to affect.
        :param collection: The collection to affect.
        :param key: The key to use for the replacement.
        :return: A DeleteResult.
        """

        return self._client[database][collection].delete_one({"_id": self._wrap_oid(key)})

    def get_by_key(self, database, collection, key):
        """
        Gets a document by its key in a particular database/collection.
        :param database: The database to query.
        :param collection: The collection to query.
        :param key: The key to use for the replacement.
        :return: None (if no matching document found) or an arbitrary BSON-compatible dict.
        """

        return self._client[database][collection].find_one({"_id": self._wrap_oid(key)})

    def create(self, database, collection, data):
        """
        Inserts a document in a particular database/collection.
        :param database: The database to affect.
        :param collection: The collection to affect.
        :param data: The data for the new document.
        """

        return self._client[database][collection].insert_one(data)

    def query(self, database, collection, skip=0, limit=0, sort=None,
              where=None, select=None):
        """
        Gets the elements inside a collection.
        :param database: The database to query.
        :param collection: The collection to query.
        :param skip: If specified (and positive - it will be clamped
          anyway), a custom start offset will be used for the listings.
        :param limit: If specified (and positive - it will be clamped
          anyway), how many elements to retrieve. Otherwise, all the
          elements starting from the offset will be retrieved (according
          to the ordering).
        :param sort: If specified, a custom ordering will be chosen for
          the query. Otherwise, mongo's default ordering will be used.
        :param where: If specified, a custom filter on the data. Omitting
          it selects all the documents.
        :param select: If specified, a custom subset on the columns.
          Omitting it selects all the columns of each document.
        :return: An iterable of matching dictionaries.
        """

        skip = max(skip, 0)
        limit = max(limit, 0)
        result = self._client[database][collection].find(
            skip=skip, limit=limit, filter=where, projection=select
        )
        if sort:
            result = result.sort(sort)
        return result
