from bson import json_util
import asyncio
import os
import functools
import json
import codecs
import mongomock
import pytest
import yaml

_cache = {}


def pytest_addoption(parser):

    parser.addini(
        name="async_mongodb_fixtures",
        help="Load these fixtures for tests",
        type="linelist",
    )

    parser.addini(
        name="async_mongodb_fixture_dir",
        help="Try loading fixtures from this directory",
        default=os.getcwd(),
    )

    parser.addoption(
        "--async_mongodb-fixture-dir", help="Try loading fixtures from this directory"
    )


def async_decorator(func):
    async def wrapped(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapped


def wrapp_methods(cls):
    for method_name in cls.ASYNC_METHODS:
        method = getattr(cls, method_name)
        setattr(cls, method_name, async_decorator(method))
    return cls


class AsyncCursor(mongomock.collection.Cursor):
    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self)
        except StopIteration:
            raise StopAsyncIteration()

    async def to_list(self, length=None):
        the_list = []
        try:
            while length is None or len(the_list) < length:
                the_list.append(next(self))
        finally:
            return the_list


@wrapp_methods
class AsyncCollection(mongomock.Collection):

    ASYNC_METHODS = [
        "find_one",
        "find_one_and_delete",
        "find_one_and_replace",
        "find_one_and_update",
        "find_and_modify",
        "save",
        "delete_one",
        "delete_many",
        "count",
        "insert_one",
        "insert_many",
        "update_one",
        "update_many",
        "replace_one",
        "count_documents",
        "estimated_document_count",
        "drop",
        "create_index",
        "ensure_index",
        "map_reduce",
        "bulk_write",
    ]

    def find(self, *args, **kwargs) -> AsyncCursor:
        cursor = super().find(*args, **kwargs)
        cursor.__class__ = AsyncCursor
        return cursor


@wrapp_methods
class AsyncDatabase(mongomock.Database):

    ASYNC_METHODS = ["list_collection_names"]

    def get_collection(self, *args, **kwargs) -> AsyncCollection:
        collection = super().get_collection(*args, **kwargs)
        collection.__class__ = AsyncCollection
        return collection


class Session:
    async def __aenter__(self):
        await asyncio.sleep(0)

    async def __aexit__(self, exc_type, exc, tb):
        await asyncio.sleep(0)


class AsyncMockMongoClient(mongomock.MongoClient):
    def get_database(self, *args, **kwargs) -> AsyncDatabase:
        db = super().get_database(*args, **kwargs)
        db.__class__ = AsyncDatabase
        return db

    async def start_session(self, **kwargs):
        await asyncio.sleep(0)
        return Session()


@pytest.fixture(scope="function")
async def async_mongodb(pytestconfig):
    client = AsyncMockMongoClient()
    db = client["pytest"]
    await clean_database(db)
    await load_fixtures(db, pytestconfig)
    return db


@pytest.fixture(scope="function")
async def async_mongodb_client(pytestconfig):
    client = AsyncMockMongoClient()
    db = client["pytest"]
    await clean_database(db)
    await load_fixtures(db, pytestconfig)
    return client


async def clean_database(db):
    collections = await db.list_collection_names()
    for name in collections:
        db.drop_collection(name)


async def load_fixtures(db, config):
    option_dir = config.getoption("async_mongodb_fixture_dir")
    ini_dir = config.getini("async_mongodb_fixture_dir")
    fixtures = config.getini("async_mongodb_fixtures")
    basedir = option_dir or ini_dir

    for file_name in os.listdir(basedir):
        collection, ext = os.path.splitext(os.path.basename(file_name))
        file_format = ext.strip(".")
        supported = file_format in ("json", "yaml")
        selected = fixtures and collection in fixtures
        if selected and supported:
            path = os.path.join(basedir, file_name)
            await load_fixture(db, collection, path, file_format)


async def load_fixture(db, collection, path, file_format):
    if file_format == "json":
        loader = functools.partial(json.load, object_hook=json_util.object_hook)
    elif file_format == "yaml":
        loader = functools.partial(yaml.load, Loader=yaml.FullLoader)
    else:
        return
    try:
        docs = _cache[path]
    except KeyError:
        with codecs.open(path, encoding="utf-8") as fp:
            _cache[path] = docs = loader(fp)

    for document in docs:
        await db[collection].insert_one(document)
