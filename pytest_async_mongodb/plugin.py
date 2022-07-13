from bson import json_util
import asyncio
import os
import functools
import json
import codecs
import mongomock
import yaml
import pytest_asyncio

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


def async_wrap(obj):
    # wrap all the public interfaces except the one has been re-defined in obj
    for item in dir(obj._base_sync_obj):
        if not item.startswith("_"):
            member = getattr(obj._base_sync_obj, item)
            if callable(member) and item not in dir(obj):
                setattr(obj, item, async_decorator(member))


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


class AsyncCommandCursor(mongomock.command_cursor.CommandCursor):
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


class AsyncCollection:
    def __init__(self, mongomock_collection):
        self._base_sync_obj = mongomock_collection
        async_wrap(self)

    def find(self, *args, **kwargs) -> AsyncCursor:
        cursor = self._base_sync_obj.find(*args, **kwargs)
        cursor.__class__ = AsyncCursor
        return cursor

    def aggregate(self, *args, **kwargs) -> AsyncCommandCursor:
        cursor = self._base_sync_obj.aggregate(*args, **kwargs)
        cursor.__class__ = AsyncCommandCursor
        return cursor


class AsyncDatabase:
    def __init__(self, mongomock_db):
        self._base_sync_obj = mongomock_db
        async_wrap(self)

    def __getattr__(self, attr):
        return self[attr]

    def __getitem__(self, db_name):
        return self.get_collection(db_name)

    def get_collection(self, *args, **kwargs) -> AsyncCollection:
        collection = self._base_sync_obj.get_collection(*args, **kwargs)
        return AsyncCollection(collection)


class Session:
    async def __aenter__(self):
        await asyncio.sleep(0)

    async def __aexit__(self, exc_type, exc, tb):
        await asyncio.sleep(0)


class AsyncMockMongoClient:
    def __init__(self, mongomock_client):
        self._base_sync_obj = mongomock_client
        async_wrap(self)

    def __getattr__(self, attr):
        return self[attr]

    def __getitem__(self, db_name):
        return self.get_database(db_name)

    def get_database(self, *args, **kwargs) -> AsyncDatabase:
        db = self._base_sync_obj.get_database(*args, **kwargs)
        return AsyncDatabase(db)

    async def start_session(self, **kwargs):
        await asyncio.sleep(0)
        return Session()


@pytest_asyncio.fixture(scope="function")
async def async_mongodb(event_loop, pytestconfig):
    client = AsyncMockMongoClient(mongomock.MongoClient())
    db = client["pytest"]
    await clean_database(db)
    await load_fixtures(db, pytestconfig)
    return db


@pytest_asyncio.fixture(scope="function")
async def async_mongodb_client(event_loop, pytestconfig):
    client = AsyncMockMongoClient(mongomock.MongoClient())
    db = client["pytest"]
    await clean_database(db)
    await load_fixtures(db, pytestconfig)
    return client


async def clean_database(db):
    collections = await db.list_collection_names()
    for name in collections:
        await db.drop_collection(name)


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
