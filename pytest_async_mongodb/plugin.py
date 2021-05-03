import asyncio
import os
import functools
import json
import codecs
import types

import mongomock
import pytest
import yaml
from bson import json_util


_cache = {}


def pytest_addoption(parser):

    parser.addini(
        name='async_mongodb_fixtures',
        help='Load these fixtures for tests',
        type='linelist')

    parser.addini(
        name='async_mongodb_fixture_dir',
        help='Try loading fixtures from this directory',
        default=os.getcwd())

    parser.addoption(
        '--async_mongodb-fixture-dir',
        help='Try loading fixtures from this directory')


def wrapper(func):
    @functools.wraps(func)
    async def wrapped(*args, **kwargs):
        coro_func = asyncio.coroutine(func)
        return await coro_func(*args, **kwargs)
    return wrapped


class AsyncClassMethod(object):

    ASYNC_METHODS = []

    def __getattribute__(self, name):
        attr = super(AsyncClassMethod, self).__getattribute__(name)
        if type(attr) == types.MethodType and name in self.ASYNC_METHODS:
            attr = wrapper(attr)
        return attr


class AsyncCollection(AsyncClassMethod, mongomock.Collection):

    ASYNC_METHODS = [
        'find',
        'find_one',
        'find_one_and_delete',
        'find_one_and_replace',
        'find_one_and_update',
        'find_and_modify',
        'save',
        'delete_one',
        'delete_many',
        'count',
        'insert_one',
        'insert_many',
        'update_one',
        'update_many',
        'replace_one',
        'count_documents',
        'estimated_document_count',
        'drop',
        'create_index',
        'ensure_index',
        'map_reduce',
    ]

    async def find_one(self, filter=None, *args, **kwargs):
        import collections
        # Allow calling find_one with a non-dict argument that gets used as
        # the id for the query.
        if filter is None:
            filter = {}
        if not isinstance(filter, collections.Mapping):
            filter = {'_id': filter}

        cursor = await self.find(filter, *args, **kwargs)
        try:
            return next(cursor)
        except StopIteration:
            return None


class AsyncDatabase(AsyncClassMethod, mongomock.Database):

    ASYNC_METHODS = [
        'list_collection_names'
    ]

    def get_collection(self, name, codec_options=None, read_preference=None,
                       write_concern=None):
        collection = self._collections.get(name)
        if collection is None:
            collection = self._collections[name] = AsyncCollection(self, name)
        return collection


class Session:
    async def __aenter__(self):
        await asyncio.sleep(0)

    async def __aexit__(self, exc_type, exc, tb):
        await asyncio.sleep(0)


class AsyncMockMongoClient(mongomock.MongoClient):

    def get_database(self, name, codec_options=None, read_preference=None,
                     write_concern=None):
        db = self._databases.get(name)
        if db is None:
            db = self._databases[name] = AsyncDatabase(self, name)
        return db

    async def start_session(self, **kwargs):
        await asyncio.sleep(0)
        return Session()


@pytest.fixture(scope='function')
async def async_mongodb(pytestconfig):
    client = AsyncMockMongoClient()
    db = client['pytest']
    await clean_database(db)
    await load_fixtures(db, pytestconfig)
    return db


@pytest.fixture(scope='function')
async def async_mongodb_client(pytestconfig):
    client = AsyncMockMongoClient()
    db = client['pytest']
    await clean_database(db)
    await load_fixtures(db, pytestconfig)
    return client


async def clean_database(db):
    collections = await db.list_collection_names()
    for name in collections:
        db.drop_collection(name)


async def load_fixtures(db, config):
    option_dir = config.getoption('async_mongodb_fixture_dir')
    ini_dir = config.getini('async_mongodb_fixture_dir')
    fixtures = config.getini('async_mongodb_fixtures')
    basedir = option_dir or ini_dir

    for file_name in os.listdir(basedir):
        collection, ext = os.path.splitext(os.path.basename(file_name))
        file_format = ext.strip('.')
        supported = file_format in ('json', 'yaml')
        selected = fixtures and collection in fixtures
        if selected and supported:
            path = os.path.join(basedir, file_name)
            await load_fixture(db, collection, path, file_format)


async def load_fixture(db, collection, path, file_format):
    if file_format == 'json':
        loader = functools.partial(json.load, object_hook=json_util.object_hook)
    elif file_format == 'yaml':
        loader = functools.partial(yaml.load, Loader=yaml.FullLoader)
    else:
        return
    try:
        docs = _cache[path]
    except KeyError:
        with codecs.open(path, encoding='utf-8') as fp:
            _cache[path] = docs = loader(fp)

    for document in docs:
        await db[collection].insert_one(document)
