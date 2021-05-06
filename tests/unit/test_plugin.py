import pytest
from pytest_async_mongodb import plugin

pytestmark = pytest.mark.asyncio


async def test_load(async_mongodb):
    collection_names = await async_mongodb.list_collection_names()
    assert 'players' in collection_names
    assert 'championships' in collection_names
    assert len(plugin._cache.keys()) == 2
    await check_players(async_mongodb.players)
    await check_championships(async_mongodb.championships)


async def check_players(players):
    count = await players.count_documents({})
    assert count == 2
    await check_keys_in_docs(players, ['name', 'surname', 'position'])
    manuel = await players.find_one({'name': 'Manuel'})
    assert manuel['surname'] == 'Neuer'
    assert manuel['position'] == 'keeper'


async def check_championships(championships):
    count = await championships.count_documents({})
    assert count == 3
    await check_keys_in_docs(championships, ['year', 'host', 'winner'])


async def check_keys_in_docs(collection, keys):
    docs = collection.find()
    for doc in docs:
        for key in keys:
            assert key in doc


async def test_insert(async_mongodb):
    await async_mongodb.players.insert_one({
        'name': 'Bastian',
        'surname': 'Schweinsteiger',
        'position': 'midfield'
    })
    count = await async_mongodb.players.count_documents({})
    bastian = await async_mongodb.players.find_one({'name': 'Bastian'})
    assert count == 3
    assert bastian.get('name') == 'Bastian'
