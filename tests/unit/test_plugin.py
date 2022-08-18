from bson import ObjectId
from pytest_async_mongodb import plugin
import pytest
from pymongo import InsertOne, DESCENDING

pytestmark = pytest.mark.asyncio


async def test_load(async_mongodb):
    collection_names = await async_mongodb.list_collection_names()
    assert "players" in collection_names
    assert "championships" in collection_names
    assert len(plugin._cache.keys()) == 2
    await check_players(async_mongodb.players)
    await check_championships(async_mongodb.championships)


async def check_players(players):
    count = await players.count_documents({})
    assert count == 2
    await check_keys_in_docs(players, ["name", "surname", "position"])
    manuel = await players.find_one({"name": "Manuel"})
    assert manuel["surname"] == "Neuer"
    assert manuel["position"] == "keeper"


async def check_championships(championships):
    count = await championships.count_documents({})
    assert count == 4
    await check_keys_in_docs(championships, ["year", "host", "winner"])


async def check_keys_in_docs(collection, keys):
    docs = collection.find()
    for doc in docs:
        for key in keys:
            assert key in doc


async def test_insert(async_mongodb):
    count_before = await async_mongodb.players.count_documents({})
    await async_mongodb.players.insert_one(
        {"name": "Bastian", "surname": "Schweinsteiger", "position": "midfield"}
    )
    count_after = await async_mongodb.players.count_documents({})
    bastian = await async_mongodb.players.find_one({"name": "Bastian"})
    assert count_after == count_before + 1
    assert bastian.get("name") == "Bastian"


async def test_find_one(async_mongodb):
    doc = await async_mongodb.championships.find_one()
    assert doc == {
        "_id": ObjectId("608b0151a20cf0c679939f59"),
        "year": 2018,
        "host": "Russia",
        "winner": "France",
    }


async def test_find(async_mongodb):
    docs = async_mongodb.championships.find()
    docs_list = []
    async for doc in docs:
        docs_list.append(doc)
    assert docs_list == [
        {
            "_id": ObjectId("608b0151a20cf0c679939f59"),
            "year": 2018,
            "host": "Russia",
            "winner": "France",
        },
        {
            "_id": ObjectId("55d2db06f4811f83a1f27be8"),
            "year": 2014,
            "host": "Brazil",
            "winner": "Germany",
        },
        {
            "_id": ObjectId("55d2db19f4811f83a1f27be9"),
            "year": 2010,
            "host": "South Africa",
            "winner": "Spain",
        },
        {
            "_id": ObjectId("55d2db30f4811f83a1f27bea"),
            "year": 2006,
            "host": "Germany",
            "winner": "France",
        },
    ]


async def test_find_with_filter(async_mongodb):
    docs = async_mongodb.championships.find({"winner": "France"})
    docs_list = []
    async for doc in docs:
        docs_list.append(doc)
    assert docs_list == [
        {
            "_id": ObjectId("608b0151a20cf0c679939f59"),
            "year": 2018,
            "host": "Russia",
            "winner": "France",
        },
        {
            "_id": ObjectId("55d2db30f4811f83a1f27bea"),
            "year": 2006,
            "host": "Germany",
            "winner": "France",
        },
    ]


async def test_find_sorted(async_mongodb):
    docs = async_mongodb.championships.find(sort=[("year", 1)])
    docs_list = []
    async for doc in docs:
        docs_list.append(doc)
    assert docs_list == [
        {
            "_id": ObjectId("55d2db30f4811f83a1f27bea"),
            "year": 2006,
            "host": "Germany",
            "winner": "France",
        },
        {
            "_id": ObjectId("55d2db19f4811f83a1f27be9"),
            "year": 2010,
            "host": "South Africa",
            "winner": "Spain",
        },
        {
            "_id": ObjectId("55d2db06f4811f83a1f27be8"),
            "year": 2014,
            "host": "Brazil",
            "winner": "Germany",
        },
        {
            "_id": ObjectId("608b0151a20cf0c679939f59"),
            "year": 2018,
            "host": "Russia",
            "winner": "France",
        },
    ]


async def test_find_sorted_with_filter(async_mongodb):
    docs = async_mongodb.championships.find(
        filter={"winner": "France"}, sort=[("year", 1)]
    )
    docs_list = []
    async for doc in docs:
        docs_list.append(doc)
    assert docs_list == [
        {
            "_id": ObjectId("55d2db30f4811f83a1f27bea"),
            "year": 2006,
            "host": "Germany",
            "winner": "France",
        },
        {
            "_id": ObjectId("608b0151a20cf0c679939f59"),
            "year": 2018,
            "host": "Russia",
            "winner": "France",
        },
    ]


async def test_bulk_write_and_to_list(async_mongodb):
    await async_mongodb.championships.bulk_write(
        [
            InsertOne({"_id": 1, "a": 22}),
            InsertOne({"_id": 2, "a": 22}),
            InsertOne({"_id": 3, "a": 33}),
        ]
    )
    result = async_mongodb.championships.find({"a": 22})
    docs = await result.to_list()
    assert len(docs) == 2
    assert docs[0]["a"] == 22
    assert docs[1]["a"] == 22


async def test_estimated_document_count(async_mongodb):
    assert await async_mongodb.championships.estimated_document_count() == 4


async def test_find_one_and_update(async_mongodb):
    await async_mongodb.championships.find_one_and_update(
        filter={"_id": ObjectId("608b0151a20cf0c679939f59")},
        update={"$set": {"year": 2022}},
    )
    doc = await async_mongodb.championships.find_one(
        {"_id": ObjectId("608b0151a20cf0c679939f59")}
    )
    assert doc["year"] == 2022


async def test_chained_operations(async_mongodb):
    docs = (
        await async_mongodb.championships.find()
        .sort("year", DESCENDING)
        .skip(1)
        .limit(2)
        .to_list()
    )
    assert len(docs) == 2
    assert docs[0]["year"] == 2014
    assert docs[1]["year"] == 2010
    docs = (
        await async_mongodb.championships.find()
        .sort("year", DESCENDING)
        .skip(3)
        .limit(2)
        .to_list()
    )
    assert len(docs) == 1
    assert docs[0]["year"] == 2006
