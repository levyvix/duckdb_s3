import json
import os

import pandas as pd
import requests
from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
    asset_check,
)


@asset
def topstory_ids():
    newstories_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    top_new_story_ids = requests.get(newstories_url).json()[:100]

    os.makedirs("data", exist_ok=True)
    with open("data/topstory_ids.json", "w") as f:
        json.dump(top_new_story_ids, f)


@asset(deps=[topstory_ids])  # this asset is dependent on topstory_ids
def topstories(context: AssetExecutionContext) -> MaterializeResult:
    with open("data/topstory_ids.json", "r") as f:
        topstory_ids = json.load(f)

    results = []
    for item_id in topstory_ids:
        item = requests.get(
            f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
        ).json()
        results.append(item)

        if len(results) % 20 == 0:
            context.log.info(f"Got {len(results)} items so far.")

    df = pd.DataFrame(results)
    df.to_csv("data/topstories.csv", index=False)

    return MaterializeResult(
        metadata={
            "num_records": len(df),  # Metadata can be any key-value pair
            "preview": MetadataValue.md(df.head().to_markdown()),
            # The `MetadataValue` class has useful static methods to build Metadata
        }
    )


@asset_check(asset=topstories)
def topstories_not_empty():
    df = pd.read_csv("data/topstories.csv")
    row_count = len(df)

    return AssetCheckResult(
        passed=bool(row_count > 0),
    )


@asset(deps=[topstories])
def most_frequent_words() -> MaterializeResult:
    stopwords = [
        "a",
        "the",
        "an",
        "of",
        "to",
        "in",
        "for",
        "and",
        "with",
        "on",
        "is",
        "-",
    ]

    topstories_df = pd.read_csv("data/topstories.csv")

    word_counts = {}
    for raw_title in topstories_df["title"]:
        title = raw_title.lower()
        for word in title.split():
            cleaned_word = word.strip(".,-!?:;()[]'\"-")
            if cleaned_word not in stopwords and len(cleaned_word) > 0:
                word_counts[cleaned_word] = word_counts.get(cleaned_word, 0) + 1

    # # Get the top 25 most frequent words
    top_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:25]

    with open("data/most_frequent_words.json", "w") as f:
        json.dump(top_words, f)

    return MaterializeResult(
        metadata={
            "top_words": MetadataValue.json(top_words),
            "preview": MetadataValue.md(
                pd.DataFrame(top_words, columns=["Word", "Count"]).to_markdown()
            ),
        }
    )
