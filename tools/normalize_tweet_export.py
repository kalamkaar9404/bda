#!/usr/bin/env python3
"""
Convert a reviewed Twitter/X or TweetClaw CSV export into the 4-column layout
the Kafka producer expects:

    tweet_id, entity, sentiment, text

Usage:
    python tools/normalize_tweet_export.py INPUT.csv \
        --output Kafka-PySpark/twitter_validation.csv

Rows whose sentiment is missing or unrecognised are skipped and reported, so a
raw export without a review step produces an obvious "0 rows written" result
rather than a file full of silently mislabelled tweets.
"""

import argparse
import csv
import sys

# Accepted header spellings for each output column, in order of preference.
FIELD_ALIASES = {
    "tweet_id": ("tweet_id", "tweetid", "id", "id_str", "status_id"),
    "entity": ("entity", "account", "author", "username", "screen_name", "user", "topic"),
    "sentiment": ("sentiment", "label", "class", "category", "polarity"),
    "text": ("text", "tweet", "full_text", "content", "tweet_content", "body"),
}

# The only labels the trained model knows about.
VALID_SENTIMENTS = ("Negative", "Positive", "Neutral", "Irrelevant")

# Common spellings mapped onto those four labels.
SENTIMENT_ALIASES = {
    "negative": "Negative", "neg": "Negative", "-1": "Negative", "bad": "Negative",
    "positive": "Positive", "pos": "Positive", "1": "Positive", "good": "Positive",
    "neutral": "Neutral", "neu": "Neutral", "0": "Neutral",
    "irrelevant": "Irrelevant", "unrelated": "Irrelevant", "off_topic": "Irrelevant",
    "off-topic": "Irrelevant",
}


def _normalise_header(name):
    """Lowercase a header and strip spaces/underscores for alias matching."""
    return (name or "").strip().lower().replace(" ", "_").replace("-", "_")


def resolve_columns(header):
    """Map each output field to its column index in the input header.

    Returns (mapping, missing) where mapping is {field: index}.
    """
    normalised = [_normalise_header(h) for h in header]
    mapping, missing = {}, []

    for field, aliases in FIELD_ALIASES.items():
        for alias in aliases:
            if alias in normalised:
                mapping[field] = normalised.index(alias)
                break
        else:
            missing.append(field)

    return mapping, missing


def normalise_sentiment(value):
    """Return one of the four canonical labels, or None if unrecognised."""
    raw = (value or "").strip()
    if raw in VALID_SENTIMENTS:
        return raw
    return SENTIMENT_ALIASES.get(raw.lower())


def convert(input_path, output_path):
    """Write the normalised CSV and return (written, skipped) counts."""
    with open(input_path, "r", encoding="utf-8", errors="replace", newline="") as src:
        reader = csv.reader(src)
        try:
            header = next(reader)
        except StopIteration:
            print(f"[ERROR] {input_path} is empty.", file=sys.stderr)
            return 0, 0

        mapping, missing = resolve_columns(header)
        if "text" in missing or "sentiment" in missing:
            print(
                f"[ERROR] Could not find required column(s) {missing} in the header:\n"
                f"        {header}\n"
                f"        Expected one of: "
                + "; ".join(f"{f} -> {FIELD_ALIASES[f]}" for f in missing),
                file=sys.stderr,
            )
            return 0, 0

        if missing:
            print(f"[warn] Optional column(s) {missing} not found; defaults will be used.")

        written, skipped = 0, 0
        unknown_labels = set()

        with open(output_path, "w", encoding="utf-8", newline="") as dst:
            writer = csv.writer(dst)

            for line_no, row in enumerate(reader, start=2):
                if not any(cell.strip() for cell in row):
                    continue

                def cell(field, default=""):
                    index = mapping.get(field)
                    if index is None or index >= len(row):
                        return default
                    return row[index].strip()

                text = cell("text")
                sentiment = normalise_sentiment(cell("sentiment"))

                if not text:
                    skipped += 1
                    continue
                if sentiment is None:
                    skipped += 1
                    raw = cell("sentiment")
                    if raw:
                        unknown_labels.add(raw)
                    continue

                writer.writerow([
                    cell("tweet_id", str(line_no)),
                    cell("entity", "unknown"),
                    sentiment,
                    # Newlines would split one tweet across several Kafka messages.
                    " ".join(text.split()),
                ])
                written += 1

        if unknown_labels:
            print(
                "[warn] Skipped rows with unrecognised sentiment values: "
                + ", ".join(sorted(unknown_labels)[:10])
            )
            print(f"       Valid labels are: {', '.join(VALID_SENTIMENTS)}")

        return written, skipped


def main():
    parser = argparse.ArgumentParser(
        description="Normalise a Twitter/X or TweetClaw export for the Kafka producer."
    )
    parser.add_argument("input", help="the exported CSV to read")
    parser.add_argument(
        "--output", "-o",
        default="Kafka-PySpark/twitter_validation.csv",
        help="where to write the 4-column CSV (default: %(default)s)",
    )
    args = parser.parse_args()

    written, skipped = convert(args.input, args.output)
    print(f"[done] Wrote {written} rows to {args.output} ({skipped} skipped).")

    if written == 0:
        print(
            "[ERROR] Nothing was written. Reviewed exports need a sentiment column\n"
            f"        containing one of: {', '.join(VALID_SENTIMENTS)}",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
