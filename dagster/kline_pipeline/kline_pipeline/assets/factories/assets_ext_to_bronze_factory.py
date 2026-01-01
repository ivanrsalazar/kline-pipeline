from dagster import asset, AssetExecutionContext
import boto3
import json
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta, timezone
import os

PG_DSN = os.environ["PG_DSN"]
S3_BUCKET = "kline-pipeline-bronze"


def make_ext_to_bronze_asset(
    *,
    exchange: str,
    symbol: str,
    s3_prefix: str,  # bucket-relative, up to interval_minutes=1
):
    asset_name = f"bronze_ohlcv_native_{exchange}_{symbol}_1m_v2"

    @asset(
        name=asset_name,
        description=f"S3 → bronze native OHLCV ({exchange})",
        group_name="bronze_native_v2",
        deps=["dummy_asset"],
    )
    def _asset(context: AssetExecutionContext) -> None:
        s3 = boto3.client("s3")
        conn = psycopg2.connect(PG_DSN)
        cur = conn.cursor()

        # -------------------------------------------------
        # Determine LAST COMPLETED UTC HOUR
        # -------------------------------------------------
        hour = (
            datetime.now(tz=timezone.utc)
            .replace(minute=0, second=0, microsecond=0)
            - timedelta(hours=1)
        )

        hour_prefix = (
            f"{s3_prefix}/"
            f"year={hour:%Y}/"
            f"month={hour:%m}/"
            f"day={hour:%d}/"
            f"hour={hour:%H}/"
        )

        context.log.info(
            f"S3 → bronze ingest | exchange={exchange} | hour={hour.isoformat()}"
        )
        context.log.info(
            f"Reading Hive partition: s3://{S3_BUCKET}/{hour_prefix}"
        )

        rows = []
        objects_seen = 0
        skipped = 0
        continuation_token = None

        # -------------------------------------------------
        # List + read ALL part_*.jsonl files for that hour
        # -------------------------------------------------
        while True:
            kwargs = {
                "Bucket": S3_BUCKET,
                "Prefix": hour_prefix,
            }
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token

            resp = s3.list_objects_v2(**kwargs)

            for obj in resp.get("Contents", []):
                objects_seen += 1
                body = s3.get_object(
                    Bucket=S3_BUCKET,
                    Key=obj["Key"],
                )["Body"]

                for line in body.iter_lines():
                    rec = json.loads(line.decode("utf-8"))
                    payload = rec.get("payload", {})

                    # -----------------------------------------
                    # Binance parsing (final candles only)
                    # -----------------------------------------
                    if exchange == "binance":
                        k = payload.get("k")
                        if not k:
                            skipped += 1
                            continue

                        # Only final candles
                        if not k.get("x", False):
                            skipped += 1
                            continue

                        volume = float(k["v"])
                        quote_volume = float(k.get("q", 0))

                        open_ = float(k["o"])
                        high = float(k["h"])
                        low = float(k["l"])
                        close = float(k["c"])
                        trades = int(k["n"])
                        vwap = (
                            quote_volume / volume
                            if volume > 0
                            else None
                        )

                    # -----------------------------------------
                    # Kraken parsing (structural validation)
                    # -----------------------------------------
                    elif exchange == "kraken":
                        data = payload.get("data")
                        if not data or not isinstance(data, list):
                            skipped += 1
                            continue

                        d = data[0]
                        required = {
                            "open",
                            "high",
                            "low",
                            "close",
                            "volume",
                            "vwap",
                            "trades",
                        }

                        if not required.issubset(d):
                            skipped += 1
                            continue

                        open_ = d["open"]
                        high = d["high"]
                        low = d["low"]
                        close = d["close"]
                        volume = d["volume"]
                        vwap = d["vwap"]
                        trades = d["trades"]

                    else:
                        raise ValueError(
                            f"Unsupported exchange: {exchange}"
                        )

                    rows.append(
                        (
                            exchange,
                            rec["symbol"].replace("/", ""),
                            rec["interval_minutes"] * 60,
                            rec["interval_start"],
                            rec["interval_end"],
                            open_,
                            high,
                            low,
                            close,
                            volume,
                            vwap,
                            trades,
                            f"{exchange}_ws",
                            rec["event_ts"],
                        )
                    )

            if not resp.get("IsTruncated"):
                break

            continuation_token = resp["NextContinuationToken"]

        context.log.info(
            f"Hive ingest complete | objects={objects_seen} "
            f"rows={len(rows)} skipped={skipped}"
        )

        # -------------------------------------------------
        # Skip empty hours safely
        # -------------------------------------------------
        if not rows:
            context.log.warning(
                "No valid rows found for this hour — skipping insert"
            )
            cur.close()
            conn.close()
            return

        # -------------------------------------------------
        # Insert into Postgres
        # -------------------------------------------------
        sql = """
        INSERT INTO bronze.bronze_ohlcv_native (
            exchange, symbol, interval_seconds,
            interval_start, interval_end,
            open, high, low, close,
            volume, vwap, trade_count,
            source, ingestion_ts
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        execute_batch(cur, sql, rows, page_size=1000)
        conn.commit()

        context.add_output_metadata(
            {
                "rows_inserted": len(rows),
                "rows_skipped": skipped,
                "s3_objects": objects_seen,
                "hour": hour.isoformat(),
            }
        )

        cur.close()
        conn.close()

    return _asset