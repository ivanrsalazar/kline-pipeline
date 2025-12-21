import asyncio

from ingestion.writers.jsonl_writer import JSONLWriter
from ingestion.writers.s3_jsonl_writer import S3JSONLWriter
from ingestion.exchanges.kraken import KrakenClient
from ingestion.exchanges.binance import BinanceClient
from ingestion.config import PAIRS
from ingestion.utils import now_iso, ts_ms_to_iso
from ingestion.utils import cleanup_local_files



EXCHANGE_MAP = {
    "kraken": KrakenClient,
    "binance": BinanceClient,
}


async def run_pair(pair):
    client = EXCHANGE_MAP[pair.exchange](
        symbol=pair.symbol,
        interval=pair.interval_minutes,
    )

    # ✅ Writers must be created HERE
    local_writer = JSONLWriter(
        base_dir=pair.output_dir,
        prefix=pair.symbol.replace("/", "").lower(),
    )

    s3_writer = S3JSONLWriter(
        bucket="kline-pipeline-bronze"
    )

    while True:
        try:
            await client.connect()
            await client.subscribe()

            async for msg in client.messages():

                # ---------- BINANCE ----------
                if pair.exchange == "binance":
                    k = msg["k"]

                    record = {
                        "exchange": "binance",
                        "symbol": pair.symbol,
                        "interval_minutes": pair.interval_minutes,

                        "event_type": "update",
                        "event_ts": ts_ms_to_iso(msg["E"]),

                        "interval_start": ts_ms_to_iso(k["t"]),
                        "interval_end": ts_ms_to_iso(k["T"]),
                        "is_final": k["x"],

                        "payload": msg,
                    }

                    local_writer.write(record)
                    cleanup_local_files(
                        base_dir=pair.output_dir,
                        active_file=local_writer.current_file,
                        max_age_seconds=6 * 3600
                    )

                    s3_writer.write(record)
                    continue

                # ---------- KRAKEN ----------
                if pair.exchange == "kraken":
                    payload = msg

                    if payload.get("channel") == "status":
                        record = {
                            "exchange": "kraken",
                            "symbol": pair.symbol,
                            "interval_minutes": pair.interval_minutes,

                            "event_type": "status",
                            "event_ts": now_iso(),

                            "interval_start": None,
                            "interval_end": None,
                            "is_final": None,

                            "payload": payload,
                        }

                        local_writer.write(record)
                        cleanup_local_files(
                            base_dir=pair.output_dir,
                            active_file=local_writer.current_file,
                            max_age_seconds=6 * 3600
                        )
                        s3_writer.write(record)
                        continue

                    if payload.get("channel") == "ohlc":
                        event_type = payload.get("type")

                        for candle in payload["data"]:
                            record = {
                                "exchange": "kraken",
                                "symbol": pair.symbol,
                                "interval_minutes": pair.interval_minutes,

                                "event_type": event_type,
                                "event_ts": payload["timestamp"],

                                "interval_start": candle["interval_begin"],
                                "interval_end": candle["timestamp"],
                                "is_final": event_type == "snapshot",

                                "payload": payload,
                            }

                            local_writer.write(record)
                            cleanup_local_files(
                                base_dir=pair.output_dir,
                                active_file=local_writer.current_file,
                                max_age_seconds=6 * 3600
                            )

                            s3_writer.write(record)

        except Exception as e:
            print(f"[{pair.exchange}:{pair.symbol}] error: {e}")
            await asyncio.sleep(5)


async def main():
    await asyncio.gather(*(run_pair(p) for p in PAIRS))
