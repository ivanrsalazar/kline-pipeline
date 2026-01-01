import os
import requests


def send_slack_message(text: str) -> None:
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook:
        raise RuntimeError("SLACK_WEBHOOK_URL is not set")

    payload = {
        "text": text
    }

    resp = requests.post(webhook, json=payload, timeout=10)
    resp.raise_for_status()