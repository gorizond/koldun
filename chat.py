#!/usr/bin/env python3
import json
from typing import Dict, List

import requests

# URL = "http://koldun.localtest.me/v1/chat/completions"
URL = "http://anonymous-koldun.localtest.me/v1/chat/completions"
MODEL = "default/hf-convert-script"
HEADERS = {
    # "KOLDUN_API_TOKEN": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "Content-Type": "application/json",
}


def stream_completion(messages: List[Dict[str, str]]) -> str:
    payload = {"model": MODEL, "messages": messages, "stream": True}
    assistant_chunks: List[str] = []

    with requests.post(URL, headers=HEADERS, json=payload, stream=True) as resp:
        resp.raise_for_status()

        current_event = None
        for raw_line_bytes in resp.iter_lines(decode_unicode=False):
            if raw_line_bytes is None:
                continue

            try:
                raw_line = raw_line_bytes.decode("utf-8")
            except UnicodeDecodeError:
                print("[WARN] не удалось декодировать строку ответа")
                continue

            if raw_line == "":
                continue

            if raw_line.startswith("event:"):
                current_event = raw_line.partition(":")[2].strip()
                continue

            if not raw_line.startswith("data:"):
                continue

            data = raw_line.partition(":")[2].strip()
            if data == "[DONE]":
                break

            if current_event != "message":
                print(f"[{current_event or 'info'}] {data}")
                continue

            try:
                payload = json.loads(data)
            except json.JSONDecodeError:
                print(f"[WARN] не удалось разобрать JSON: {data}")
                continue

            for choice in payload.get("choices", []):
                delta = choice.get("delta", {})
                content = delta.get("content")
                if content:
                    assistant_chunks.append(content)
                    print(content, end="", flush=True)

    print()
    return "".join(assistant_chunks)


def chat_loop():
    messages: List[Dict[str, str]] = []
    print("Введите сообщение. Команда /exit чтобы выйти.")

    while True:
        try:
            user_input = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not user_input:
            continue
        if user_input.lower() in {"/exit", "/quit"}:
            break

        messages.append({"role": "user", "content": user_input})
        assistant_reply = stream_completion(messages)
        if assistant_reply:
            messages.append({"role": "assistant", "content": assistant_reply})


if __name__ == "__main__":
    chat_loop()
