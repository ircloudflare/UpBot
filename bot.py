"""Telegram file -> download link bot.

Telegram (MTProto, bot token + api_id/api_hash) -> streamed multipart upload to an
S3-compatible bucket (Sevalla object storage / Cloudflare R2) -> presigned link.
Files are deleted from the bucket after FILE_TTL_HOURS.
All configuration comes from environment variables.
"""
import asyncio
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from uuid import uuid4

import boto3
from botocore.config import Config
from telethon import TelegramClient, events
from telethon.sessions import StringSession

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
log = logging.getLogger("filelink")


# ----------------------------------------------------------------- config
def env(name, default=None, required=False):
    value = os.environ.get(name, default)
    if required and not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


API_ID = int(env("API_ID", required=True))
API_HASH = env("API_HASH", required=True)
BOT_TOKEN = env("BOT_TOKEN", required=True)

S3_ENDPOINT = env("S3_ENDPOINT", required=True)
S3_BUCKET = env("S3_BUCKET", required=True)  # the *bucket name*, not the display name
S3_ACCESS_KEY_ID = env("S3_ACCESS_KEY_ID", required=True)
S3_SECRET_ACCESS_KEY = env("S3_SECRET_ACCESS_KEY", required=True)
S3_REGION = env("S3_REGION", "auto")

# Comma-separated Telegram user IDs allowed to use the bot.
ALLOWED = {int(x) for x in re.split(r"[,\s]+", env("ALLOWED_USER_IDS", "")) if x}

FILE_TTL_HOURS = float(env("FILE_TTL_HOURS", "6"))  # link expiry == deletion age
CLEANUP_EVERY_MIN = float(env("CLEANUP_EVERY_MIN", "30"))
PART_SIZE = max(5, int(env("PART_SIZE_MB", "8"))) * 1024 * 1024  # R2 min part = 5 MiB
MAX_BYTES = int(float(env("MAX_FILE_GB", "4")) * 1024**3)
MAX_CONCURRENT = int(env("MAX_CONCURRENT", "2"))
HEALTH_PORT = env("HEALTH_PORT") or env("PORT")  # optional tiny HTTP 200 endpoint

REQUEST_SIZE = 512 * 1024  # bytes per MTProto download request

# ----------------------------------------------------------------- clients
s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY_ID,
    aws_secret_access_key=S3_SECRET_ACCESS_KEY,
    region_name=S3_REGION,
    config=Config(
        signature_version="s3v4",
        retries={"max_attempts": 5, "mode": "standard"},
        # R2 rejects the default CRC checksums newer boto3 versions add.
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
    ),
)

client = TelegramClient(StringSession(), API_ID, API_HASH)
client.parse_mode = None  # plain text: presigned URLs contain markdown-ish chars
sem = asyncio.Semaphore(MAX_CONCURRENT)


# ----------------------------------------------------------------- helpers
def human(n):
    n = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024


def safe_name(name):
    name = os.path.basename(name or "").strip() or "file"
    name = re.sub(r"[\x00-\x1f\x7f/\\]", "_", name)
    return name[-150:]


def make_link(key, name):
    return s3.generate_presigned_url(
        "get_object",
        Params={
            "Bucket": S3_BUCKET,
            "Key": key,
            "ResponseContentDisposition": f"attachment; filename*=UTF-8''{quote(name)}",
        },
        ExpiresIn=int(FILE_TTL_HOURS * 3600),  # S3/R2 max is 7 days
    )


# ----------------------------------------------------------------- transfer
async def transfer(msg, progress):
    """Stream the message's file from Telegram into the bucket. Returns (key, name, size)."""
    f = msg.file
    size = f.size
    name = safe_name(f.name or f"file{f.ext or ''}")
    key = f"{uuid4().hex}/{name}"

    mp = await asyncio.to_thread(
        s3.create_multipart_upload,
        Bucket=S3_BUCKET,
        Key=key,
        ContentType=f.mime_type or "application/octet-stream",
    )
    upload_id = mp["UploadId"]
    queue: asyncio.Queue = asyncio.Queue(maxsize=2)  # download runs ahead of upload

    async def producer():
        buf = bytearray()
        try:
            async for chunk in client.iter_download(
                msg.media, request_size=REQUEST_SIZE, file_size=size
            ):
                buf += chunk
                # R2 needs every part except the last to be the same size.
                while len(buf) >= PART_SIZE:
                    await queue.put(bytes(buf[:PART_SIZE]))
                    del buf[:PART_SIZE]
            if buf:
                await queue.put(bytes(buf))
            await queue.put(None)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # hand the error to the consumer
            await queue.put(exc)

    prod = asyncio.create_task(producer())
    parts, sent, ok = [], 0, False
    try:
        while True:
            item = await queue.get()
            if item is None:
                break
            if isinstance(item, Exception):
                raise item
            number = len(parts) + 1
            r = await asyncio.to_thread(
                s3.upload_part,
                Bucket=S3_BUCKET,
                Key=key,
                UploadId=upload_id,
                PartNumber=number,
                Body=item,
            )
            parts.append({"ETag": r["ETag"], "PartNumber": number})
            sent += len(item)
            await progress(sent)
        await prod
        if not parts:
            raise RuntimeError("Telegram returned an empty file")
        await asyncio.to_thread(
            s3.complete_multipart_upload,
            Bucket=S3_BUCKET,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
        ok = True
        return key, name, size
    finally:
        if not ok:
            prod.cancel()
            try:
                await asyncio.to_thread(
                    s3.abort_multipart_upload,
                    Bucket=S3_BUCKET,
                    Key=key,
                    UploadId=upload_id,
                )
            except Exception:
                log.exception("abort_multipart_upload failed")


# ----------------------------------------------------------------- handlers
HELP = (
    "Send me any file (up to {max}) and I'll reply with a download link.\n"
    "The link and the file expire after {ttl:g} hours.\n\n"
    "Your Telegram ID: {uid}"
)


@client.on(events.NewMessage(incoming=True, func=lambda e: e.is_private))
async def on_message(event):
    uid = event.sender_id

    if ALLOWED and uid not in ALLOWED:
        await event.reply(
            f"Not authorized. Your Telegram ID is {uid}; ask the owner to add it."
        )
        return
    if not ALLOWED:
        # Nobody configured yet: only tell the user their ID so they can set it.
        await event.reply(
            f"Bot not configured yet. Set ALLOWED_USER_IDS={uid} in the environment variables."
        )
        return

    msg = event.message
    if not msg.file:
        await event.reply(
            HELP.format(max=human(MAX_BYTES), ttl=FILE_TTL_HOURS, uid=uid)
        )
        return

    if msg.file.size > MAX_BYTES:
        await event.reply(f"File too large ({human(msg.file.size)}). Max is {human(MAX_BYTES)}.")
        return

    status = await event.reply(
        "Queued..." if sem.locked() else "Starting..."
    )
    async with sem:
        last = 0.0
        total = msg.file.size

        async def progress(done):
            nonlocal last
            now = time.monotonic()
            if now - last < 5:
                return
            last = now
            try:
                await status.edit(
                    f"Uploading... {done * 100 // total}% ({human(done)} / {human(total)})"
                )
            except Exception:
                pass  # edit throttling / not modified: ignore

        try:
            key, name, size = await transfer(msg, progress)
            url = make_link(key, name)
        except Exception:
            log.exception("transfer failed for user %s", uid)
            await status.edit("Transfer failed. Please try again.")
            return

    try:
        await status.delete()
    except Exception:
        pass
    await event.reply(
        f"{name} ({human(size)})\n\n{url}\n\n"
        f"Link and file expire in {FILE_TTL_HOURS:g} hours.",
        link_preview=False,
    )


# ----------------------------------------------------------------- cleanup
def cleanup_once():
    cutoff = datetime.now(timezone.utc) - timedelta(hours=FILE_TTL_HOURS)
    batch, deleted = [], 0

    def flush():
        nonlocal batch, deleted
        if not batch:
            return
        try:
            s3.delete_objects(
                Bucket=S3_BUCKET, Delete={"Objects": batch, "Quiet": True}
            )
        except Exception:
            log.exception("delete_objects failed, falling back to single deletes")
            for obj in batch:
                s3.delete_object(Bucket=S3_BUCKET, Key=obj["Key"])
        deleted += len(batch)
        batch = []

    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=S3_BUCKET):
        for obj in page.get("Contents", []):
            if obj["LastModified"] < cutoff:
                batch.append({"Key": obj["Key"]})
                if len(batch) >= 1000:
                    flush()
    flush()
    return deleted


async def cleanup_loop():
    while True:
        try:
            n = await asyncio.to_thread(cleanup_once)
            if n:
                log.info("cleanup: deleted %d expired object(s)", n)
        except Exception:
            log.exception("cleanup failed")
        await asyncio.sleep(CLEANUP_EVERY_MIN * 60)


# ----------------------------------------------------------------- health (optional)
async def health_server(port):
    async def handle(reader, writer):
        try:
            await reader.read(1024)
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
                b"Content-Length: 2\r\nConnection: close\r\n\r\nok"
            )
            await writer.drain()
        finally:
            writer.close()

    await asyncio.start_server(handle, "0.0.0.0", int(port))
    log.info("health endpoint listening on :%s", port)


# ----------------------------------------------------------------- main
async def main():
    if not ALLOWED:
        log.warning("ALLOWED_USER_IDS is empty: the bot will only reply with the sender's ID")
    if HEALTH_PORT:
        await health_server(HEALTH_PORT)
    await client.start(bot_token=BOT_TOKEN)
    me = await client.get_me()
    log.info("bot @%s started", me.username)
    asyncio.create_task(cleanup_loop())
    await client.run_until_disconnected()


if __name__ == "__main__":
    client.loop.run_until_complete(main())
