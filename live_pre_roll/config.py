# A few global config settings
import os
from collections import namedtuple

VideoRung = namedtuple("VideoRung", "height bitrate profile level")
AudioRung = namedtuple("AudioRung", "bitrate")


# === broadpeak.io ===
# broadpeak.io API key
BPKIO_API_KEY = os.getenv("BPKIO_API_KEY")

# ID of the transcoding profile to use.
# Talk to your account manager if you don't have a suitable one, and make sure
# that it matches the profile defined below in this file.
TRANSCODING_PROFILE_ID = 1234

# -- Ad Server
# ID of a VMAP-compliant ad server source.
# Comment out to create a new one automatically
# AD_SERVER_ID = 1234

# URL of the VAST tag that will be used by the ad proxy (at creation)
VAST_TAG = "https://bpkiovast.s3.eu-west-1.amazonaws.com/vastbpkio"


# === Bitmovin ===
# Bitmovin API key and (optional) tenant organization ID
BITMOVIN_API_KEY = os.getenv("BITMOVIN_API_KEY")
BITMOVIN_TENANT_ORG_ID = os.getenv("BITMOVIN_TENANT_ORG_ID")


# === Source Stream ===
# Stream Key for the Bitmovin RTMP ingest endpoint
RTMP_STREAM_KEY = "myStreamKey"

# Set this to true to automatically create a dummy stream with ffmpeg
# This is useful for debugging, but not recommended for production
MAKE_DUMMY_FEED_WITH_FFMPEG = True


# === Origin ===
# Bitmovin ID of the S3 output bucket where the transcoded files will be stored.
# Comment out and fill out the next 3 fields to have one created automatically
# S3_OUTPUT_ID = "e46bc4f9-a793-42a0-a44e-692062063a1c"
S3_OUTPUT_BUCKET_NAME = os.getenv("S3_OUTPUT_BUCKET_NAME")
S3_OUTPUT_ACCESS_KEY = os.getenv("S3_OUTPUT_ACCESS_KEY")
S3_OUTPUT_SECRET_KEY = os.getenv("S3_OUTPUT_SECRET_KEY")

# Root folder under which the outputs will be stored
S3_OUTPUT_BASE_PATH = "outputs/live/"


# === CDN ===
# If a CDN is used to stream the content, provide its domain name
CDN_FQDN = "mydistribution.cloudfront.net"


# === Encoding and Packaging Configuration ===
# WARNING: changing the configuration below may require you to ask for a change in the
# transcoding profile configuration on the broadpeak.io side.

FRAME_RATE = 24.0

VIDEO_LADDER = [
    VideoRung(
        height=240,
        bitrate=500_000,
        profile="baseline",
        level="1.3",
    ),
    VideoRung(
        height=360,
        bitrate=1_600_000,
        profile="main",
        level="3",
    ),
    VideoRung(
        height=480,
        bitrate=2_300_000,
        profile="main",
        level="3.1",
    ),
    VideoRung(
        height=720,
        bitrate=3_200_000,
        profile="high",
        level="3.2",
    ),
    VideoRung(
        height=1080,
        bitrate=5_000_000,
        profile="high",
        level="4",
    ),
]
AUDIO_LADDER = [
    AudioRung(bitrate=128_000),
]

SEGMENT_DURATION = 2.0
