# A few global config settings
import os
from collections import namedtuple

VideoRung = namedtuple("VideoRung", "height bitrate profile level")
AudioRung = namedtuple("AudioRung", "bitrate")


# Unique identifier for the job
JOB_ID = "avod-conditioned"


# === broadpeak.io ===
# broadpeak.io API key
BPKIO_API_KEY = os.getenv("BPKIO_API_KEY")

# ID of the transcoding profile to use.
# Run this script with the following line commented out to get a profile definition
# that you can pass to your broadpeak.io account manager to add to your account.
# Then, set the identifier in the line below and uncomment it before re-running the script.
# TRANSCODING_PROFILE_ID =

# ID of the asset catalog source to use. Comment out to create a new one automatically
# ASSET_CATALOG_ID =

# ID of the AVOD service. Comment out to create a new one automatically
# AVOD_SERVICE_ID =

# -- Ad Server
# ID of an Ad Proxy (VMAP Generator) source.
# Comment out to create a new one automatically
# AD_SERVER_ID = 60308

# URL of the VAST tag that will be used by the ad proxy (at creation)
VAST_TAG = "https://bpkiovast.s3.eu-west-1.amazonaws.com/vastbpkio10s"


# === Bitmovin ===
# Bitmovin API key and (optional) tenant organization ID
BITMOVIN_API_KEY = os.getenv("BITMOVIN_API_KEY")
BITMOVIN_TENANT_ORG_ID = os.getenv("BITMOVIN_TENANT_ORG_ID")


# === Source File ===
SOURCE_FILE_PATH = (
    "https://bpkioassets.s3-eu-west-1.amazonaws.com/ToS-full-dubbed-subs/"
)
# relative path to the source file (from the SOURCE_FILE_PATH)
SOURCE_FILE_PATH_VIDEO = "TOS-original-24fps-1080p.mp4"
SOURCE_FILE_PATHS_AUDIO = {
    "en": "TOS-original-24fps-1080p.mp4",
    "it": "TOS-dubbed-it.mp3",
}
SOURCE_FILE_PATHS_SUBTITLES = {
    "en": "tears-of-steel-en.srt",
    "fr": "tears-of-steel-fr.srt",
    "de": "tears-of-steel-de.srt",
}

# Bitmovin ID of the HTTPS input that contains the source files.
# Comment out to have one created automatically from the source file path
# HTTPS_INPUT_ID = "4b7bf7e7-f2c3-4d19-b091-96f5aba957a9"

# Ad opportunity placements (expressed in seconds)
SPLICE_POINTS = [69.91, 257.91, 588.40]


# === Origin ===
# Bitmovin ID of the S3 output bucket where the transcoded files will be stored.
# Comment out and fill out the next 3 fields to have one created automatically
# S3_OUTPUT_ID = "e46bc4f9-a793-42a0-a44e-692062063a1c"
S3_OUTPUT_BUCKET_NAME = os.getenv("S3_OUTPUT_BUCKET_NAME")
S3_OUTPUT_ACCESS_KEY = os.getenv("S3_OUTPUT_ACCESS_KEY")
S3_OUTPUT_SECRET_KEY = os.getenv("S3_OUTPUT_SECRET_KEY")

# Root folder under which the outputs will be stored
S3_OUTPUT_BASE_PATH = "/AVOD/"


# === CDN ===
# If a CDN is used to stream the content, provide its FQDN
CDN_FQDN = "my-domain.my-cdn.net"


# === Encoding and Packaging Configuration ===
# WARNING: changing the configuration below may require you to ask for a change in the
# transcoding profile configuration on the broadpeak.io side.
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

# Set FRAME_RATE to None to retain the source frame rate 
# and avoid frame rate conversion (recommended).
FRAME_RATE = 24.0

# Segment duration applies to both HLS and DASH
SEGMENT_DURATION = 4.0


# === Miscellaneous ===
# Specific language labels for subtitles or audio streams,
# for more readible information in players
LANGUAGE_LABELS = dict(
    en="English", it="Italiano", fr="Français", de="Deutsch", ar="عربي"
)
