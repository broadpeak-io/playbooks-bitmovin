import argparse
import importlib
import random
import string
from os import path
from urllib.parse import urlparse

from bitmovin import BitmovinController
from broadpeak import BroadpeakIOController


def main():
    args = parse_arguments()

    cfg = importlib.import_module(args.config)

    # Initialising the broadpeak.io APIs
    broadpeakio = BroadpeakIOController(config=cfg)

    # Initalising the Bitmovin SDK
    bitmovin = BitmovinController(config=cfg)

    # Defining some names for resources
    asset_name = path.splitext(path.basename(cfg.SOURCE_FILE_PATH_VIDEO))[0]
    if hasattr(cfg, "JOB_ID"):
        uid = cfg.JOB_ID
    else:
        uid = generate_random_string()

    encoding_name = f"{asset_name} - {uid}"
    output_prefix = f"{asset_name}/{uid}"
    ssai_service_name = f"AVOD w/ Bitmovin encoding and Ad Proxy - {uid}"

    # Encoding and packaging the asset with Bitmovin
    print("Configuring and starting the Bitmovin encoder")
    (_, manifests) = bitmovin.encode_and_package(
        name=encoding_name,
        source_path=urlparse(cfg.SOURCE_FILE_PATH).path,
        source_video_file=cfg.SOURCE_FILE_PATH_VIDEO,
        source_audio_files=cfg.SOURCE_FILE_PATHS_AUDIO,
        source_subtitle_files=cfg.SOURCE_FILE_PATHS_SUBTITLES,
        output_sub_path=output_prefix,
    )

    # List the outputs
    print("Outputs:")
    manifest_urls = []
    for manifest in manifests:
        manifest_url = bitmovin.determine_origin_url(manifest)
        print("Manifest URL: " + manifest_url)
        manifest_urls.append(manifest_url)

    # Creating the broadpeak.io resources
    print("broadpeak.io resources:")
    (ad_server, asset_catalog, service) = broadpeakio.create_resources(
        origin_urls=manifest_urls, service_name=ssai_service_name
    )

    # Calculating the streaming URLs
    streaming_urls = broadpeakio.calculate_streaming_urls(
        service_id=service["id"], origin_manifest_urls=manifest_urls
    )

    print("broadpeak.io streaming URLs: ")
    for url in streaming_urls:
        print(f"- {url}")

    print("All done!")
    print(
        "Note: to be able to re-run this script with error, "
        "plug the relevant Bitmovin and broadpeak.io resource IDs listed above "
        "into the appropriate constants in the config.py file"
    )


# get random string of letters and digits
def generate_random_string(length=8):
    source = string.ascii_letters + string.digits
    result_str = "".join((random.choice(source) for i in range(length)))
    return result_str


# parse arguments with argparse
def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="path to config file", default="config")
    return parser.parse_args()


if __name__ == "__main__":
    main()
