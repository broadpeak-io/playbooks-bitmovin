import random
import string
from os import path
from urllib.parse import urlparse

import config as cfg
from bitmovin import BitmovinController
from broadpeak import BroadpeakIOController


def main():
    # Initialising the broadpeak.io APIs
    broadpeakio = BroadpeakIOController()

    # Initalising the Bitmovin SDK
    bitmovin = BitmovinController()

    # Defining some names for resources
    asset_name = path.splitext(path.basename(cfg.SOURCE_FILE_PATH))[0]
    uid = generate_random_string()

    encoding_name = f"Conditioned VOD - {asset_name} - test {uid}"
    output_prefix = f"{asset_name}/{uid}"
    ssai_service_name = f"Bitmovin AVOD w/ VMAP Generator - test {uid}"

    # Encoding and packaging the asset with Bitmovin
    print("Starting the Bitmovin encoder")
    (_, manifests) = bitmovin.encode_and_package(
        name=encoding_name,
        source_file_path=urlparse(cfg.SOURCE_FILE_PATH).path,
        output_sub_path=output_prefix,
    )

    manifest_urls = bitmovin.determine_origin_urls(manifests)
    print("Manifest URLs on the origin: ")
    for url in manifest_urls:
        print(f"- {url}")

    # Creating the broadpeak.io resources
    (_, _, service) = broadpeakio.create_resources(
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


if __name__ == "__main__":
    main()
