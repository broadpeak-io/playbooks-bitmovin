import random
import string
from time import sleep

import config as cfg
import requests
from bitmovin import BitmovinController
from broadpeak import BroadpeakIOController
from ffmpeg import generate_dummy_feed

max_minutes_to_wait_for_manifest_files = 2


def main():
    # Initialising the broadpeak.io APIs
    broadpeakio = BroadpeakIOController()
    ad_server_source = broadpeakio.create_or_retrieve_ad_server()

    # Initalising the Bitmovin SDK
    bitmovin = BitmovinController()

    # Defining some names for resources
    stream_id = generate_random_string()
    encoding_name = f"Live RTMP - test {stream_id}"
    output_prefix = f"{stream_id}"

    print("Starting the Bitmovin encoder")
    (encoding, live_encoding, manifests) = bitmovin.encode_and_package(
        name=encoding_name, output_sub_path=output_prefix
    )

    print("Live encoder is up and ready for ingest.")

    manifest_urls = bitmovin.determine_origin_urls(manifests)
    print("Manifest URLs on the Origin: ")
    for url in manifest_urls:
        print(f"- {url}")

    ffmpeg_process = None
    if cfg.MAKE_DUMMY_FEED_WITH_FFMPEG:
        print("Starting FFmpeg to push a dummy RTMP stream to the live encoder")
        ffmpeg_process = generate_dummy_feed(
            rtmp_endpoint=live_encoding.encoder_ip,
            stream_key=live_encoding.stream_key,
            stream_id=stream_id,
            rate=cfg.FRAME_RATE,
        )
    else:
        print(
            "Send an RTMP stream to rtmp://{ip}/live with stream key {key}"
            "and frame rate {rate}".format(
                ip=live_encoding.encoder_ip,
                key=live_encoding.stream_key,
                rate=cfg.FRAME_RATE,
            )
        )

    print("Waiting for manifests to be ready on the Origin")
    wait_until_manifest_files_are_ready(manifest_urls)

    print("Creating the broadpeak.io SSAI service")
    streaming_urls = []
    for url in manifest_urls:
        format = "HLS"
        
        live_source = broadpeakio.create_live_source(
            name=f"Bitmovin Live - {stream_id} - {format}", url=url
        )

        preroll_service = broadpeakio.create_preroll_service(
            name=f"Bitmovin Live w/ PreRoll - {stream_id} - {format}",
            ad_server_id=ad_server_source["id"],
            live_source_id=live_source["id"],
            transcoding_profile_id=cfg.TRANSCODING_PROFILE_ID,
        )

        streaming_url = preroll_service["url"]
        if hasattr(cfg, "CDN_FQDN"):
            streaming_url = streaming_url.replace("stream.broadpeak.io", cfg.CDN_FQDN)
        streaming_urls.append(streaming_url)

    print("broadpeak.io streaming URLs:")
    for url in streaming_urls:
        print(f"- {url}")

    try:
        print("Press Ctrl+C to shutdown the live encoding...")
        while True:
            pass  # Keep script running

    except KeyboardInterrupt:
        print("Shutting down live encoding.")
        if ffmpeg_process:
            ffmpeg_process.kill()

        bitmovin.stop_encoding(encoding)

        print("All done!")
        print(
            "Note: to be able to re-run this script with error, "
            "plug the relevant Bitmovin and broadpeak.io resource IDs listed above "
            "into the appropriate constants in the config.py file"
        )


def wait_until_manifest_files_are_ready(manifest_urls):
    check_interval_in_seconds = 5
    max_attempts = max_minutes_to_wait_for_manifest_files * (
        60 / check_interval_in_seconds
    )
    attempt = 0

    while attempt < max_attempts:
        response_codes = []
        for url in manifest_urls:
            response = requests.get(url)
            response_codes.append(response.status_code)

        if all(code == 200 for code in response_codes):
            return

        sleep(check_interval_in_seconds)
        attempt += 1

    raise Exception(
        "Manifest files did not become ready after {0} minutes. Aborting.".format(
            max_minutes_to_wait_for_manifest_files
        )
    )


def generate_random_string(length=8):
    source = string.ascii_letters + string.digits
    result_str = "".join((random.choice(source) for i in range(length)))
    return result_str


if __name__ == "__main__":
    main()
