import json
import sys
from typing import Dict
from urllib.parse import urljoin, urlparse

import config as cfg
import requests


class BroadpeakIOController:
    def __init__(self) -> None:
        self.headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "authorization": f"Bearer {cfg.BPKIO_API_KEY}",
        }

        if not hasattr(cfg, "TRANSCODING_PROFILE_ID"):
            print("You need to provide a Transcoding Profile ID in the config file")
            self.build_transcoding_profile_config()
            sys.exit(1)

    def create_or_retrieve_ad_server(self) -> Dict:
        if hasattr(cfg, "AD_SERVER_ID"):
            ad_server = self._get_ad_server(getattr(cfg, "AD_SERVER_ID"))
        else:
            ad_server = self._create_ad_server(vast_tag=cfg.VAST_TAG)

        return ad_server

    def _create_ad_server(self, vast_tag: str) -> Dict:
        url = urlparse(vast_tag)
        base_url = urljoin(vast_tag, url.path)

        ad_proxy_payload = {
            "name": "VAST Ad Server",
            "template": "custom",
            "url": base_url,
            "queries": url.query,
        }

        return self._post_wrapper(
            endpoint_url="https://api.broadpeak.io/v1/sources/ad-server",
            payload=ad_proxy_payload,
        )

    def _get_ad_server(self, id: int) -> Dict:
        return self._get_wrapper(
            endpoint_url=f"https://api.broadpeak.io/v1/sources/ad-server/{id}"
        )

    def create_preroll_service(
        self,
        name: str,
        live_source_id: int,
        ad_server_id: int,
        transcoding_profile_id: int,
    ) -> Dict:
        adinsertion_service_payload = {
            "name": name,
            "source": {"id": live_source_id},
            "liveAdPreRoll": {"adServer": {"id": ad_server_id}},
            "transcodingProfile": {"id": transcoding_profile_id},
            "enableAdTranscoding": True,
        }

        return self._post_wrapper(
            endpoint_url="https://api.broadpeak.io/v1/services/ad-insertion",
            payload=adinsertion_service_payload,
        )

    def create_live_source(self, name: str, url: str) -> Dict:
        source_payload = {"name": name, "url": url}
        return self._post_wrapper(
            endpoint_url="https://api.broadpeak.io/v1/sources/live",
            payload=source_payload,
        )

    def _get_wrapper(self, endpoint_url: str):
        response = requests.get(endpoint_url, headers=self.headers)
        if response.status_code != 200:
            raise Exception(f"Unable to retrieve {endpoint_url}: " + response.text)

        return response.json()

    def _post_wrapper(self, endpoint_url: str, payload: Dict):
        response = requests.post(endpoint_url, json=payload, headers=self.headers)
        if response.status_code != 201:
            raise Exception(f"Unable to create {endpoint_url}: " + response.text)
        else:
            j = response.json()
            print(f"Created resource on {endpoint_url} with id {j['id']}")
            return j

    def build_transcoding_profile_config(self):
        config = {
            "packaging": {
                "--hls.client_manifest_version=": "4",
                "--hls.minimum_fragment_length=": "4",
            },
            "servicetype": "offline_transcoding",
            "transcoding": {
                "jobs": [
                    {
                        "level": str(r.level),
                        "scale": f"-2:{r.height}",
                        "bitratev": str(r.bitrate),
                        "profilev": r.profile,
                        "frameratev": str(cfg.FRAME_RATE),
                    }
                    for r in cfg.VIDEO_LADDER
                ],
                "common": {
                    "codeca": "aac",
                    "codecv": "h264",
                    "preset": "veryfast",
                    "bitratea": str(cfg.AUDIO_LADDER[0].bitrate),
                    "loudnorm": "I=-23:TP=-1",
                },
            },
        }

        print(
            "Ask your broadpeak.io account manager "
            "to add the following profile to your account"
        )
        print(json.dumps(config, indent=4))
