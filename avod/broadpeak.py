import json
import sys
from typing import Dict, List, Tuple
from urllib.parse import urlparse

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

    def create_resources(
        self, service_name: str, origin_urls: List[str]
    ) -> Tuple[Dict, Dict, Dict]:
        """Create resources for the broadpeak.io service"""
        if hasattr(cfg, "AD_SERVER_ID"):
            ad_server = self._get_ad_server(getattr(cfg, "AD_SERVER_ID"))
        else:
            ad_server = self._create_ad_server_ad_proxy_vmap_gen(vast_tag=cfg.VAST_TAG)

        if hasattr(cfg, "ASSET_CATALOG_ID"):
            asset_catalog = self._get_asset_catalog(getattr(cfg, "ASSET_CATALOG_ID"))
        else:
            asset_catalog = self._create_asset_catalog(origin_urls=origin_urls)

        if hasattr(cfg, "AVOD_SERVICE_ID"):
            avod_service = self._get_avod_service(getattr(cfg, "AVOD_SERVICE_ID"))
        else:
            avod_service = self._create_avod_service(
                service_name=service_name,
                ad_server_id=ad_server["id"],
                asset_catalog_id=asset_catalog["id"],
                transcoding_profile_id=cfg.TRANSCODING_PROFILE_ID,
            )

        return (ad_server, asset_catalog, avod_service)

    def _create_asset_catalog(self, origin_urls: List[str]) -> Dict:
        # Define source URL from origin_urls
        url = origin_urls[0]
        pos = url.find(cfg.S3_OUTPUT_BASE_PATH) + len(cfg.S3_OUTPUT_BASE_PATH)

        asset_catalog_payload = {
            "name": "Bitmovin AVOD outputs",
            "url": url[:pos],
            "assetSample": url[pos:],
        }

        return self._post_wrapper(
            endpoint_url="https://api.broadpeak.io/v1/sources/asset-catalog",
            payload=asset_catalog_payload,
        )

    def _get_asset_catalog(self, id: int):
        return self._get_wrapper(
            endpoint_url=f"https://api.broadpeak.io/v1/sources/asset-catalog/{id}"
        )

    def _create_ad_server_ad_proxy_vmap_gen(self, vast_tag: str):
        ad_proxy_payload = {
            "name": "AdProxy VMAP Generator",
            "template": "ad-proxy-vmap-generator",
            "queries": "&".join(
                [
                    "bpkio_pre=true",
                    "bpkio_post=true",
                    "bpkio_mids=$arg_bpkio_mids",
                    f"bpkio_tag={vast_tag}",
                ]
            ),
        }

        return self._post_wrapper(
            endpoint_url="https://api.broadpeak.io/v1/sources/ad-server",
            payload=ad_proxy_payload,
        )

    def _get_ad_server(self, id: int):
        return self._get_wrapper(
            endpoint_url=f"https://api.broadpeak.io/v1/sources/ad-server/{id}"
        )

    def _create_avod_service(
        self,
        service_name: str,
        asset_catalog_id: int,
        ad_server_id: int,
        transcoding_profile_id: int,
    ):
        adinsertion_service_payload = {
            "name": service_name,
            "source": {"id": asset_catalog_id},
            "vodAdInsertion": {"adServer": {"id": ad_server_id}},
            "transcodingProfile": {"id": transcoding_profile_id},
            "enableAdTranscoding": True,
        }

        return self._post_wrapper(
            endpoint_url="https://api.broadpeak.io/v1/services/ad-insertion",
            payload=adinsertion_service_payload,
        )

    def _get_avod_service(self, id: int):
        return self._get_wrapper(
            endpoint_url=f"https://api.broadpeak.io/v1/services/ad-insertion/{id}"
        )

    def calculate_streaming_urls(
        self, service_id: int, origin_manifest_urls: List[str]
    ) -> List[str]:
        streaming_urls = []
        adinsertion_service = self._get_avod_service(service_id)

        for origin_url in origin_manifest_urls:
            service_url = adinsertion_service["url"]
            if hasattr(cfg, "CDN_FQDN"):
                service_url = service_url.replace("stream.broadpeak.io", cfg.CDN_FQDN)

            origin_url = urlparse(origin_url)
            source_url = urlparse(adinsertion_service["source"]["url"])

            full_url = "{base}{asset}?bpkio_mids={mids}".format(
                base=service_url,
                asset=origin_url.path.replace(source_url.path, ""),
                mids=",".join([str(i) for i in cfg.SPLICE_POINTS]),
            )
            streaming_urls.append(full_url)

        return streaming_urls

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
            "Ask your broadpeak.io account manager to add the following profile to your account"
        )
        print(json.dumps(config, indent=4))
