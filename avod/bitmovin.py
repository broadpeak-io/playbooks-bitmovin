from os import path
from time import sleep
from typing import List, Tuple
from urllib.parse import urlparse

import bitmovin_api_sdk as bm
import config as cfg


class BitmovinController:
    def __init__(self) -> None:
        self.bitmovin_api = bm.BitmovinApi(
            api_key=cfg.BITMOVIN_API_KEY,
            tenant_org_id=getattr(cfg, "BITMOVIN_TENANT_ORG_ID", ""),
            # logger=bm.BitmovinApiLogger(),
        )

        self.encoding_api = self.bitmovin_api.encoding
        self.dash_api = self.bitmovin_api.encoding.manifests.dash
        self.hls_api = self.bitmovin_api.encoding.manifests.hls

        if hasattr(cfg, "HTTPS_INPUT_ID"):
            self.input = self._get_https_input(input_id=cfg.HTTPS_INPUT_ID)
        else:
            self.input = self._create_https_input(source_path=cfg.SOURCE_FILE_PATH)
            print(f"Created HTTPS input with id {self.input.id}")

        if hasattr(cfg, "S3_OUTPUT_ID"):
            self.output = self._get_s3_output(output_id=cfg.S3_OUTPUT_ID)
        else:
            self.output = self._create_s3_output(
                bucket_name=getattr(cfg, "S3_OUTPUT_BUCKET_NAME"),
                access_key=getattr(cfg, "S3_OUTPUT_ACCESS_KEY"),
                secret_key=getattr(cfg, "S3_OUTPUT_SECRET_KEY"),
            )
            print(f"Created S3 output with id {self.output.id}")

    def encode_and_package(
        self,
        name: str,
        source_file_path: str,
        output_sub_path: str,
    ) -> Tuple[bm.Encoding, List[bm.HlsManifest | bm.DashManifest]]:
        encoding = self._create_encoding(name=name, description="")

        # Manifests
        (
            dash_manifest,
            period,
            video_adaptation_set,
            audio_adaptation_set,
        ) = self._generate_dash_manifest(
            output=self.output, output_path=output_sub_path
        )

        hls_manifest = self._generate_hls_manifest(
            output=self.output, output_path=output_sub_path
        )

        # ABR Ladder
        video_configurations = [
            self._create_h264_video_configuration(
                height=r.height,
                bitrate=r.bitrate,
                profile=bm.ProfileH264(r.profile.upper()),
                level=bm.LevelH264(r.level),
                rate=cfg.FRAME_RATE,
            )
            for r in cfg.VIDEO_LADDER
        ]

        audio_configurations = [
            self._create_aac_audio_configuration(bitrate=r.bitrate)
            for r in cfg.AUDIO_LADDER
        ]

        # create video streams, muxings, dash representations and hls variant playlists
        for i, video_config in enumerate(video_configurations):
            h264_video_stream = self._create_stream(
                encoding=encoding,
                input=self.input,
                input_path=source_file_path,
                codec_configuration=video_config,
            )

            relative_path_ts = f"video/{video_config.bitrate}/ts"
            ts_muxing = self._create_ts_muxing(
                encoding=encoding,
                output=self.output,
                output_path=f"{output_sub_path}/{relative_path_ts}",
                stream=h264_video_stream,
            )

            relative_path_fmp4 = f"video/{video_config.bitrate}/fmp4"
            fmp4_muxing = self._create_fmp4_muxing(
                encoding=encoding,
                output=self.output,
                output_path=f"{output_sub_path}/{relative_path_fmp4}",
                stream=h264_video_stream,
            )

            self._add_dash_representation(
                encoding=encoding,
                dash_manifest=dash_manifest,
                period=period,
                adaptation_set=video_adaptation_set,
                fmp4_muxing=fmp4_muxing,
                relative_path=relative_path_fmp4,
            )

            self._add_hls_variant(
                encoding=encoding,
                hls_manifest=hls_manifest,
                stream=h264_video_stream,
                ts_muxing=ts_muxing,
                relative_path=relative_path_ts,
                filename_suffix=f"{video_config.height}p_{video_config.bitrate}",
            )

        # create audio streams and muxings
        for i, audio_config in enumerate(audio_configurations):
            audio_stream = self._create_stream(
                encoding=encoding,
                input=self.input,
                input_path=source_file_path,
                codec_configuration=audio_config,
            )

            relative_path_ts = f"audio/{audio_config.bitrate}/ts"
            ts_muxing = self._create_ts_muxing(
                encoding=encoding,
                output=self.output,
                output_path=f"{output_sub_path}/{relative_path_ts}",
                stream=audio_stream,
            )

            relative_path_fmp4 = f"audio/{audio_config.bitrate}/fmp4"
            fmp4_muxing = self._create_fmp4_muxing(
                encoding=encoding,
                output=self.output,
                output_path=f"{output_sub_path}/{relative_path_fmp4}",
                stream=audio_stream,
            )

            self._add_dash_representation(
                encoding=encoding,
                dash_manifest=dash_manifest,
                period=period,
                adaptation_set=audio_adaptation_set,
                fmp4_muxing=fmp4_muxing,
                relative_path=relative_path_fmp4,
            )

            self._add_hls_media(
                encoding=encoding,
                hls_manifest=hls_manifest,
                stream=audio_stream,
                ts_muxing=ts_muxing,
                relative_path=relative_path_ts,
                filename_suffix=f"{audio_config.bitrate}",
            )

        self._create_keyframes(encoding=encoding, splice_points=cfg.SPLICE_POINTS)

        start_encoding_request = bm.StartEncodingRequest(
            manifest_generator=bm.ManifestGenerator.V2
        )

        start_encoding_request.vod_hls_manifests = [
            bm.ManifestResource(manifest_id=hls_manifest.id)
        ]
        start_encoding_request.vod_dash_manifests = [
            bm.ManifestResource(manifest_id=dash_manifest.id)
        ]

        self._execute_encoding(
            encoding=encoding, start_encoding_request=start_encoding_request
        )

        return (encoding, [hls_manifest, dash_manifest])

    def determine_origin_urls(self, manifests: List[bm.HlsManifest | bm.DashManifest]):
        baseurl = f"https://{self.output.bucket_name}.s3.amazonaws.com/"
        manifest_urls = []

        for manifest in manifests:
            manifest_url = path.join(
                baseurl + manifest.outputs[0].output_path + "/" + manifest.manifest_name
            )

            manifest_urls.append(manifest_url)

        return manifest_urls

    def _poll_encoding_status(self, encoding: bm.Encoding) -> bm.Task:
        sleep(5)
        task = self.encoding_api.encodings.status(encoding_id=encoding.id)
        print(
            "Encoding status is {} (progress: {} %)".format(
                task.status.value, task.progress
            )
        )
        return task

    def _execute_encoding(self, encoding, start_encoding_request):
        self.encoding_api.encodings.start(
            encoding_id=encoding.id, start_encoding_request=start_encoding_request
        )

        task = self._poll_encoding_status(encoding=encoding)

        while task.status not in [
            bm.Status.FINISHED,
            bm.Status.ERROR,
            bm.Status.CANCELED,
        ]:
            task = self._poll_encoding_status(encoding=encoding)

        if task.status is bm.Status.ERROR:
            self._log_task_errors(task=task)
            raise Exception("Encoding failed")

        print("Encoding finished successfully")

    def _create_encoding(self, name: str, description: str) -> bm.Encoding:
        encoding = bm.Encoding(name=name, description=description)

        return self.encoding_api.encodings.create(encoding=encoding)

    def _create_keyframes(self, encoding, splice_points):
        keyframes = []

        for splice_point in splice_points:
            keyframe = bm.Keyframe(time=splice_point, segment_cut=True)

            keyframes.append(
                self.encoding_api.encodings.keyframes.create(
                    encoding_id=encoding.id, keyframe=keyframe
                )
            )

        return keyframes

    def _get_s3_output(self, output_id: str) -> bm.S3Output:
        return self.encoding_api.outputs.s3.get(output_id=output_id)

    def _create_s3_output(
        self, access_key: str, secret_key: str, bucket_name: str
    ) -> bm.S3Output:
        s3_output = bm.S3Output(
            name=bucket_name,
            access_key=access_key,
            secret_key=secret_key,
            bucket_name=bucket_name,
        )

        return self.encoding_api.outputs.s3.create(s3_output=s3_output)

    def _get_https_input(self, input_id: str) -> bm.HttpsInput:
        return self.encoding_api.inputs.https.get(input_id=input_id)

    def _create_https_input(self, source_path: str) -> bm.HttpsInput:
        source_file = urlparse(source_path)

        https_input = bm.HttpsInput(
            host=source_file.hostname, name=source_file.hostname
        )

        return self.encoding_api.inputs.https.create(https_input=https_input)

    def _create_h264_video_configuration(
        self,
        height: int,
        bitrate: int,
        profile: bm.ProfileH264,
        level: bm.LevelH264,
        rate: float,
    ) -> bm.H264VideoConfiguration:
        config = bm.H264VideoConfiguration(
            name="H.264 {0} {1} Mbit/s".format(height, bitrate / (1000 * 1000)),
            preset_configuration=bm.PresetConfiguration.VOD_STANDARD,
            height=height,
            bitrate=bitrate,
            rate=rate,
            profile=profile,
            level=level,
        )

        # For correct alignment between content and ads, it's critical that the Bitmovin
        # encoding abides by the selected profile. The following code makes sure of it,
        # see https://developer.bitmovin.com/encoding/docs/h264-presets#conformance-with-h264-profiles,
        # written by the author when he was working at Bitmovin ;)

        if profile is bm.ProfileH264.BASELINE:
            config.adaptive_spatial_transform = False
            config.bframes = 0
            config.cabac = False
            config.weighted_prediction_p_frames = bm.WeightedPredictionPFrames.DISABLED

        if profile is bm.ProfileH264.MAIN:
            config.adaptive_spatial_transform = False

        if profile is bm.ProfileH264.HIGH:
            config.adaptive_spatial_transform = True

        return self.encoding_api.configurations.video.h264.create(
            h264_video_configuration=config
        )

    def _create_stream(
        self,
        encoding: bm.Encoding,
        input: bm.Input,
        input_path: str,
        codec_configuration: bm.CodecConfiguration,
    ) -> bm.Stream:
        stream_input = bm.StreamInput(
            input_id=input.id,
            input_path=input_path,
            selection_mode=bm.StreamSelectionMode.AUTO,
        )

        stream = bm.Stream(
            input_streams=[stream_input], codec_config_id=codec_configuration.id
        )

        return self.encoding_api.encodings.streams.create(
            encoding_id=encoding.id, stream=stream
        )

    def _create_ts_muxing(
        self,
        encoding: bm.Encoding,
        output: bm.Output,
        output_path: str,
        stream: bm.Stream,
    ) -> bm.TsMuxing:
        muxing = bm.TsMuxing(
            outputs=[
                self._build_encoding_output(output=output, output_path=output_path)
            ],
            segment_length=cfg.SEGMENT_DURATION,
            streams=[bm.MuxingStream(stream_id=stream.id)],
        )

        return self.encoding_api.encodings.muxings.ts.create(
            encoding_id=encoding.id, ts_muxing=muxing
        )

    def _create_fmp4_muxing(
        self,
        encoding: bm.Encoding,
        output: bm.Output,
        output_path: str,
        stream: bm.Stream,
    ) -> bm.Fmp4Muxing:
        muxing = bm.Fmp4Muxing(
            outputs=[
                self._build_encoding_output(output=output, output_path=output_path)
            ],
            segment_length=cfg.SEGMENT_DURATION,
            streams=[bm.MuxingStream(stream_id=stream.id)],
        )

        return self.encoding_api.encodings.muxings.fmp4.create(
            encoding_id=encoding.id, fmp4_muxing=muxing
        )

    def _create_aac_audio_configuration(self, bitrate: int) -> bm.AacAudioConfiguration:
        config = bm.AacAudioConfiguration(
            name="AAC {0} kbit/s".format(bitrate / 1000), bitrate=bitrate
        )

        return self.encoding_api.configurations.audio.aac.create(
            aac_audio_configuration=config
        )

    def _generate_hls_manifest(
        self, output: bm.Output, output_path: str
    ) -> bm.HlsManifest:
        hls_manifest = bm.HlsManifest(
            outputs=[self._build_encoding_output(output, output_path)],
            name="HLS/ts Manifest",
            hls_master_playlist_version=bm.HlsVersion.HLS_V6,
            hls_media_playlist_version=bm.HlsVersion.HLS_V6,
            manifest_name="stream.m3u8",
        )

        return self.hls_api.create(hls_manifest=hls_manifest)

    def _generate_dash_manifest(
        self, output: bm.Output, output_path: str
    ) -> Tuple[
        bm.DashManifest, bm.Period, bm.VideoAdaptationSet, bm.AudioAdaptationSet
    ]:
        dash_manifest = self.dash_api.create(
            dash_manifest=bm.DashManifest(
                name="Single-Period DASH Manifest",
                manifest_name="stream.mpd",
                outputs=[self._build_encoding_output(output, output_path)],
                profile=bm.DashProfile.LIVE,
            )
        )

        period = self.dash_api.periods.create(
            manifest_id=dash_manifest.id, period=bm.Period()
        )

        video_adaptation_set = self.dash_api.periods.adaptationsets.video.create(
            manifest_id=dash_manifest.id,
            period_id=period.id,
            video_adaptation_set=bm.VideoAdaptationSet(),
        )

        audio_adaptation_set = self.dash_api.periods.adaptationsets.audio.create(
            manifest_id=dash_manifest.id,
            period_id=period.id,
            audio_adaptation_set=bm.AudioAdaptationSet(lang="en"),
        )

        return (dash_manifest, period, video_adaptation_set, audio_adaptation_set)

    def _add_dash_representation(
        self,
        encoding: bm.Encoding,
        dash_manifest: bm.DashManifest,
        period: bm.Period,
        adaptation_set: bm.AdaptationSet,
        fmp4_muxing: bm.Fmp4Muxing,
        relative_path: str,
    ) -> bm.DashFmp4Representation:
        representation = bm.DashFmp4Representation(
            type_=bm.DashRepresentationType.TIMELINE,
            encoding_id=encoding.id,
            muxing_id=fmp4_muxing.id,
            segment_path=relative_path,
        )

        return self.dash_api.periods.adaptationsets.representations.fmp4.create(
            manifest_id=dash_manifest.id,
            period_id=period.id,
            adaptationset_id=adaptation_set.id,
            dash_fmp4_representation=representation,
        )

    def _add_hls_variant(
        self,
        encoding: bm.Encoding,
        hls_manifest: bm.HlsManifest,
        stream: bm.Stream,
        ts_muxing: bm.TsMuxing,
        relative_path: str,
        filename_suffix: str,
    ) -> bm.StreamInfo:
        stream_info = bm.StreamInfo(
            audio="AUDIO",
            segment_path=relative_path,
            uri=f"video_{filename_suffix}.m3u8",
            encoding_id=encoding.id,
            stream_id=stream.id,
            muxing_id=ts_muxing.id,
            force_frame_rate_attribute=True,
            force_video_range_attribute=True,
        )

        return self.hls_api.streams.create(
            manifest_id=hls_manifest.id, stream_info=stream_info
        )

    def _add_hls_media(
        self,
        encoding: bm.Encoding,
        hls_manifest: bm.HlsManifest,
        stream: bm.Stream,
        ts_muxing: bm.TsMuxing,
        relative_path: str,
        filename_suffix: str,
    ) -> bm.AudioMediaInfo:
        media_info = bm.AudioMediaInfo(
            name=filename_suffix,
            group_id="AUDIO",
            language="en",
            segment_path=relative_path,
            uri=f"audio_{filename_suffix}.m3u8",
            encoding_id=encoding.id,
            stream_id=stream.id,
            muxing_id=ts_muxing.id,
        )

        return self.hls_api.media.audio.create(
            manifest_id=hls_manifest.id, audio_media_info=media_info
        )

    def _build_encoding_output(
        self, output: bm.Output, output_path: str
    ) -> bm.EncodingOutput:
        acl_entry = bm.AclEntry(permission=bm.AclPermission.PUBLIC_READ)

        return bm.EncodingOutput(
            output_path=self._build_absolute_path(relative_path=output_path),
            output_id=output.id,
            acl=[acl_entry],
        )

    def _build_absolute_path(self, relative_path: str) -> str:
        return path.join(cfg.S3_OUTPUT_BASE_PATH, relative_path)

    def _log_task_errors(self, task: bm.Task) -> None:
        if task is None:
            return

        filtered = [x for x in task.messages if x.type is bm.MessageType.ERROR]

        for message in filtered:
            print(message.text)
