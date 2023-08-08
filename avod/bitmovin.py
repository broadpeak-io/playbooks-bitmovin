import os
from os import path
from time import sleep
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import bitmovin_api_sdk as bm


class BitmovinController:
    def __init__(self, config) -> None:
        self.config = config
        self.bitmovin_api = bm.BitmovinApi(
            api_key=self.config.BITMOVIN_API_KEY,
            tenant_org_id=getattr(self.config, "BITMOVIN_TENANT_ORG_ID", ""),
            # logger=bm.BitmovinApiLogger(),
        )

        self.encoding_api = self.bitmovin_api.encoding
        self.dash_api = self.bitmovin_api.encoding.manifests.dash
        self.hls_api = self.bitmovin_api.encoding.manifests.hls

        if hasattr(self.config, "HTTPS_INPUT_ID"):
            self.input = self._get_https_input(input_id=self.config.HTTPS_INPUT_ID)
        else:
            self.input = self._create_https_input(
                source_path=self.config.SOURCE_FILE_PATH
            )
            print(f"Created HTTPS input with id {self.input.id}")

        if hasattr(self.config, "S3_OUTPUT_ID"):
            self.output = self._get_s3_output(output_id=self.config.S3_OUTPUT_ID)
        else:
            self.output = self._create_s3_output(
                bucket_name=getattr(self.config, "S3_OUTPUT_BUCKET_NAME"),
                access_key=getattr(self.config, "S3_OUTPUT_ACCESS_KEY"),
                secret_key=getattr(self.config, "S3_OUTPUT_SECRET_KEY"),
            )
            print(f"Created S3 output with id {self.output.id}")

    def encode_and_package(
        self,
        name: str,
        source_path: str,
        source_video_file: str,
        source_audio_files: Dict[str, str],
        source_subtitle_files: Dict[str, str],
        output_sub_path: str,
    ) -> Tuple[bm.Encoding, List[bm.HlsManifest | bm.DashManifest]]:
        self.encoding = self._create_encoding(name=name, description="")

        # Manifests
        (
            dash_manifest,
            period,
        ) = self._generate_dash_manifest_with_single_period(
            output_path=output_sub_path,
        )

        hls_manifest = self._generate_hls_manifest(output_path=output_sub_path)

        # ABR Ladder
        video_configurations = [
            self._create_h264_video_configuration(
                height=r.height,
                bitrate=r.bitrate,
                profile=bm.ProfileH264(r.profile.upper()),
                level=bm.LevelH264(r.level),
                rate=self.config.FRAME_RATE,
            )
            for r in self.config.VIDEO_LADDER
        ]

        audio_configurations = [
            self._create_aac_audio_configuration(bitrate=r.bitrate)
            for r in self.config.AUDIO_LADDER
        ]

        webvtt_subtitle_configuration = self._create_webvtt_configuration()
        sidecars = {}

        # create video stream, muxings, dash representations and hls variant playlists
        video_adaptation_set = self._add_video_adaptation_set(
            dash_manifest=dash_manifest, period=period
        )
        for i, video_config in enumerate(video_configurations):
            h264_video_stream = self._create_stream(
                input_path=os.path.join(source_path, source_video_file),
                codec_configuration=video_config,
            )

            relative_path_ts = f"video/{video_config.bitrate}/ts"
            ts_muxing = self._create_ts_muxing(
                output_path=f"{output_sub_path}/{relative_path_ts}",
                stream=h264_video_stream,
            )

            relative_path_fmp4 = f"video/{video_config.bitrate}/fmp4"
            fmp4_muxing = self._create_fmp4_muxing(
                output_path=f"{output_sub_path}/{relative_path_fmp4}",
                stream=h264_video_stream,
            )

            self._add_dash_fmp4_representation(
                dash_manifest=dash_manifest,
                period=period,
                adaptation_set=video_adaptation_set,
                fmp4_muxing=fmp4_muxing,
                relative_path=relative_path_fmp4,
            )

            self._add_hls_variant(
                hls_manifest=hls_manifest,
                stream=h264_video_stream,
                ts_muxing=ts_muxing,
                relative_path=relative_path_ts,
                filename_suffix=f"{video_config.height}p_{video_config.bitrate}",
            )

        # create audio streams and muxings, dash representations and hls media playlists
        for lang, source_audio_file in source_audio_files.items():
            audio_adaptation_set = self._add_audio_adaptation_set(
                dash_manifest=dash_manifest,
                period=period,
                lang=self._make_language_label(lang),
            )

            for i, audio_config in enumerate(audio_configurations):
                audio_stream = self._create_stream(
                    input_path=os.path.join(source_path, source_audio_file),
                    codec_configuration=audio_config,
                    language=lang,
                )

                relative_path_ts = f"audio_{lang}/{audio_config.bitrate}/ts"
                ts_muxing = self._create_ts_muxing(
                    output_path=f"{output_sub_path}/{relative_path_ts}",
                    stream=audio_stream,
                )

                relative_path_fmp4 = f"audio_{lang}/{audio_config.bitrate}/fmp4"
                fmp4_muxing = self._create_fmp4_muxing(
                    output_path=f"{output_sub_path}/{relative_path_fmp4}",
                    stream=audio_stream,
                )

                self._add_dash_fmp4_representation(
                    dash_manifest=dash_manifest,
                    period=period,
                    adaptation_set=audio_adaptation_set,
                    fmp4_muxing=fmp4_muxing,
                    relative_path=relative_path_fmp4,
                )

                self._add_hls_media(
                    hls_manifest=hls_manifest,
                    stream=audio_stream,
                    ts_muxing=ts_muxing,
                    relative_path=relative_path_ts,
                    filename_suffix=f"{audio_config.bitrate}",
                    language=lang,
                    label=self._make_language_label(lang),
                )

        # create subtitle streams and muxings
        for lang, source_sub_file in source_subtitle_files.items():
            # In-manifest HLS
            vtt_subtitle_stream = self._create_subtitle_stream(
                input_path=os.path.join(source_path, source_sub_file),
                codec_configuration=webvtt_subtitle_configuration,
                language=lang,
            )

            relative_path_vtt = f"subtitles_{lang}/vtt"
            vtt_chunked_text_muxing = self._create_chunked_text_muxing(
                output_path=f"{output_sub_path}/{relative_path_vtt}",
                stream=vtt_subtitle_stream,
                extension="vtt",
            )

            self._add_hls_subtitle_media(
                hls_manifest=hls_manifest,
                stream=vtt_subtitle_stream,
                text_muxing=vtt_chunked_text_muxing,
                relative_path=relative_path_vtt,
                label=self._make_language_label(lang),
                language=lang,
            )

            # In-manifest DASH
            subtitle_adaptation_set = self._add_subtitle_adaptation_set(
                dash_manifest=dash_manifest,
                period=period,
                lang=self._make_language_label(lang),
            )

            self._add_dash_chunked_text_representation(
                dash_manifest=dash_manifest,
                period=period,
                adaptation_set=subtitle_adaptation_set,
                text_muxing=vtt_chunked_text_muxing,
                relative_path=relative_path_vtt,
            )

        if hasattr(self.config, "SPLICE_POINTS"):
            self._create_keyframes(splice_points=self.config.SPLICE_POINTS)

        start_encoding_request = bm.StartEncodingRequest(
            manifest_generator=bm.ManifestGenerator.V2
        )

        start_encoding_request.vod_hls_manifests = [
            bm.ManifestResource(manifest_id=hls_manifest.id)
        ]
        start_encoding_request.vod_dash_manifests = [
            bm.ManifestResource(manifest_id=dash_manifest.id)
        ]

        self._execute_encoding(start_encoding_request=start_encoding_request)

        return (self.encoding, [hls_manifest, dash_manifest])

    def determine_origin_url(self, resource: bm.HlsManifest | bm.DashManifest) -> str:
        baseurl = f"https://{self.output.bucket_name}.s3.amazonaws.com/"

        return "/".join(
            p.strip("/")
            for p in [baseurl, resource.outputs[0].output_path, resource.manifest_name]
        )

    def _poll_encoding_status(self) -> bm.Task:
        sleep(5)
        task = self.encoding_api.encodings.status(encoding_id=self.encoding.id)
        print(
            "Encoding status is {} (progress: {} %)".format(
                task.status.value, task.progress
            )
        )
        return task

    def _execute_encoding(self, start_encoding_request):
        self.encoding_api.encodings.start(
            encoding_id=self.encoding.id, start_encoding_request=start_encoding_request
        )

        task = self._poll_encoding_status()

        while task.status not in [
            bm.Status.FINISHED,
            bm.Status.ERROR,
            bm.Status.CANCELED,
        ]:
            task = self._poll_encoding_status()

        if task.status is bm.Status.ERROR:
            self._log_task_errors(task=task)
            raise Exception("Encoding failed")

        print("Encoding finished successfully")

    def _create_encoding(self, name: str, description: str) -> bm.Encoding:
        encoding = bm.Encoding(name=name, description=description)

        return self.encoding_api.encodings.create(encoding=encoding)

    def _create_keyframes(self, splice_points):
        keyframes = []

        for splice_point in splice_points:
            keyframe = bm.Keyframe(time=splice_point, segment_cut=True)

            keyframes.append(
                self.encoding_api.encodings.keyframes.create(
                    encoding_id=self.encoding.id, keyframe=keyframe
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
        rate: Optional[float] = None,
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
        # encoding complies with the selected profile. The following code makes sure of it,
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

    def _create_aac_audio_configuration(self, bitrate: int) -> bm.AacAudioConfiguration:
        config = bm.AacAudioConfiguration(
            name="AAC {0} kbit/s".format(bitrate / 1000), bitrate=bitrate
        )

        return self.encoding_api.configurations.audio.aac.create(
            aac_audio_configuration=config
        )

    def _create_webvtt_configuration(self) -> bm.WebVttConfiguration:
        config = bm.WebVttConfiguration(
            name="WebVTT",
            # styling=bm.WebVttStyling(mode=bm.WebVttStylingMode.PASSTHROUGH),
            cue_identifier_policy=bm.WebVttCueIdentifierPolicy.OMIT_IDENTIFIERS,
            append_optional_zero_hour=True,
        )

        return self.encoding_api.configurations.subtitles.webvtt.create(
            web_vtt_configuration=config
        )

    def _create_stream(
        self,
        input_path: str,
        codec_configuration: bm.CodecConfiguration,
        language: Optional[str] = None,
    ) -> bm.Stream:
        stream_input = bm.StreamInput(
            input_id=self.input.id,
            input_path=input_path,
            selection_mode=bm.StreamSelectionMode.AUTO,
        )

        stream = bm.Stream(
            input_streams=[stream_input], codec_config_id=codec_configuration.id
        )
        if language:
            stream.metadata = bm.StreamMetadata(language=language)

        return self.encoding_api.encodings.streams.create(
            encoding_id=self.encoding.id, stream=stream
        )

    def _create_subtitle_stream(
        self,
        input_path: str,
        codec_configuration: bm.CodecConfiguration,
        language: Optional[str] = None,
    ) -> bm.Stream:
        input_stream = bm.FileInputStream(input_id=self.input.id, input_path=input_path)
        if input_path.endswith(".srt"):
            input_stream.file_type = bm.FileInputStreamType.SRT
        if input_path.endswith(".vtt"):
            input_stream.file_type = bm.FileInputStreamType.WEBVTT

        input_stream = self.encoding_api.encodings.input_streams.file.create(
            encoding_id=self.encoding.id, file_input_stream=input_stream
        )

        stream_input = bm.StreamInput(input_stream_id=input_stream.id)

        stream = bm.Stream(
            input_streams=[stream_input], codec_config_id=codec_configuration.id
        )

        return self.encoding_api.encodings.streams.create(
            encoding_id=self.encoding.id, stream=stream
        )

    def _create_ts_muxing(
        self,
        output_path: str,
        stream: bm.Stream,
    ) -> bm.TsMuxing:
        muxing = bm.TsMuxing(
            outputs=[self._build_encoding_output(output_path=output_path)],
            segment_length=self.config.SEGMENT_DURATION,
            streams=[bm.MuxingStream(stream_id=stream.id)],
            start_offset=10,
        )

        return self.encoding_api.encodings.muxings.ts.create(
            encoding_id=self.encoding.id, ts_muxing=muxing
        )

    def _create_fmp4_muxing(
        self,
        output_path: str,
        stream: bm.Stream,
    ) -> bm.Fmp4Muxing:
        muxing = bm.Fmp4Muxing(
            outputs=[self._build_encoding_output(output_path=output_path)],
            segment_length=self.config.SEGMENT_DURATION,
            streams=[bm.MuxingStream(stream_id=stream.id)],
        )

        return self.encoding_api.encodings.muxings.fmp4.create(
            encoding_id=self.encoding.id, fmp4_muxing=muxing
        )

    def _create_text_muxing(
        self,
        output_path: str,
        stream: bm.Stream,
        filename: str,
    ) -> bm.TextMuxing:
        muxing = bm.TextMuxing(
            outputs=[self._build_encoding_output(output_path=output_path)],
            streams=[bm.MuxingStream(stream_id=stream.id)],
            filename=filename,
            start_offset=10,
        )

        return self.encoding_api.encodings.muxings.text.create(
            encoding_id=self.encoding.id, text_muxing=muxing
        )

    def _create_chunked_text_muxing(
        self,
        output_path: str,
        stream: bm.Stream,
        extension: str,
    ) -> bm.ChunkedTextMuxing:
        muxing = bm.ChunkedTextMuxing(
            outputs=[self._build_encoding_output(output_path=output_path)],
            segment_length=self.config.SEGMENT_DURATION,
            streams=[bm.MuxingStream(stream_id=stream.id)],
            segment_naming=f"segment_%number%.{extension}",
            start_offset=10,
        )

        return self.encoding_api.encodings.muxings.chunked_text.create(
            encoding_id=self.encoding.id, chunked_text_muxing=muxing
        )

    def _generate_hls_manifest(self, output_path: str) -> bm.HlsManifest:
        hls_manifest = bm.HlsManifest(
            outputs=[self._build_encoding_output(output_path)],
            name="HLS/ts Manifest",
            hls_master_playlist_version=bm.HlsVersion.HLS_V6,
            hls_media_playlist_version=bm.HlsVersion.HLS_V6,
            manifest_name="stream.m3u8",
        )

        return self.hls_api.create(hls_manifest=hls_manifest)

    def _generate_dash_manifest_with_single_period(
        self,
        output_path: str,
    ) -> Tuple[bm.DashManifest, bm.Period]:
        dash_manifest = self.dash_api.create(
            dash_manifest=bm.DashManifest(
                name="Single-Period DASH Manifest",
                manifest_name="stream.mpd",
                outputs=[self._build_encoding_output(output_path)],
                profile=bm.DashProfile.LIVE,
            )
        )

        period = self.dash_api.periods.create(
            manifest_id=dash_manifest.id, period=bm.Period()
        )

        return (dash_manifest, period)

    def _add_video_adaptation_set(
        self, dash_manifest: bm.DashManifest, period: bm.Period
    ):
        return self.dash_api.periods.adaptationsets.video.create(
            manifest_id=dash_manifest.id,
            period_id=period.id,
            video_adaptation_set=bm.VideoAdaptationSet(),
        )

    def _add_audio_adaptation_set(
        self, dash_manifest: bm.DashManifest, period: bm.Period, lang: str
    ):
        return self.dash_api.periods.adaptationsets.audio.create(
            manifest_id=dash_manifest.id,
            period_id=period.id,
            audio_adaptation_set=bm.AudioAdaptationSet(lang=lang),
        )

    def _add_subtitle_adaptation_set(
        self, dash_manifest: bm.DashManifest, period: bm.Period, lang: str
    ):
        return self.dash_api.periods.adaptationsets.subtitle.create(
            manifest_id=dash_manifest.id,
            period_id=period.id,
            subtitle_adaptation_set=bm.SubtitleAdaptationSet(lang=lang),
        )

    def _add_dash_fmp4_representation(
        self,
        dash_manifest: bm.DashManifest,
        period: bm.Period,
        adaptation_set: bm.AdaptationSet,
        fmp4_muxing: bm.Fmp4Muxing,
        relative_path: str,
    ) -> bm.DashFmp4Representation:
        representation = bm.DashFmp4Representation(
            type_=bm.DashRepresentationType.TIMELINE,
            encoding_id=self.encoding.id,
            muxing_id=fmp4_muxing.id,
            segment_path=relative_path,
        )

        return self.dash_api.periods.adaptationsets.representations.fmp4.create(
            manifest_id=dash_manifest.id,
            period_id=period.id,
            adaptationset_id=adaptation_set.id,
            dash_fmp4_representation=representation,
        )

    def _add_dash_chunked_text_representation(
        self,
        dash_manifest: bm.DashManifest,
        period: bm.Period,
        adaptation_set: bm.SubtitleAdaptationSet,
        text_muxing: bm.ChunkedTextMuxing,
        relative_path: str,
    ) -> bm.DashChunkedTextRepresentation:
        representation = bm.DashChunkedTextRepresentation(
            type_=bm.DashRepresentationType.TIMELINE,
            encoding_id=self.encoding.id,
            muxing_id=text_muxing.id,
            segment_path=relative_path,
        )

        return self.dash_api.periods.adaptationsets.representations.chunked_text.create(
            manifest_id=dash_manifest.id,
            period_id=period.id,
            adaptationset_id=adaptation_set.id,
            dash_chunked_text_representation=representation,
        )

    def _add_hls_variant(
        self,
        hls_manifest: bm.HlsManifest,
        stream: bm.Stream,
        ts_muxing: bm.TsMuxing,
        relative_path: str,
        filename_suffix: str,
    ) -> bm.StreamInfo:
        stream_info = bm.StreamInfo(
            audio="AUDIO",
            subtitles="SUBS",
            segment_path=relative_path,
            uri=f"video_{filename_suffix}.m3u8",
            encoding_id=self.encoding.id,
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
        hls_manifest: bm.HlsManifest,
        stream: bm.Stream,
        ts_muxing: bm.TsMuxing,
        relative_path: str,
        filename_suffix: str,
        label: str,
        language: Optional[str] = None,
    ) -> bm.AudioMediaInfo:
        media_info = bm.AudioMediaInfo(
            name=label,
            group_id="AUDIO",
            segment_path=relative_path,
            uri=f"audio_{language}_{filename_suffix}.m3u8",
            encoding_id=self.encoding.id,
            stream_id=stream.id,
            muxing_id=ts_muxing.id,
            language=language,
        )

        return self.hls_api.media.audio.create(
            manifest_id=hls_manifest.id, audio_media_info=media_info
        )

    def _add_hls_subtitle_media(
        self,
        hls_manifest: bm.HlsManifest,
        stream: bm.Stream,
        text_muxing: bm.ChunkedTextMuxing,
        relative_path: str,
        label: str,
        language: Optional[str] = None,
    ) -> bm.SubtitlesMediaInfo:
        media_info = bm.SubtitlesMediaInfo(
            name=label,
            group_id="SUBS",
            segment_path=relative_path,
            uri=f"subtitles_{language}.m3u8",
            encoding_id=self.encoding.id,
            stream_id=stream.id,
            muxing_id=text_muxing.id,
            language=language,
        )

        return self.hls_api.media.subtitles.create(
            manifest_id=hls_manifest.id, subtitles_media_info=media_info
        )

    def _build_encoding_output(self, output_path: str) -> bm.EncodingOutput:
        acl_entry = bm.AclEntry(permission=bm.AclPermission.PUBLIC_READ)

        return bm.EncodingOutput(
            output_path=self._build_absolute_path(relative_path=output_path),
            output_id=self.output.id,
            acl=[acl_entry],
        )

    def _build_absolute_path(self, relative_path: str) -> str:
        return path.join(self.config.S3_OUTPUT_BASE_PATH, relative_path)

    def _log_task_errors(self, task: bm.Task) -> None:
        if task is None:
            return

        filtered = [x for x in task.messages if x.type is bm.MessageType.ERROR]

        for message in filtered:
            print(message.text)

    def _make_language_label(self, lang: str, suffix: str = "") -> str:
        if lang in self.config.LANGUAGE_LABELS:
            label = self.config.LANGUAGE_LABELS[lang]
        else:
            label = lang

        if suffix and suffix != "":
            label += " " + suffix

        return label
