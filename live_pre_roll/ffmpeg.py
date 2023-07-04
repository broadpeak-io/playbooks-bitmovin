import multiprocessing
import subprocess


def generate_dummy_feed(
    rtmp_endpoint: str, stream_key: str, rate: str | float, stream_id: str
) -> multiprocessing.Process:
    out_file_path = f"ffmpeg_output_{stream_id}.txt"
    p = multiprocessing.Process(
        target=_start_ffmpeg_process,
        args=(rtmp_endpoint, stream_key, rate, out_file_path),
    )
    p.start()
    return p


def _start_ffmpeg_process(
    rtmp_endpoint: str, stream_key: str, rate: str | float, out_file_path: str
):
    command = [
        "ffmpeg",
        "-re",
        "-f",
        "lavfi",
        "-i",
        f"testsrc2=size=1920x1080:rate={rate}",
        "-f",
        "lavfi",
        "-i",
        "aevalsrc='0.1*sin(2*PI*(360-2.5/2)*t) | 0.1*cos(2*PI*(440+2.5/2)*t)'",
        "-vf",
        "drawtext=text='time %{localtime\:%X}': fontsize=40: fontcolor=white: box=1: boxborderw=6: boxcolor=black@0.75: x=40: y=main_h-(2*line_h), "
        + "drawtext=text='pts %{pts \: hms}': fontsize=40: fontcolor=white: box=1: boxborderw=6: boxcolor=black@0.75: x=(w-text_w)/2: y=main_h-(2*line_h), "
        + "drawtext=text='frame %{n}': fontsize=40: fontcolor=white: box=1: boxborderw=6: boxcolor=black@0.75: x=w-text_w-40: y=main_h-(2*line_h)",
        "-c:v",
        "libx264",
        "-c:a",
        "aac",
        "-f",
        "flv",
        f"rtmp://{rtmp_endpoint}/live/{stream_key}",
    ]

    with open(out_file_path, "w") as f:
        proc = subprocess.Popen(command, stdout=f, stderr=subprocess.STDOUT)
        proc.communicate()
