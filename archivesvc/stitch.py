import abc
import os
import subprocess

from trpycore.pool.simple import SimplePool
from trsvcscore.storage.exception import NotImplemented
from trsvcscore.storage.filesystem import FileSystemStorage
from stream import ArchiveStream, ArchiveStreamType

class ArchiveStitcher(object):
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractproperty
    def stitch(self, archive_streams, output_filename):
        return

class FFMpegSoxStitcher(ArchiveStitcher):

    def __init__(self,
            ffmpeg_path,
            sox_path,
            storage_pool,
            working_directory):
        self.ffmpeg_path = ffmpeg_path
        self.sox_path = sox_path
        self.storage_pool = storage_pool
        self.working_directory = working_directory
        self.filesystem_storage_pool = SimplePool(
                FileSystemStorage(self.working_directory))
    
    def _ensure_directory(self, path):
        directory, filename = os.path.split(path)
        if not os.path.exists(directory):
            os.makedirs(directory)

    def _extract_audio_stream(self, storage_backend, archive_stream, output_filename):
        output_path = storage_backend.path(output_filename)
        self._ensure_directory(output_path)

        ffmpeg_arguments = [
                self.ffmpeg_path,
                "-y",
                "-i",
                storage_backend.path(archive_stream.filename),
                "-vn",
                "-ar",
                "44100",
                storage_backend.path(output_filename)
                ]

        subprocess.call(ffmpeg_arguments)

        return ArchiveStream(
                filename=output_filename,
                type=ArchiveStreamType.USER_AUDIO_STREAM,
                length=archive_stream.length,
                users=archive_stream.users,
                offset=archive_stream.offset)
    
    def _get_audio_stream_length(self, storage_backend, archive_stream):
        result = None

        sox_arguments = [
                self.sox_path,
                storage_backend.path(archive_stream.filename),
                "-n",
                "stat"]
        
        output = subprocess.check_output(sox_arguments, stderr=subprocess.STDOUT)
        for line in output.split("\n"):
            line = line.strip()
            if line.startswith("Length"):
                length = float(line.split(":")[1])
                result = int(length * 1000)
        
        return result

    def _stitch_audio_streams(self, storage_backend, archive_streams, output_filename):
        output_path = storage_backend.path(output_filename)
        self._ensure_directory(output_path)

        sox_arguments = [self.sox_path, "-m", "--norm"]
        
        users = []
        for stream in archive_streams:
            users.extend(stream.users)
            sox_arguments.append("|sox %s -p pad %s" % (\
                    storage_backend.path(stream.filename),
                    (stream.offset or 0)/1000.0))
        sox_arguments.append(output_filename)

        subprocess.call(sox_arguments)
        
        result = ArchiveStream(
                filename=output_filename,
                type=ArchiveStreamType.STITCHED_AUDIO_STREAM,
                length=None,
                users=users,
                offset=min([s.offset for s in archive_streams]))

        length = self._get_audio_stream_length(storage_backend, result)
        result.length = length

        return result
    
    def _to_mp4_stream(self, storage_backend, archive_stream, output_filename):
        output_path = storage_backend.path(output_filename)
        self._ensure_directory(output_path)

        ffmpeg_arguments = [
                self.ffmpeg_path,
                "-y",
                "-i",
                storage_backend.path(archive_stream.filename),
                storage_backend.path(output_filename)
                ]

        subprocess.call(ffmpeg_arguments)

        return ArchiveStream(
                filename=output_filename,
                type=archive_stream.type,
                length=archive_stream.length,
                users=archive_stream.users,
                offset=archive_stream.offset)
    
    def _download_archive_streams(self, archive_streams):
        with self.storage_pool.get() as remote_storage:
            with self.filesystem_storage_pool.get() as local_storage:
                for stream in archive_streams:
                    with remote_storage.open(stream.filename, "r") as stream_file:
                        local_storage.save(stream.filename, stream_file)

    def _upload_archive_streams(self, archive_streams):
        with self.storage_pool.get() as remote_storage:
            with self.filesystem_storage_pool.get() as local_storage:
                for stream in archive_streams:
                    with local_storage.open(stream.filename, "r") as stream_file:
                        remote_storage.save(stream.filename, stream_file)

    def stitch(self, archive_streams, output_filename):
        video_streams = archive_streams
        
        with self.storage_pool.get() as storage_backend:
            for stream in archive_streams[:1]:
                try:
                    storage_backend.path(stream.filename)
                    storage_pool = self.storage_pool
                except NotImplemented:
                    self._download_archive_streams(archive_streams)
                    storage_pool = self.filsystem_storage_pool
        
        with storage_pool.get() as storage_backend:
            #extact audio from video streams
            audio_streams = []
            for index, stream in enumerate(video_streams):
                audio_stream = self._extract_audio_stream(
                        storage_backend=storage_backend,
                        archive_stream=stream,
                        output_filename="%s-%s.mp3" % (output_filename, index+1))
                audio_streams.append(audio_stream)
            
            #stitch audio streams together
            stitched_stream = self._stitch_audio_streams(
                    storage_backend=storage_backend,
                    archive_streams=audio_streams,
                    output_filename="%s.mp3" % output_filename)

            #convert stitched stream to mp4
            mp4_stream = self._to_mp4_stream(
                    storage_backend=storage_backend,
                    archive_stream=stitched_stream,
                    output_filename="%s.mp4" % output_filename)
        
        if storage_pool is not self.storage_pool:
            self._upload_archive_streams([mp4_stream])

        return mp4_stream
