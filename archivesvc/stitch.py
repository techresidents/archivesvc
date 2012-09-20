import abc
import logging
import os
import subprocess

from trpycore.pool.simple import SimplePool
from trsvcscore.storage.exception import NotImplemented
from trsvcscore.storage.filesystem import FileSystemStorage
from stream import ArchiveStream, ArchiveStreamType

class ArchiveStitcherException(Exception):
    """Archive stitcher exception."""
    pass


class ArchiveStitcher(object):
    """Archive stitcher abstract base class.

    Archive stitcher is responsible for anonymizing and stiching
    together individual video streams into a single audio stream.
    """
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractproperty
    def stitch(self, archive_streams, output_filename):
        """Stitch video streams into single audio stream.

        Args:
            archive_streams: list of ArchiveStream objects to
                stitch into single audio stream.
            output_filename: output base filename to be used
                to construct the stiched stream's filename.
        Returns:
            stitched ArchiveStream object.
        Raises:
            ArchiveStitcherException
        """
        return


class FFMpegSoxStitcher(ArchiveStitcher):
    """ffmpeg/sox based archive stitcher.

    Archive stitcher is responsible for anonymizing and stiching
    together individual video streams into a single audio stream.
    The stitched stream is stored in the specified storage_pool.

    Note that this stitcher requires archive streams to be
    available on the local filesystem for stitching.
    If the storage_pool provided is not accessible on
    the local filesystem, all streams will be downloaded
    prior to stitching.
    """

    def __init__(self,
            ffmpeg_path,
            sox_path,
            storage_pool,
            working_directory):
        """FFMpegSoxStitcher constructor.

        Args:
            ffmpeg_path: absolute path to ffmpeg executable
            sox_path: absolute path to sox executable
            storage_pool: Pool of Storage objects used to
                access the ArchiveStream objects passed to
                fetch(), and to store the resulting
                stitched ArchiveStream object.
            working_directory: working directory path. This
                path will be used to store downloaded
                archive streams if the specified storage_pool
                is not accessible on the local filesystem.
        """

        self.ffmpeg_path = ffmpeg_path
        self.sox_path = sox_path
        self.storage_pool = storage_pool
        self.working_directory = working_directory
        self.filesystem_storage_pool = SimplePool(
                FileSystemStorage(self.working_directory))

        self.log = logging.getLogger("%s.%s" \
                % (__name__, self.__class__.__name__))
    
    def _ensure_directory(self, path):
        """Ensure directory at path exists."""
        directory, filename = os.path.split(path)
        if not os.path.exists(directory):
            os.makedirs(directory)

    def _extract_audio_stream(self, storage_backend, archive_stream, output_filename):
        """Extract audio stream from specified archive stream.

        Extracts audio stream from video stream using ffmpeg, and stores the resulting
        stream using the specified storage_backend.
        
        Args:
            storage_backend: Storage object, accessible on local filesystem,
                where archive_stream can be found.
            archive_stream: ArchiveStream object from which to extract audio.
            output_filename: output filename to use when  storing audio stream
                on the storage_backend.
        Returns:
            ArchiveStream object containing the audio stream.
        Raises:
            subprocess.CalledProcessError, StorageException
        """
        output_path = storage_backend.path(output_filename)
        self._ensure_directory(output_path)
        
        if not os.path.exists(output_path):
            self.log.info("Extracting audio from %s" % archive_stream)

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
            
            self.log.info(ffmpeg_arguments)

            output = subprocess.check_output(
                    ffmpeg_arguments,
                    stderr=subprocess.STDOUT)

            self.log.info(output)

        return ArchiveStream(
                filename=output_filename,
                type=ArchiveStreamType.USER_AUDIO_STREAM,
                length=archive_stream.length,
                users=archive_stream.users,
                offset=archive_stream.offset)
    
    def _get_audio_stream_length(self, storage_backend, archive_stream):
        """Get audio stream length in milliseconds.
        
        Args:
            storage_backend: Storage object, accessible on local filesystem,
                where archive_stream can be found.
            archive_stream: ArchiveStream object for which to determine length.
        Returns:
            audio stream length in milliseconds.
        Raises:
            subprocess.CalledProcessError, StorageException
        """
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
        """Stitch multiple audio streams into a single audio stream.
        
        Stitches multiple audio stream into a single audio stream using sox,
        and stores the resulting stream using the specified storage_backend.
        
        Args:
            storage_backend: Storage object, accessible on local filesystem,
                where archive_stream can be found.
            archive_streams: ArchiveStream objects to stitch
            output_filename: output filename to use when storing audio stream
                on the storage_backend.
        Returns:
            ArchiveStream object containing stitched audio streams.
        Raises:
            subprocess.CalledProcessError, StorageException
        """
        output_path = storage_backend.path(output_filename)
        self._ensure_directory(output_path)

        users = []
        for stream in archive_streams:
            users.extend(stream.users)
        
        if not os.path.exists(output_path):
            self.log.info("Stitching audio from %s" % archive_streams)

            if len(archive_streams) > 1:
                sox_arguments = [self.sox_path, "-m", "--norm"]

                for stream in archive_streams:
                    sox_arguments.append("|sox %s -p pad %s" % (\
                            storage_backend.path(stream.filename),
                            (stream.offset or 0)/1000.0))
                sox_arguments.append(output_filename)
            else:
                input_filename = archive_streams[0].filename
                sox_arguments = [
                        self.sox_path,
                        "--norm",
                        input_filename,
                        output_filename]
                
            self.log.info(sox_arguments)

            output = subprocess.check_output(
                    sox_arguments,
                    stderr=subprocess.STDOUT)

            self.log.info(output)
        
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
        """Convert stream mp4 archive stream.

        Converts archive stream to a mp4 archive stream.

        Args:
            storage_backend: Storage object, accessible on local filesystem,
                where archive_stream can be found.
            archive_stream: ArchiveStream object to convert.
            output_filename: output filename to use when  storing mp4 stream
                on the storage_backend.
        Returns:
            ArchiveStream object containing the mp4 stream.
        Raises:
            subprocess.CalledProcessError, StorageException
        """
        output_path = storage_backend.path(output_filename)
        self._ensure_directory(output_path)
        
        if not os.path.exists(output_path):
            self.log.info("Converting to mp4 for %s" % archive_stream)

            ffmpeg_arguments = [
                    self.ffmpeg_path,
                    "-y",
                    "-i",
                    storage_backend.path(archive_stream.filename),
                    storage_backend.path(output_filename)
                    ]

            output = subprocess.check_output(
                    ffmpeg_arguments,
                    stderr=subprocess.STDOUT)
            
            self.log.info(output)

        return ArchiveStream(
                filename=output_filename,
                type=archive_stream.type,
                length=archive_stream.length,
                users=archive_stream.users,
                offset=archive_stream.offset)
    
    def _download_archive_streams(self, archive_streams):
        """Download archive streams to local filesystem.
        
        Downloads all archive_streams from self.storage_pool to
        the local filesystem using self.filesystem_storage.
        Downloaded archive streams will be accessible on
        self.filesystem_storage using the same filenames.

        Raises:
            subprocess.CalledProcessError, StorageException
        """
        with self.storage_pool.get() as remote_storage:
            with self.filesystem_storage_pool.get() as local_storage:
                for stream in archive_streams:
                    with remote_storage.open(stream.filename, "r") as stream_file:
                        local_storage.save(stream.filename, stream_file)

    def _upload_archive_streams(self, archive_streams):
        """Upload archive streams to storage_pool.
        
        Uploads all archive_streams from self.filesystem_storage to
        self.storage_pool.  Uploaded archive streams will be accessible 
        on self.storage_pool using the same filenames.

        Raises:
            subprocess.CalledProcessError, StorageException
        """
        with self.storage_pool.get() as remote_storage:
            with self.filesystem_storage_pool.get() as local_storage:
                for stream in archive_streams:
                    with local_storage.open(stream.filename, "r") as stream_file:
                        remote_storage.save(stream.filename, stream_file)

    def stitch(self, archive_streams, output_filename):
        """Stitch video streams into single audio stream.

        Note that stitching requires archive streams to be
        available on the local filesystem for stitching.
        If the storage_pool provided is not accessible on
        the local filesystem, all streams will be downloaded
        prior to stitching.

        Args:
            archive_streams: list of ArchiveStream objects to
                stitch into single audio stream.
            output_filename: output base filename to be used
                to construct the stiched stream's filename.
        Returns:
            stitched ArchiveStream object.
        Raises:
            ArchiveStitcherException
        """
        try:
            video_streams = archive_streams
            
            #check to see if the archive_streams stored on self.storage_pool
            #are accessible on the local filesystem. Stitching requres the
            #archive streams to be accessible on the local filesystem, so if
            #they're not, we need to download the streams before they can
            #be stitched.
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
                            output_filename="%s-%s.mp3" \
                                    % (output_filename, index+1))
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
            
            #if the storage_pool is not accessible on local filesystem
            #upload the stitched stream.
            if storage_pool is not self.storage_pool:
                self._upload_archive_streams([mp4_stream])
        
        except Exception as error:
            self.log.exception(error)
            raise ArchiveStitcherException(str(error))

        return mp4_stream
