import abc
import logging
import os
import re
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
            list of stitched ArchiveStream objects.
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
    
    def _get_audio_stream_stats(self, storage_backend, archive_stream):
        """Get dict of audio stream stats.
        
        Returns dict of audio stream stats based on the output
        from 'sox <file> -n stat'. Stats include the following:

            Samples read
            Length (seconds)
            Scaled by
            Maximum amplitude
            Minimum amplitude
            Midline amplitude
            Mean norm
            Mean amplitude
            RMS amplitude
            Maximum delta
            Minimum delta
            Mean delta
            RMS delta
            Rough frequency
            Volume adjustment

        Args:
            storage_backend: Storage object, accessible on local filesystem,
                where archive_stream can be found.
            archive_stream: ArchiveStream object for which to determine length.
        Returns:
            dict of audio stream stats
        Raises:
            subprocess.CalledProcessError, StorageException
        """
        result = {}

        sox_arguments = [
                self.sox_path,
                storage_backend.path(archive_stream.filename),
                "-n",
                "stat"]
        
        output = subprocess.check_output(sox_arguments, stderr=subprocess.STDOUT)
        for line in output.split("\n"):
            line = line.strip()
            key_value = line.split(":", 1)
            if len(key_value) == 2:
                #remove duplicate spaces from stat key
                key = re.sub(r"\s+", " ", key_value[0].strip())
                value = float(key_value[1].strip())
                result[key] = value

        return result

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
        stats = self._get_audio_stream_stats(storage_backend, archive_stream)
        return stats["Length (seconds)"] * 1000.0

    def _adjust_audio_stream_volume(self,
            storage_backend,
            archive_stream,
            volume_factor,
            output_filename):
        """Adjust audio stream volume by volume factor.

        This method will adjust archive_stream's volume by volume_factor
        through sox's vol <volume_factor>. The updated audio stream will
        be stored on storage_backend as output_filename.

        Args:
            storage_backend: Storage object, accessible on local filesystem,
                where archive_stream can be found.
            archive_stream: ArchiveStream object for which to determine length.
            volume_factor: factor adjust volume by
            output_filename: output filename to use when  storing audio stream
                on the storage_backend.
        Returns:
            ArchiveStream object with adjusted volume
        Raises:
            subprocess.CalledProcessError, StorageException
        """
        output_path = storage_backend.path(output_filename)
        self._ensure_directory(output_path)

        if not os.path.exists(output_path):
            self.log.info("Adjusting audio volume for %s" % archive_stream)

            sox_arguments = [
                    self.sox_path,
                    storage_backend.path(archive_stream.filename),
                    output_path,
                    "vol",
                    "%s" % volume_factor]
            
            self.log.info(sox_arguments)
            output = subprocess.check_output(sox_arguments, stderr=subprocess.STDOUT)
            self.log.info(output)

        return ArchiveStream(
                filename=output_filename,
                type=archive_stream.type,
                length=archive_stream.length,
                users=archive_stream.users,
                offset=archive_stream.offset)
    
    def _normalize_audio_streams(self, storage_backend, archive_streams):
        """Normalize audio streams.

        This method with attempt to normalize the specified audio streams
        with the following approach:
            1) Find the audio stream with the lowest RMS amplitude
               (RMS amplitude is the best gauge of volume), and
               raise it's volume to 70% of the maximum amount
               the stream can be increased to before clipping
               occurs. This will increase the volume of the
               stream and still leave a bit of headroom.
            2) Determine the new RMS amplitude of the stream
               we adjusted and use this as the target volume
               moving forward
            3) Adjust all of the other streams to the target
               volume.
        
        The adjusted audio stream will be stored on the specified
        storage_backend with filenames calculated from the
        input stream. The output filename will be identical
        to the input stream with addition of "-norm" immediately
        before the filename's extension.

        Args:
            storage_backend: Storage object, accessible on local filesystem,
                where archive_stream can be found.
            archive_stream: list of ArchiveStream objects to normalize
        Returns:
            list of normalized ArchiveStream objects
        Raises:
            subprocess.CalledProcessError, StorageException
        """
        results = []

        def build_output_filename(stream):
            """Get output_filename for given stream."""
            path, ext = os.path.splitext(stream.filename)
            return "%s-norm%s" % (path, ext)
        
        #determine stream with the lowest volume (RMS Amplitude)
        stream_stats = []
        lowest_volume_index = None
        for index, stream in enumerate(archive_streams):
            stats = self._get_audio_stream_stats(storage_backend, stream)
            if lowest_volume_index is None:
                lowest_volume_index = index
            else:
                lowest_stats = stream_stats[lowest_volume_index]
                if stats["RMS amplitude"] < lowest_stats["RMS amplitude"]:
                    lowest_volume_index = index
            stream_stats.append(stats)

        lowest_volume_stream = archive_streams[lowest_volume_index]
        lowest_volume_stats = stream_stats[lowest_volume_index]

        #adjust the lowest volume stream by 70% of the maximum
        #possible amount without clipping. The max volume factor
        #the stream can be increased by without clippling is
        #available in stats as "Volume adjustment." We only
        #use 70% of this number to leave a little headroom.
        adjusted_stream = self._adjust_audio_stream_volume(
                storage_backend=storage_backend,
                archive_stream=lowest_volume_stream, 
                volume_factor=lowest_volume_stats["Volume adjustment"] * 0.7,
                output_filename=build_output_filename(lowest_volume_stream))
        results.append(adjusted_stream)
        
        #get the new volume of the adjusted lowest stream, and
        #use this as the target volume to which the rest of the
        #streams should be adjusted.
        adjusted_stream_stats = self._get_audio_stream_stats(
                storage_backend=storage_backend,
                archive_stream=adjusted_stream)
        target_volume = adjusted_stream_stats["RMS amplitude"]
        
        #adjust the remaining streams volume to the target volume.
        for index, stream in enumerate(archive_streams):

            #skip the stream with the lowest volume, since
            #we've already adjusted it.
            if index == lowest_volume_index:
                continue
            stats = stream_stats[index]
            volume_factor = target_volume / stats["RMS amplitude"]
            adjusted_stream = self._adjust_audio_stream_volume(
                    storage_backend=storage_backend,
                    archive_stream=stream,
                    volume_factor=volume_factor,
                    output_filename=build_output_filename(stream))
            results.append(adjusted_stream)
        
        return results


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
                sox_arguments.append(output_path)
            else:
                input_filename = storage_backend.path(archive_streams[0].filename)
                sox_arguments = [
                        self.sox_path,
                        "--norm",
                        input_filename,
                        output_path,
                        "pad",
                        "%s" % ((stream.offset or 0)/1000.0)
                        ]
                
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
            list of stitched ArchiveStream objects.
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
                
                #normalize audio streams volume
                normalized_streams = self._normalize_audio_streams(
                        storage_backend=storage_backend,
                        archive_streams=audio_streams)

                #stitch audio streams together
                stitched_stream = self._stitch_audio_streams(
                        storage_backend=storage_backend,
                        archive_streams=normalized_streams,
                        output_filename="%s.mp3" % output_filename)

                #convert stitched stream to mp4
                mp4_stream = self._to_mp4_stream(
                        storage_backend=storage_backend,
                        archive_stream=stitched_stream,
                        output_filename="%s.mp4" % output_filename)
            
            #if the storage_pool is not accessible on local filesystem
            #upload the stitched stream.
            if storage_pool is not self.storage_pool:
                self._upload_archive_streams([stitched_stream, mp4_stream])
        
        except Exception as error:
            self.log.exception(error)
            raise ArchiveStitcherException(str(error))

        return [mp4_stream, stitched_stream]
