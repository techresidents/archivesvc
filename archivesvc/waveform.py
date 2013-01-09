import abc
import json
import logging
import os
import subprocess

import numpy as np
from PIL import Image, ImageDraw
from scikits.audiolab import Sndfile

from trpycore.pool.simple import SimplePool
from trsvcscore.storage.exception import NotImplemented
from trsvcscore.storage.filesystem import FileSystemStorage
from stream import ArchiveStream, ArchiveStreamType

class Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return [float("%.4f" % n) for n in obj]
        return json.JSONEncoder.default(self, obj)

class ArchiveWaveformGeneratorException(Exception):
    """Archive waveform generator exception."""
    pass


class ArchiveWaveformGenerator(object):
    """Archive waveform generator abstract base class.

    Archive waveform generator is responsible for generating waveform
    data and its corresponding image given an archive stream.
    """
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractproperty
    def generate(self, archive_stream, output_filename):
        """Generate waveform data and corresponding images for streams.

        Args:
            archive_streams: ArchiveStream object to
                generate waveform data and image for.
            output_filename: output base filename to be used
                to construct the waveform image's filename.
        Returns:
            modified ArchiveStream object.
        Raises:
            ArchiveStitcherException
        """
        return

class FFMpegWaveformGenerator(ArchiveWaveformGenerator):
    """ffmpeg based archive waveform generator.

    Archive waveform generator is responsible for generating waveform
    data and its corresponding image given an archive stream.
    The waveform image is stored in the specified storage_pool.

    Note that this generator requires archive streams to be
    available on the local filesystem for waveform generation.
    If the storage_pool provided is not accessible on
    the local filesystem, all streams will be downloaded
    prior to waveform generation.
    """

    def __init__(self,
            ffmpeg_path,
            storage_pool,
            working_directory):
        """FFMpegWaveformGenerator constructor.

        Args:
            ffmpeg_path: absolute path to ffmpeg executable
            storage_pool: Pool of Storage objects used to
                access the ArchiveStream objects passed to
                generate(), and to store the resulting
                waveform images.
            working_directory: working directory path. This
                path will be used to store downloaded
                archive streams if the specified storage_pool
                is not accessible on the local filesystem.
        """

        self.ffmpeg_path = ffmpeg_path
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

        Extracts audio stream from video stream using ffmpeg, and
        stores the resulting stream using the specified storage_backend.
        
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
            self.log.info("Extracting .wav audio from %s" % archive_stream)

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

    def _extract_waveform_data(self, storage_backend, archive_stream, size=1800):
        """Extract waveform data from .wav archive stream.

        Extracts waveform data from stream as normalized numpy array
        suitable for rendering. Values in the array indicate the 
        maximum magnitude of the waveform in a time window and will
        be between 0 and 1.
        
        Args:
            storage_backend: Storage object, accessible on local filesystem,
                where archive_stream can be found.
            archive_stream: ArchiveStream object containing a .wav stream for
                which to extract the waveform data.
            size: size of the array to return
        Returns:
            numpy array of normalized max amplitude waveform data
        Raises:
            StorageException
        """
        self.log.info("Extracting waveform data from %s" % archive_stream)

        sound_file = Sndfile(storage_backend.path(archive_stream.filename), 'r')
        frames = sound_file.read_frames(sound_file.nframes, dtype=np.float64)
        if sound_file.channels == 2:
            frames = frames[::2]

        frames_per_pixel = len(frames)/size

        data = []
        for x in range(0, size):
            f = frames[x*frames_per_pixel: (x+1) * frames_per_pixel]
            value = np.abs(f).max()
            data.append(value)
        
        sound_file.close()
    
        return np.array(data)

    def _render_waveform_data(self, storage_backend, waveform_data, output_filename, height=280):
        """Render waveform_data to output_filename.
        
        Renders waveform_data as transparent image.

        Args:
            storage_backend: Storage object, accessible on local filesystem,
                where waveform image file can be generated.
            waveform_data: numpy array with normalized waveform data
            output_filename: output filename to use when storing waveform image 
                on the storage_backend.
            height: height in pixels of image
        Raises:
            StorageException
        """

        output_path = storage_backend.path(output_filename)
        width = len(waveform_data)
        image = Image.new("RGBA", (width, height), (238,238,238,255))
        draw = ImageDraw.Draw(image)
        
        scale = 1 - max(waveform_data)

        for x,value in enumerate(waveform_data):
            value += scale
            value *= height/2
            draw.line([x, height/2 + value, x, height/2 - value], (0,0,0,0)) 

        image.save(output_path)
        
    
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

    def generate(self, archive_stream, output_filename):
        """Generate waveform data and image for stream.

        Note that waveform generation requires archive streams
        to be available on the local filesystem for stitching.
        If the storage_pool provided is not accessible on
        the local filesystem, all streams will be downloaded
        prior to stitching.

        Args:
            archive_stream: ArchiveStream object to
                to generate waveform data and image for.
            output_filename: output base filename to be used
                to construct the stiched stream's filename.
        Returns:
            modified ArchiveStream object.
        Raises:
            ArchiveWaveformGeneratorException
        """
        try:
            #check to see if the archive_stream stored on self.storage_pool
            #are accessible on the local filesystem. Waveform generation requres
            #the archive streams to be accessible on the local filesystem, so if
            #they're not, we need to download the streams before they can
            #be stitched.
            with self.storage_pool.get() as storage_backend:
                try:
                    storage_backend.path(archive_stream.filename)
                    storage_pool = self.storage_pool
                except NotImplemented:
                    self._download_archive_streams([archive_stream])
                    storage_pool = self.filsystem_storage_pool
            
            with storage_pool.get() as storage_backend:
                #extact .wav audio from stream
                audio_stream = self._extract_audio_stream(
                        storage_backend=storage_backend,
                        archive_stream=archive_stream,
                        output_filename="%s.wav" % (output_filename))

                waveform_data = self._extract_waveform_data(
                        storage_backend=storage_backend,
                        archive_stream=audio_stream)
                
                waveform_filename = "%s.png" % output_filename
                self._render_waveform_data(
                        storage_backend=storage_backend,
                        waveform_data=waveform_data,
                        output_filename=waveform_filename)

                archive_stream.waveform = json.dumps(waveform_data, cls=Encoder)
                archive_stream.waveform_filename = waveform_filename

            #if the storage_pool is not accessible on local filesystem
            #upload the stitched stream.
            if storage_pool is not self.storage_pool:
                self._upload_archive_streams([audio_stream])

            return archive_stream
        
        except Exception as error:
            self.log.exception(error)
            raise ArchiveWaveformGeneratorException(str(error))

        return archive_stream
