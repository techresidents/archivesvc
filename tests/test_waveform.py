import logging
import platform
import unittest

from testbase import WORKING_DIRECTORY
from trpycore.pool.simple import SimplePool
from trsvcscore.storage.filesystem import FileSystemStorage

from waveform import FFMpegWaveformGenerator
from stream import ArchiveStream, ArchiveStreamType

if platform.system() == "Darwin":
    FFMPEG_PATH = "/opt/local/bin/ffmpeg"
else:
    FFMPEG_PATH = "/opt/3ps/bin/ffmpeg"

class ArchivePersisterTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.DEBUG)
        cls.storage_pool = SimplePool(FileSystemStorage(WORKING_DIRECTORY))

        cls.archive_stream = ArchiveStream("data/stitch.mp4", ArchiveStreamType.STITCHED_AUDIO_STREAM, 75154, 2380, [11,12])
        
        cls.waveform_generator = FFMpegWaveformGenerator(
                ffmpeg_path=FFMPEG_PATH,
                storage_pool=cls.storage_pool,
                working_directory=WORKING_DIRECTORY)
        
    @classmethod
    def tearDownClass(cls):
        pass
                 
    def test_generate(self):
        stream = self.waveform_generator.generate(self.archive_stream, "stitch")
        self.assertIsNotNone(stream.waveform)
        self.assertIsNotNone(stream.waveform_filename)

if __name__ == '__main__':
    unittest.main()
