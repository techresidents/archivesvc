import logging
import platform
import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from testbase import WORKING_DIRECTORY
from trpycore.pool.simple import SimplePool
from trsvcscore.storage.filesystem import FileSystemStorage

import settings
from stitch import FFMpegSoxStitcher
from stream import ArchiveStream, ArchiveStreamType

if platform.system() == "Darwin":
    FFMPEG_PATH = "/opt/local/bin/ffmpeg"
else:
    FFMPEG_PATH = "/opt/3ps/bin/ffmpeg"

if platform.system() == "Darwin":
    SOX_PATH = "/opt/local/bin/sox"
else:
    SOX_PATH = "/opt/3ps/bin/sox"

class TokboxArchiveFetcherTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.DEBUG)
        cls.engine = create_engine(settings.DATABASE_CONNECTION)
        cls.db_session_factory = sessionmaker(bind=cls.engine)
        cls.storage_pool = SimplePool(FileSystemStorage(WORKING_DIRECTORY))

        cls.archive_streams = []
        for filename, type, length, offset in [
                ("data/0375edd7-3c01-48e1-86b9-304ebefe9629.flv", ArchiveStreamType.USER_VIDEO_STREAM, 66458, 10288),
                ("data/6bd443fe-5807-4c44-a4ae-f9fa7708583d.flv", ArchiveStreamType.USER_VIDEO_STREAM, 74367, 2380)
                ]:
            cls.archive_streams.append(ArchiveStream(
                filename=filename,
                type=type,
                length=length,
                offset=offset))
        
        cls.stitcher = FFMpegSoxStitcher(
                ffmpeg_path=FFMPEG_PATH,
                sox_path=SOX_PATH,
                storage_pool=cls.storage_pool,
                working_directory=WORKING_DIRECTORY)

    @classmethod
    def tearDownClass(cls):
        pass
    
    def test_stitch(self):
        archive_streams = self.stitcher.stitch(self.archive_streams, "output/stitch")
        self.assertIsNotNone(archive_streams[0].filename)
        self.assertIsNotNone(archive_streams[0].length)
        self.assertIsNotNone(archive_streams[0].offset)
        self.assertIsNotNone(archive_streams[1].filename)
        self.assertIsNotNone(archive_streams[1].length)
        self.assertIsNotNone(archive_streams[1].offset)

if __name__ == '__main__':
    unittest.main()
