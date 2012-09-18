import logging
import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from testbase import WORKING_DIRECTORY
from trpycore.pool.simple import SimplePool
from trsvcscore.db.models import ChatArchive, ChatArchiveUser
from trsvcscore.storage.filesystem import FileSystemStorage

import settings
from persist import DefaultPersister
from stream import ArchiveStream, ArchiveStreamType

class ArchivePersisterTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.DEBUG)
        cls.engine = create_engine(settings.DATABASE_CONNECTION)
        cls.db_session_factory = sessionmaker(bind=cls.engine)
        cls.storage_pool = SimplePool(FileSystemStorage(WORKING_DIRECTORY))

        cls.archive_streams = []
        for filename, type, length, offset, users in [
                ("data/0375edd7-3c01-48e1-86b9-304ebefe9629.flv", ArchiveStreamType.USER_VIDEO_STREAM, 66458, 10288, [11]),
                ("data/6bd443fe-5807-4c44-a4ae-f9fa7708583d.flv", ArchiveStreamType.USER_VIDEO_STREAM, 74367, 2380, [12]),
                ("data/stitch-1.mp3", ArchiveStreamType.USER_AUDIO_STREAM, 66458, 10288, [11]),
                ("data/sttich-2.mp3", ArchiveStreamType.USER_AUDIO_STREAM, 74367, 2380, [12]),
                ("data/stitch.mp4", ArchiveStreamType.STITCHED_AUDIO_STREAM, 75154, 2380, [11,12]),
                ]:
            cls.archive_streams.append(ArchiveStream(
                filename=filename,
                type=type,
                length=length,
                users=users,
                offset=offset))
        
        cls.persister = DefaultPersister(
                db_session_factory=cls.db_session_factory,
                storage_pool=cls.storage_pool)
        
    @classmethod
    def tearDownClass(cls):
        try:
            db_session = cls.db_session_factory()
            for stream in cls.archive_streams:
                try:
                    archive = db_session.query(ChatArchive)\
                            .filter_by(path=stream.filename)\
                            .one()
                    archive_users = db_session.query(ChatArchiveUser)\
                            .filter_by(chat_archive_id=archive.id)
                    for archive_user in archive_users:
                        db_session.delete(archive_user)

                    db_session.delete(archive)
                except Exception:
                    pass
            db_session.commit()
        finally:
            if db_session:
                db_session.close()
                 
    
    def test_persist(self):
        self.persister.persist(chat_session_id=1, archive_streams=self.archive_streams)
        try:
            for stream in self.archive_streams:
                db_session = self.db_session_factory()

                archive = db_session.query(ChatArchive)\
                        .filter_by(path=stream.filename)\
                        .one()

                self.assertEqual(stream.filename, archive.path)
                self.assertEqual(stream.length, archive.length)
                self.assertEqual(stream.offset, archive.offset)
        finally:
            if db_session:
                db_session.commit()
                db_session.close()

if __name__ == '__main__':
    unittest.main()
