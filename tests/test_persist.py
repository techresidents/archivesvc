import logging
import os
import unittest
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

from testbase import WORKING_DIRECTORY
from trpycore.pool.simple import SimplePool
from trpycore.thrift.serialization import deserialize
from trpycore.timezone import tz
from trsvcscore.db.models import ChatArchive, ChatArchiveUser, ChatMessage, ChatSession
from trsvcscore.storage.filesystem import FileSystemStorage
from trchatsvc.gen.ttypes import MessageType, Message

import settings
from persist import DefaultPersister
from stream import ArchiveStream, ArchiveStreamType

MESSAGE_TYPE_IDS= {
    "MARKER_CREATE": 1,
    "MINUTE_CREATE": 2,
    "MINUTE_UPDATE": 3,
    "TAG_CREATE": 4,
    "TAG_DELETE": 5,
    "WHITEBOARD_CREATE": 6,
    "WHITEBOARD_DELETE": 7,
    "WHITEBOARD_CREATE_PATH": 8,
    "WHITEBOARD_DELETE_PATH": 9,
}

MESSAGE_FORMAT_TYPE_IDS = {
    "JSON": 1,
    "THRIFT_BINARY_B64": 2,
}

class ArchivePersisterTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.DEBUG)
        cls.engine = create_engine(settings.DATABASE_CONNECTION)
        cls.db_session_factory = sessionmaker(bind=cls.engine)
        cls.local_storage_pool = SimplePool(FileSystemStorage(WORKING_DIRECTORY))

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
                local_storage_pool=cls.local_storage_pool)
        
    @classmethod
    def tearDownClass(cls):
        pass
                 
    @contextmanager   
    def _chat_session(self, chat_id=1):
        try:
            db_session = self.db_session_factory()

            chat_session = ChatSession(
                    chat_id=chat_id,
                    token="2_MX4xNTg4OTk5MX4xMjcuMC4wLjF-MjAxMi0wOS0xNCAxMzo1MTozOS41MTAyNzErMDA6MDB-MC4yNjU5NjM2MTgzNTZ-",
                    participants=2,
                    start=func.current_timestamp(),
                    end=func.current_timestamp())
            
            db_session.add(chat_session)

            chat_messages_filename = os.path.join(
                    WORKING_DIRECTORY,
                    "data",
                    "chat_messages.thrift")
            
            chat_messages = []
            with open(chat_messages_filename) as f:
                for message in f.readlines():
                    thrift_message = deserialize(Message(), message)
                    chat_message = ChatMessage(
                            message_id=thrift_message.header.id,
                            chat_session=chat_session,
                            type_id=MESSAGE_TYPE_IDS[MessageType._VALUES_TO_NAMES[thrift_message.header.type]],
                            format_type_id=MESSAGE_FORMAT_TYPE_IDS["THRIFT_BINARY_B64"],
                            timestamp=thrift_message.header.timestamp,
                            time=tz.timestamp_to_utc(thrift_message.header.timestamp),
                            data=message)
                    chat_messages.append(chat_message)
                    db_session.add(chat_message)
            
            db_session.commit()
            yield chat_session
            
            for chat_message in chat_messages:
                db_session.delete(chat_message)

            for stream in self.archive_streams:
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

            db_session.delete(chat_session)
            db_session.commit()
        finally:
            if db_session:
                db_session.close()
                    

    def test_persist(self):
        with self._chat_session() as chat_session:
            self.persister.persist(chat_session_id=chat_session.id, archive_streams=self.archive_streams)

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
