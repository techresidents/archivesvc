import abc
import os

from trsvcscore.db.models import MimeType
from trsvcscore.db.models import ChatArchive, ChatArchiveType, ChatArchiveUser

class ArchivePersistException(Exception):
    pass

class ArchivePersister(object):
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractmethod
    def persist(self, chat_session_id,  archive_streams):
        return


class DefaultPersister(ArchivePersister):

    def __init__(self,
            db_session_factory,
            storage_pool,
            remote_storage_pool=None):
        self.db_session_factory = db_session_factory
        self.local_storage_pool = storage_pool
        self.remote_storage_pool = remote_storage_pool

    def _upload_archive_streams(self, archive_streams):
        if self.remote_storage_pool is None:
            return

        with self.remote_storage_pool.get() as remote_storage:
            with self.local_storage_pool.get() as local_storage:
                for stream in archive_streams:
                    if remote_storage.exists(stream.filename):
                        continue
                    with local_storage.open(stream.filename, "r") as file:
                        remote_storage.save(stream.filename, file)
    
    def _get_chat_archive_type_id(self, archive_stream):
        try:
            db_session = self.db_session_factory()
            type = db_session.query(ChatArchiveType)\
                    .filter_by(name=archive_stream.type)\
                    .one()
            return type.id
        finally:
            if db_session:
                db_session.commit()
                db_session.close()

    def _get_mime_type_id(self, archive_stream):
        try:
            db_session = self.db_session_factory()
            root, file_extension = os.path.splitext(archive_stream.filename)
            mime_type = db_session.query(MimeType)\
                    .filter_by(extension=file_extension)\
                    .first()
            return mime_type.id
        finally:
            if db_session:
                db_session.commit()
                db_session.close()

    def persist(self, chat_session_id, archive_streams):
        try:
            db_session = self.db_session_factory()

            self._upload_archive_streams(archive_streams)

            for stream in archive_streams:
                chat_archive_type_id = self._get_chat_archive_type_id(stream)
                mime_type_id = self._get_mime_type_id(stream)
                
                #this is necessary for now since django file fields do not
                #support unique constraints.
                if db_session.query(ChatArchive)\
                        .filter_by(path=stream.filename)\
                        .count() != 0:
                            msg = "archive already exists for '%s'"\
                                    % stream.filename
                            raise RuntimeError(msg)

                archive = ChatArchive(
                        chat_session_id=chat_session_id,
                        type_id=chat_archive_type_id,
                        path=stream.filename,
                        mime_type_id=mime_type_id,
                        length=stream.length,
                        offset=stream.offset)

                db_session.add(archive)
                
                for user_id in stream.users:
                    archive_user = ChatArchiveUser(
                            user_id=user_id,
                            chat_archive=archive)
                    db_session.add(archive_user)
            
            db_session.commit()
        except Exception as error:
            db_session.rollback()
            raise ArchivePersistException(str(error))
        finally:
            if db_session:
                db_session.close()

