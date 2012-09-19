import abc
import logging
import os

from trsvcscore.db.models import MimeType
from trsvcscore.db.models import ChatArchive, ChatArchiveType, ChatArchiveUser

from stream import ArchiveStreamType

class ArchivePersisterException(Exception):
    """Archive persister exception"""
    pass

class ArchivePersister(object):
    """Archive persister abstract base class.

    Archive persister is responsible for persisting archive media
    streams and associated models.
    """
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractmethod
    def persist(self, chat_session_id,  archive_streams):
        """Persist archive stream for specified chat session id.

        Args:
            chat_session_id: chat session id
            archive_streams: list of ArchiveStream objects to
                persist.
        Raises:
            ArchivePersisterException
        """
        return


class DefaultPersister(ArchivePersister):
    """Default persister implementation.

    Archive persister is responsible for persisting archive media
    streams and associated models. Media streams will be 
    persisted to the specified storage pools. Streams containing
    video will be stored with the private_storage_pool, while
    streams only containing audio will be stored with the
    public_storage_pool.
    """

    def __init__(self,
            db_session_factory,
            local_storage_pool,
            public_storage_pool=None,
            private_storage_pool=None):
        """DefaultPersister constructor.

        Args:
            db_session_factory: callable returning sqlalchemy Session object
            local_storage_pool: Pool object containing Storage objects
                accessible on the local filesystem.
            public_storage_pool: Pool object containing Storage object
                which will be used to store media stream which have
                been anonymized and can be made public.
            private_storage_pool: Pool object containing Storage object
                which will be used to store media stream which contain
                video and should be kept private.
        """
        self.db_session_factory = db_session_factory
        self.local_storage_pool = local_storage_pool
        self.public_storage_pool = public_storage_pool
        self.private_storage_pool = private_storage_pool

        self.log = logging.getLogger("%s.%s" \
                % (__name__, self.__class__.__name__))

    def _upload_public_archive_streams(self, archive_streams):
        """Upload public archive streams.

        Uploads public (audio only) archive streams in archive_streams to 
        self.public_storage_pool.

        Args:
            archive_streams: list of ArchiveStream objects.
        Raises:
            StroageException
        """
        if self.public_storage_pool is None:
            return

        #public archive streams
        with self.public_storage_pool.get() as public_storage:
            with self.local_storage_pool.get() as local_storage:
                for stream in archive_streams:
                    if stream.type == ArchiveStreamType.STITCHED_AUDIO_STREAM:
                        if public_storage.exists(stream.filename):
                            continue
                        self.log.info("Uploading archive stream '%s'" \
                                % stream)
                        with local_storage.open(stream.filename, "r") as file:
                            public_storage.save(stream.filename, file)
                        self.log.info("Done uploading archive stream '%s'" \
                                % stream)
        
    def _upload_private_archive_streams(self, archive_streams):
        """Upload private archive streams.

        Uploads private (containing video) archive streams in archive_streams
        to self.private_storage_pool.

        Args:
            archive_streams: list of ArchiveStream objects.
        Raises:
            StroageException
        """
        if self.private_storage_pool is None:
            return

        #private archives streams
        with self.private_storage_pool.get() as private_storage:
            with self.local_storage_pool.get() as local_storage:
                for stream in archive_streams:
                    if stream.type != ArchiveStreamType.STITCHED_AUDIO_STREAM:
                        if private_storage.exists(stream.filename):
                            continue
                        self.log.info("Uploading archive stream '%s'" \
                                % stream)
                        with local_storage.open(stream.filename, "r") as file:
                            private_storage.save(stream.filename, file)
                        self.log.info("Done uploading archive stream '%s'" \
                                % stream)
    
    def _get_chat_archive_type_id(self, archive_stream):
        """Get ChatArchiveType model id for specified archive_stream.
        
        Args:
            archive_stream: ArchiveStream object
        Returns:
            ChatArchiveType model id for specified archive_stream.
        Raises:
            SQLAlchemy exceptions.
        """
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
        """Get MimeType model id for specified archive_stream.

        Args:
            archive_stream: ArchiveStream object
        Returns:
            MimeType model id
        Raises:
            SQLAlchemy exceptions.
        """
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
        """Persist archive stream for specified chat session id.

        Args:
            chat_session_id: chat session id
            archive_streams: list of ArchiveStream objects to
                persist.
        Raises:
            ArchivePersisterException
        """
        try:
            db_session = self.db_session_factory()
            
            self._upload_public_archive_streams(archive_streams)
            self._upload_private_archive_streams(archive_streams)

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
                
                if stream.type == ArchiveStreamType.STITCHED_AUDIO_STREAM:
                    is_public = True
                else:
                    is_public =False

                archive = ChatArchive(
                        chat_session_id=chat_session_id,
                        type_id=chat_archive_type_id,
                        path=stream.filename,
                        mime_type_id=mime_type_id,
                        public=is_public,
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
            self.log.exception(error)
            db_session.rollback()
            raise ArchivePersisterException(str(error))
        finally:
            if db_session:
                db_session.close()

