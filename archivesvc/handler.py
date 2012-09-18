import logging

from trpycore.cloudfiles_common.factory import CloudfilesConnectionFactory
from trpycore.factory.base import Factory
from trpycore.pool.queue import QueuePool
from trpycore.thread.util import join
from trsvcscore.service.handler.service import ServiceHandler
from trsvcscore.storage.cloudfiles import CloudfilesStoragePool
from trsvcscore.storage.filesystem import FileSystemStorage
from trarchivesvc.gen import TArchiveService

import settings
from archive import Archiver
from fetch import TokboxFetcher
from persist import DefaultPersister
from stitch import FFMpegSoxStitcher


class ArchiveServiceHandler(TArchiveService.Iface, ServiceHandler):
    def __init__(self, service):
        super(ArchiveServiceHandler, self).__init__(
                service,
                zookeeper_hosts=settings.ZOOKEEPER_HOSTS,
                database_connection=settings.DATABASE_CONNECTION)

        self.log = logging.getLogger("%s.%s" % (__name__, ArchiveServiceHandler.__name__))
        
        self.cloudfiles_connection_factory = CloudfilesConnectionFactory(
                username=settings.CLOUDFILES_USERNAME,
                api_key=settings.CLOUDFILES_API_KEY,
                password=settings.CLOUDFILES_PASSWORD,
                servicenet=settings.CLOUDFILES_SERVICENET,
                timeout=settings.CLOUDFILES_TIMEOUT)

        self.cloudfiles_storage_pool = CloudfilesStoragePool(
                cloudfiles_connection_factory=self.cloudfiles_connection_factory,
                container_name=settings.CLOUDFILES_CONTAINER_NAME,
                size=settings.CLOUDFILES_STORAGE_POOL_SIZE)
        
        def filesystem_storage_factory():
            return FileSystemStorage(
                location=settings.FILESYSTEM_STORAGE_LOCATION)
        self.filesystem_storage_pool = QueuePool(
                size=settings.ARCHIVER_THREADS,
                factory=Factory(filesystem_storage_factory))

        def fetcher_factory():
            return TokboxFetcher(
                    db_session_factory=self.get_database_session,
                    storage_pool=self.filesystem_storage_pool,
                    tokbox_api_key=settings.TOKBOX_API_KEY,
                    tokbox_api_secret=settings.TOKBOX_API_SECRET,
                    tokbox_url=settings.TOKBOX_URL)
        self.fetcher_pool = QueuePool(
                size=settings.ARCHIVER_THREADS,
                factory=Factory(fetcher_factory))
        
        def stitcher_factory():
            return FFMpegSoxStitcher(
                    ffmpeg_path=settings.STITCH_FFMPEG_PATH,
                    sox_path=settings.STITCH_SOX_PATH,
                    storage_pool=self.filesystem_storage_pool,
                    working_directory=settings.STITCH_WORKING_DIRECTORY)
        self.stitcher_pool = QueuePool(
                size=settings.ARCHIVER_THREADS,
                factory=Factory(stitcher_factory))
    
        def persister_factory():
            return DefaultPersister(
                    db_session_factory=self.get_database_session,
                    storage_pool=self.filesystem_storage_pool,
                    remote_storage_pool=self.cloudfiles_storage_pool)
        self.persister_pool = QueuePool(
                size=settings.ARCHIVER_THREADS,
                factory=Factory(persister_factory))

        self.archiver = Archiver(
                db_session_factory=self.get_database_session,
                fetcher_pool=self.fetcher_pool,
                stitcher_pool=self.stitcher_pool,
                persister_pool=self.persister_pool,
                num_threads=settings.ARCHIVER_THREADS,
                poll_seconds=settings.ARCHIVER_POLL_SECONDS)
    
    def start(self):
        """Start handler."""
        super(ArchiveServiceHandler, self).start()
        self.archiver.start()

    
    def stop(self):
        """Stop handler."""
        self.archiver.stop()
        super(ArchiveServiceHandler, self).stop()

    def join(self, timeout=None):
        """Join handler."""
        join([self.archiver, super(ArchiveServiceHandler, self)], timeout)

    def reinitialize(self, requestContext):
        """Reinitialize - nothing to do."""
        pass
