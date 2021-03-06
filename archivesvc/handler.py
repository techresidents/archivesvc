import logging

from trpycore.factory.base import Factory
from trpycore.pool.queue import QueuePool
from trpycore.thread.util import join
from trsvcscore.service.handler.service import ServiceHandler
from trsvcscore.storage.cloudfiles import CloudfilesStoragePool
from trsvcscore.storage.filesystem import FileSystemStorage
from trrackspace.services.cloudfiles.factory import CloudfilesClientFactory
from trarchivesvc.gen import TArchiveService

import settings
from archive import Archiver
from fetch import TwilioFetcher
from persist import DefaultPersister
from stitch import FFMpegSoxStitcher
from waveform import FFMpegWaveformGenerator


class ArchiveServiceHandler(TArchiveService.Iface, ServiceHandler):
    """Archive service handler."""

    def __init__(self, service):
        """Archive service handler constructor.

        Args:
            service: ArchiveService object.
        """
        super(ArchiveServiceHandler, self).__init__(
                service,
                zookeeper_hosts=settings.ZOOKEEPER_HOSTS,
                database_connection=settings.DATABASE_CONNECTION)

        self.log = logging.getLogger("%s.%s" \
                % (__name__, self.__class__.__name__))
       
        #Rackspace cloudfiles client factory
        self.cloudfiles_client_factory = CloudfilesClientFactory(
                username=settings.CLOUDFILES_USERNAME,
                api_key=settings.CLOUDFILES_API_KEY,
                password=settings.CLOUDFILES_PASSWORD,
                servicenet=settings.CLOUDFILES_SERVICENET,
                retries=settings.CLOUDFILES_RETRIES,
                timeout=settings.CLOUDFILES_TIMEOUT,
                debug_level=settings.CLOUDFILES_DEBUG_LEVEL)

        #public cloudfiles storage pool for storing archives
        #on public cdn.
        self.cloudfiles_public_storage_pool = CloudfilesStoragePool(
                cloudfiles_client_factory=self.cloudfiles_client_factory,
                container_name=settings.CLOUDFILES_PUBLIC_CONTAINER_NAME,
                size=settings.CLOUDFILES_STORAGE_POOL_SIZE)
        
        #private cloudfiles storage pool for storing archives
        #which should not be accessible on cdb.
        self.cloudfiles_private_storage_pool = CloudfilesStoragePool(
                cloudfiles_client_factory=self.cloudfiles_client_factory,
                container_name=settings.CLOUDFILES_PRIVATE_CONTAINER_NAME,
                size=settings.CLOUDFILES_STORAGE_POOL_SIZE)
        
        def filesystem_storage_factory():
            return FileSystemStorage(
                location=settings.FILESYSTEM_STORAGE_LOCATION)
        self.filesystem_storage_pool = QueuePool(
                size=settings.ARCHIVER_THREADS,
                factory=Factory(filesystem_storage_factory))

        def fetcher_factory():
            return TwilioFetcher(
                    db_session_factory=self.get_database_session,
                    storage_pool=self.filesystem_storage_pool,
                    twilio_account_sid=settings.TWILIO_ACCOUNT_SID,
                    twilio_auth_token=settings.TWILIO_AUTH_TOKEN,
                    twilio_application_sid=settings.TWILIO_APPLICATION_SID)
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

        def waveform_generator_factory():
            return FFMpegWaveformGenerator(
                    ffmpeg_path=settings.STITCH_FFMPEG_PATH,
                    storage_pool=self.filesystem_storage_pool,
                    working_directory=settings.STITCH_WORKING_DIRECTORY)
        self.waveform_generator_pool = QueuePool(
                size=settings.ARCHIVER_THREADS,
                factory=Factory(waveform_generator_factory))
    
        def persister_factory():
            return DefaultPersister(
                    db_session_factory=self.get_database_session,
                    local_storage_pool=self.filesystem_storage_pool,
                    public_storage_pool=self.cloudfiles_public_storage_pool,
                    private_storage_pool=self.cloudfiles_private_storage_pool)
        self.persister_pool = QueuePool(
                size=settings.ARCHIVER_THREADS,
                factory=Factory(persister_factory))
        
        #archiver coordinates creation of archives.
        self.archiver = Archiver(
                db_session_factory=self.get_database_session,
                fetcher_pool=self.fetcher_pool,
                stitcher_pool=self.stitcher_pool,
                waveform_generator_pool=self.waveform_generator_pool,
                persister_pool=self.persister_pool,
                num_threads=settings.ARCHIVER_THREADS,
                poll_seconds=settings.ARCHIVER_POLL_SECONDS,
                job_retry_seconds=settings.ARCHIVER_JOB_RETRY_SECONDS,
                timestamp_filenames=settings.ARCHIVER_TIMESTAMP_FILENAMES)
    
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
