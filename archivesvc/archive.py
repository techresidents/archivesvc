import datetime
import json
import logging
import threading
import time

from sqlalchemy.sql import func

from trpycore.encode.basic import basic_encode
from trpycore.thread.util import join
from trpycore.thread.threadpool import ThreadPool
from trpycore.timezone import tz
from trsvcscore.db.job import DatabaseJobQueue, QueueEmpty, QueueStopped, JobOwned
from trsvcscore.db.models import ChatArchiveJob

from stream import ArchiveStreamType


class ArchiverThreadPool(ThreadPool):
    """Archiver thread pool.

    Given a work item, DatabaseJob object, archiver will download
    single user media archive streams from the fetcher, anonymize
    and stitch the streams into a single audio-only stream,
    and persist the streams and db models.
    """
    def __init__(self,
            num_threads,
            db_session_factory,
            fetcher_pool,
            stitcher_pool,
            waveform_generator_pool,
            persister_pool,
            job_retry_seconds,
            timestamp_filenames=False):
        """Archive threadpool constructor.

        Arguments:
            num_threads: number of worker threads
            db_session_factory: callable returning new sqlalchemy
                db session.
            fetcher_pool: Pool object returning a Fetcher object.
            stitcher_pool: Pool object returning a Stitcher object.
            waveform_generator_pool: Pool object returning a WaveformGenerator object.
            persister_pool: Pool object returning a Persister object.
            num_threads: number of worker threads
            job_retry_seconds: number of seconds to wait before retrying
                a failed job.
            timestamp_filenames: optional boolean indicating that epoch
                timestamps should be used in filenames to guarantee
                uniqueness. This is useful for non-prod environments.
        """
        self.db_session_factory = db_session_factory
        self.fetcher_pool = fetcher_pool
        self.stitcher_pool = stitcher_pool
        self.waveform_generator_pool = waveform_generator_pool
        self.persister_pool = persister_pool
        self.job_retry_seconds = job_retry_seconds
        self.timestamp_filenames = timestamp_filenames
        super(ArchiverThreadPool, self).__init__(num_threads)

        self.log = logging.getLogger("%s.%s" \
                % (__name__, self.__class__.__name__))

    def _retry_job(self, job):
        """Create a ChatArchiveJob to retry a failed job.

        This method will create a new ChatArchiveJob, which
        will be delayed by job_retry_seconds, as long as 
        the number of retries_remaining on the failed
        job is greather than 1.
        """
        try:
            db_session = self.db_session_factory()
            if job.retries_remaining:
                not_before =tz.utcnow() + \
                        datetime.timedelta(seconds=self.job_retry_seconds)
                
                self.log.info("Creating retry job for chat_id=%s at %s" \
                        % (job.chat_id, not_before))

                retry = ChatArchiveJob(
                        chat_id=job.chat_id,
                        created=func.current_timestamp(),
                        not_before=not_before,
                        data=job.data,
                        retries_remaining=job.retries_remaining-1)
                db_session.add(retry)
                db_session.commit()
            else:
                self.log.info("No retries remaining for job for chat_id=%s" \
                        % (job.chat_id))
                self.log.error("Job for chat_id=%s failed!" \
                        % (job.chat_id))
        except Exception as error:
            self.log.exception(error)
            db_session.rollback()
        finally:
            if db_session:
                db_session.close()
    
    def _fetch_archives(self, chat_id, chat_session, output_filename):
        """Fetch and download single-user media streams.
        
        Args:
            chat_id: chat id
            chat_session: session data from chat
            output_filename: base output filename to be used
                to construct fetched archive filenames.
        Returns:
            ArchiveStreamManifest object or None if no archives
            exists for the given chat_id.
        Raises:
            ArchiveFetcherException
        """
        result = None

        self.log.info("Fetching archives for chat_id=%s" \
                % chat_id)

        with self.fetcher_pool.get() as fetcher:
            archive_manifest = fetcher.fetch(
                    chat_id=chat_id,
                    chat_session=chat_session,
                    output_filename=output_filename)
            if archive_manifest is not None:
                result = archive_manifest

        self.log.info("Done fetching archives for chat_id=%s" \
                % chat_id)

        return result

    def _stitch_archives(self,
            chat_id,
            archive_manifest,
            output_filename):
        """Anonymize and stitch archives streams into single stream.
        
        Returns an anonymized and stitched ArchiveStream object
        for all streams specified in the archive_manifeset.

        Args:
            chat_id: chat id
            archive_manifest: ArchiveStreamManifset object
            output_filename: base output filename to be used
                to construct fetched archive filenames.
        Returns:
            list of stitched ArchiveStream objects.
        Raises:
            ArchiveStitcherException
        """

        self.log.info("Stitching archives for chat_id=%s" \
                % chat_id)

        with self.stitcher_pool.get() as stitcher:
            stitched_archive_streams = stitcher.stitch(
                    archive_streams=archive_manifest.archive_streams,
                    output_filename=output_filename)

        self.log.info("Done stitching archives for chat_id=%s" \
                % chat_id)

        return stitched_archive_streams

    def _generate_waveform(self,
            chat_id,
            archive_streams,
            output_filename):
        """Persist archive media streams.
        
        Args:
            chat_id: chat id
            archive_streams: list of stitched ArchiveStream objects
            output_filename: base output filename to be used
                to generate waveform.
        Returns:
            Updated list of archive stream objects
        Raises:
            ArchiveWaveformGeneratorException
        """

        self.log.info("Generatoring waveforms for chat_id=%s" \
                % chat_id)
        
        #only generate waveform for first archive and copy to rest
        with self.waveform_generator_pool.get() as waveform_generator:
            waveform_generator.generate(
                    archive_stream=archive_streams[0],
                    output_filename=output_filename)
        
        #copy waveform to rest of archive streams
        for stream in archive_streams[1:]:
            stream.waveform = archive_streams[0].waveform
            stream.waveform_filename = archive_streams[0].waveform_filename

        self.log.info("Done generating waveform for chat_id=%s" \
                % chat_id)

        return archive_streams
    
    def _persist_archives(self,
            chat_id,
            archive_manifest,
            stitched_archive_streams):
        """Persist archive media streams.
        
        Args:
            chat_id: chat id
            archive_manifest: ArchiveStreamManifest object
            stitched_archive_streams: list of stitched ArchiveStream objects
        Raises:
            ArchivePersisterException
        """

        self.log.info("Persisting archives for chat_id=%s" \
                % chat_id)

        with self.persister_pool.get() as persister:
            persisted_streams = stitched_archive_streams
            for stream in archive_manifest.archive_streams:
                if stream.type != ArchiveStreamType.STITCHED_AUDIO_STREAM:
                    persisted_streams.append(stream)
            persister.persist(
                    chat_id=chat_id,
                    archive_streams=persisted_streams)

        self.log.info("Done persisting archives for chat_id=%s" \
                % chat_id)

    def _delete_fetcher_streams(self, chat_id, chat_session):
        """Delete media streams from fetcher.
        
        Deletes media streams stored at fetcher location.

        Args:
            chat_id: chat_id
        Raises:
            ArchiveFetcherException
        """
        self.log.info("Deleting archives for chat_id=%s" \
                % chat_id)

        with self.fetcher_pool.get() as fetcher:
            fetcher.delete(chat_id, chat_session)

        self.log.info("Done deleting archives for chat_id=%s" \
                % chat_id)

    def process(self, database_job):
        """Worker thread process method.

        Args:
            database_job: DatabaseJob object wrapping a ChatArchiveJob
                model in a convenient context manager.

        This method will be invoked by each worker thread when
        a new work item (chat_id) is put on the queue.
        """
        try:
            job = None
            with database_job as job:
                chat_id = job.chat_id
                encoded_chat_id = basic_encode(chat_id)
                chat_session = json.loads(job.data)
                output_filename = "archive/%s" % encoded_chat_id
                if self.timestamp_filenames:
                    output_filename += "-%s" % time.time()

                self.log.info("Creating archive for chat_id=%s (%s)" \
                        % (chat_id, encoded_chat_id))
                
                #fetch archive streams
                archive_manifest = self._fetch_archives(
                        chat_id=chat_id,
                        chat_session=chat_session,
                        output_filename=output_filename)
                if archive_manifest is None \
                        or not archive_manifest.archive_streams:
                    self.log.info("No archives for chat_id=%s" \
                            % chat_id)
                    return
    
                #stitch streams
                stitched_archive_streams = self._stitch_archives(
                        chat_id=chat_id,
                        archive_manifest=archive_manifest,
                        output_filename=output_filename)
                
                #generate waveform
                stitched_archive_streams = self._generate_waveform(
                        chat_id=chat_id,
                        archive_streams=stitched_archive_streams,
                        output_filename=output_filename)
                
                #persist streams
                self._persist_archives(
                        chat_id=chat_id,
                        archive_manifest=archive_manifest,
                        stitched_archive_streams=stitched_archive_streams)
                
                #delete fetcher streams
                self._delete_fetcher_streams(chat_id, chat_session)

                self.log.info("Done with archive for chat_id=%s (%s)" \
                        % (chat_id, encoded_chat_id))

        except JobOwned:
            self.log.info("Job for chat_id=%s already owned." \
                    % (job.chat_id))
        except Exception as error:
            if job:
                self.log.error("Job for chat_id=%s failed." \
                        % (job.chat_id))
                self.log.exception(error)
                self._retry_job(job)
            else:
                self.log.error("Job failed but is empty ...")
                self.log.exception(error)



class Archiver(object):
    """Archiver creates and delegates work items to the ArchiverThreadPool.
    """

    def __init__(self,
            db_session_factory,
            fetcher_pool,
            stitcher_pool,
            waveform_generator_pool,
            persister_pool,
            num_threads,
            poll_seconds=60,
            job_retry_seconds=300,
            timestamp_filenames=False):
        """Constructor.

        Arguments:
            db_session_factory: callable returning a new sqlalchemy db session
            fetcher_pool: Pool object returning a Fetcher object.
            stitcher_pool: Pool object returning a Stitcher object.
            waveform_generator_pool: Pool object returning WaveformGenerator object
            persister_pool: Pool object returning a Persister object.
            num_threads: number of worker threads
            poll_seconds: number of seconds between db queries to detect
                new archive jobs.
            job_retry_seconds: number of seconds to wait before retrying
                a failed job.
            timestamp_filenames: optional boolean indicating that epoch
                timestamps should be used in filenames to guarantee
                uniqueness. This is useful for non-prod environments.
        """
        self.db_session_factory = db_session_factory
        self.fetcher_pool = fetcher_pool
        self.stitcher_pool = stitcher_pool
        self.waveform_generator_pool = waveform_generator_pool
        self.persister_pool = persister_pool
        self.num_threads = num_threads
        self.poll_seconds = poll_seconds
        self.job_retry_seconds = job_retry_seconds
        self.timestamp_filenames = timestamp_filenames
        self.thread = None

        self.threadpool = ArchiverThreadPool(
                num_threads=num_threads,
                db_session_factory=db_session_factory,
                fetcher_pool=fetcher_pool,
                stitcher_pool=stitcher_pool,
                waveform_generator_pool=waveform_generator_pool,
                persister_pool=persister_pool,
                job_retry_seconds=job_retry_seconds,
                timestamp_filenames=timestamp_filenames)

        self.db_job_queue = DatabaseJobQueue(
                owner="archivesvc",
                model_class=ChatArchiveJob,
                db_session_factory=self.db_session_factory,
                poll_seconds=self.poll_seconds)

        self.running = False

        self.log = logging.getLogger("%s.%s" \
                % (__name__, self.__class__.__name__))
    
    def start(self):
        """Start archiver."""
        if not self.running:
            self.running = True
            self.threadpool.start()
            self.db_job_queue.start()
            self.thread = threading.Thread(target=self.run)
            self.thread.start()
    
    def run(self):
        """Run archiver.

        This method is invoked in the context of self.thread
        """
        while self.running:
            try:
                job = self.db_job_queue.get()
                self.threadpool.put(job)
            except QueueEmpty:
                pass
            except QueueStopped:
                break
            except Exception as error:
                self.log.exception(error)
        
        self.running = False

    def stop(self):
        """Stop archiver."""
        if self.running:
            self.running = False
            self.db_job_queue.stop()
            self.threadpool.stop()
    
    def join(self, timeout):
        """Join archiverer."""
        threads = [self.threadpool, self.db_job_queue]
        if self.thread is not None:
            threads.append(self.thread)
        join(threads, timeout)
