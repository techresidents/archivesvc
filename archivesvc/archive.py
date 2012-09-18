import logging
import threading

import OpenTokSDK

from trpycore.encode.basic import basic_encode
from trpycore.thread.util import join
from trpycore.thread.threadpool import ThreadPool
from trsvcscore.db.job import DatabaseJobQueue, QueueEmpty, QueueStopped, JobOwned
from trsvcscore.db.models import ChatArchiveJob

import settings
from stream import ArchiveStreamType

class ArchiverThreadPool(ThreadPool):
    """Schedule chat sessions.

    Given a work item (chat_id), scheduler will create the necessary
    chat sessions for the chat based on the existing chat registrations
    with checked_in == True.

    Additionally, the scheduler will create the necessary chat users
    and link existing registrations to their assigned chat sessions.
    """
    def __init__(self,
            num_threads,
            db_session_factory,
            fetcher_pool,
            stitcher_pool,
            persister_pool):
        """Constructor.

        Arguments:
            num_threads: number of worker threads
            db_session_factory: callable returning new sqlalchemy
                db session.
        """
        self.log = logging.getLogger(__name__)
        self.db_session_factory = db_session_factory
        self.fetcher_pool = fetcher_pool
        self.stitcher_pool = stitcher_pool
        self.persister_pool = persister_pool
        super(ArchiverThreadPool, self).__init__(num_threads)
    
    def process(self, database_job):
        """Worker thread process method.

        This method will be invoked by each worker thread when
        a new work item (chat_id) is put on the queue.
        """
        try:
            with database_job as job:
                chat_session_id = job.chat_session_id
                encoded_chat_session_id = basic_encode(chat_session_id)
                output_filename = "archive/%s" % encoded_chat_session_id
                
                #fetch archive streams
                with self.fetcher_pool.get() as fetcher:
                    archive_manifest = fetcher.fetch(
                            chat_session_id=chat_session_id,
                            output_filename=output_filename)
                
                #stitch streams
                with self.stitcher_pool.get() as stitcher:
                    stitched_archive_stream = stitcher.stitch(
                            archive_streams=archive_manifest.archive_streams,
                            output_filename=output_filename)
                
                #persist streams
                with self.persister_pool.get() as persister:
                    persisted_streams = [stitched_archive_stream]
                    for stream in archive_manifest.archive_streams:
                        if stream.type == ArchiveStreamType.USER_VIDEO_STREAM:
                            persisted_streams.append(stream)
                    persister.persist(
                            chat_session_id=chat_session_id,
                            archive_streams=persisted_streams)
        except JobOwned:
            pass
        except Exception as error:
            logging.exception(error)

    def _create_tokbox_session(self):
        """Create tokbox session through Tokbox API.

        Returns:
            Tokbox session object.
        """
        #Create the tokbox session
        opentok = OpenTokSDK.OpenTokSDK(
                settings.TOKBOX_API_KEY,
                settings.TOKBOX_API_SECRET, 
                settings.TOKBOX_IS_STAGING) 
        
        #IP passed to tokbox when session is created will be used to determine
        #tokbox server location for chat session. Note that tokboxchat sessions
        #never expire. But tokbox user chat tokens can be set to expire.
        session = opentok.create_session('127.0.0.1')

        return session

class Archiver(object):
    """Archiver creates and delegates work items to the ArchiverThreadPool.
    """

    def __init__(self,
            db_session_factory,
            fetcher_pool,
            stitcher_pool,
            persister_pool,
            num_threads,
            poll_seconds=60):
        """Constructor.

        Arguments:
            db_session_factory: callable returning a new sqlalchemy db session
            num_threads: number of worker threads
            poll_seconds: number of seconds between db queries to detect
                chat requiring scheduling.
        """
        self.log = logging.getLogger(__name__)
        self.db_session_factory = db_session_factory
        self.fetcher_pool = fetcher_pool
        self.stitcher_pool = stitcher_pool
        self.persister_pool = persister_pool
        self.num_threads = num_threads
        self.poll_seconds = poll_seconds
        self.thread = None

        self.threadpool = ArchiverThreadPool(
                num_threads=num_threads,
                db_session_factory=db_session_factory,
                fetcher_pool=fetcher_pool,
                stitcher_pool=stitcher_pool,
                persister_pool=persister_pool)

        self.db_job_queue = DatabaseJobQueue(
                owner="archivesvc",
                model_class=ChatArchiveJob,
                db_session_factory=self.db_session_factory,
                poll_seconds=self.poll_seconds)

        self.running = False
    
    def start(self):
        """Start scheduler."""
        if not self.running:
            self.running = True
            self.threadpool.start()
            self.db_job_queue.start()
            self.thread = threading.Thread(target=self.run)
            self.thread.start()
    
    def run(self):
        while self.running:
            try:
                job = self.db_job_queue.get()
                self.threadpool.put(job)
            except QueueEmpty:
                pass
            except QueueStopped:
                break
            except Exception as error:
                logging.exception(error)
        
        self.running = False

    def stop(self):
        """Stop scheduler."""
        if self.running:
            self.running = False
            self.db_job_queue.stop()
            self.threadpool.stop()
    
    def join(self, timeout):
        """Join scheduler."""
        threads = [self.threadpool, self.db_job_queue]
        if self.thread is not None:
            threads.append(self.thread)
        join(threads, timeout)
