import logging
import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from testbase import WORKING_DIRECTORY
from trpycore.pool.simple import SimplePool
from trsvcscore.storage.filesystem import FileSystemStorage

import settings
from fetch import TokboxFetcher

class TokboxFetcherTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.DEBUG)
        cls.engine = create_engine(settings.DATABASE_CONNECTION)
        cls.db_session_factory = sessionmaker(bind=cls.engine)
        cls.storage_pool = SimplePool(FileSystemStorage(WORKING_DIRECTORY))
        cls.fetcher = TokboxFetcher(
                storage_pool=cls.storage_pool,
                db_session_factory=cls.db_session_factory,
                tokbox_api_key=settings.TOKBOX_API_KEY,
                tokbox_api_secret=settings.TOKBOX_API_SECRET)

    @classmethod
    def tearDownClass(cls):
        pass
    
    def test_fetch(self):
        manifest = self.fetcher.fetch(chat_session_id=1, output_filename="output/fetch")
        self.assertIsNotNone(manifest.filename)
        for stream in manifest.archive_streams:
            self.assertIsNotNone(stream.filename)
            self.assertIsNotNone(stream.length)
            self.assertIsNotNone(stream.offset)


if __name__ == '__main__':
    unittest.main()
