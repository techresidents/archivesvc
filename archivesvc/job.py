import threading
import Queue

from sqlalchemy.sql import func

from trpycore.timezone import tz

class DatabaseJob(object):

    def __init__(self, owner, model_class, model_id,  db_session_factory):
        self.owner = owner
        self.model_class = model_class
        self.model_id = model_id
        self.model = None
        self.db_session_factory = db_session_factory
    
    def __enter__(self):
        self.model = self._start()
        return self.model
    
    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            self._abort()
        else:
            self._end()
    
    def _start(self):
        # This query.update generates the following sql:
        # UPDATE <table> SET owner='<owner>' WHERE
        # <table>.id = <id> AND <table>.owner IS NULL
        rows_updated = self.db_session.query(self.model_class).\
            filter(self.model_class.id==self.model_id).\
            filter(self.model_class.owner==None).\
            update({
                self.model_class.owner: self.owner,
                self.model_class.start: tz.utcnow()
            })

        if not rows_updated:
            pass

        model = self.db_session.query(self.model_class)\
                .filter(self.model_class.id==self.model_id)
        
        return model

    def _end(self):
        self.model.end = func.current_timestamp()
        self.model.successful = True
        self.db_session.commit()

    def _abort(self):
        self.model.end = func.current_timestamp()
        self.model.successful = True
        self.db_session.commit()

class DatabaseJobQueue(object):
    
    def __init__(self, model_class, db_session_factory):
        self.model_class = model_class
        self.db_session_factory = db_session_factory
        self.queue = Queue.Queue()
        self.exit = threading.Event()
        self.running = False
        self.thread = None
    
    def _query(self, db_session):
        return db_session.query(self.model_class).\
                filter(self.model_class.owner == None).\
                filter(self.model_class.start == None)

    def get(self, block=True, timeout=None):
        result = self.queue.get(block, timeout)
        if result is None:
            raise Exception()
        return result

    def put(self, model):
        session = self.db_session_factory()
        session.add(model)
        session.commit()
    
    def start(self):
        if not self.running:
            self.exit.clear()
            self.running = True
            self.thread = threading.Thread(target=self.run)

    def run(self):
        session = self.db_session_factory()

        while self.running:
            try:
                for job in self._query(session):
                    database_job = DatabaseJob(
                            model_class=self.model_class,
                            model_id=job.id)
                    self.queue.put(database_job)
                session.commit()

            except Exception as error:
                session.rollback()
                self.log.exception(error)
            finally:
                self.exit.wait(10)

        session.close()
    
    def stop(self):
        if self.running:
            self.running = False
            self.exit.set()

    def join(self, timeout=None):
        if self.thread is not None:
            self.thread.join(timeout)
