import socket

from default_settings import *

ENV = "localdev"

#Service Settings
SERVICE = "archivesvc"
SERVICE_PID_FILE = "/opt/tr/data/%s/pid/%s.%s.pid" % (SERVICE, SERVICE, ENV)
SERVICE_HOSTNAME = socket.gethostname()
SERVICE_FQDN = socket.gethostname()
SERVICE_JOIN_TIMEOUT = 1

#Thrift Server settings
THRIFT_SERVER_ADDRESS = socket.gethostname()
THRIFT_SERVER_INTERFACE = "0.0.0.0"
THRIFT_SERVER_PORT = 9094

#Database settings
DATABASE_HOST = "localhost"
DATABASE_NAME = "localdev_techresidents"
DATABASE_USERNAME = "techresidents"
DATABASE_PASSWORD = "techresidents"
DATABASE_CONNECTION = "postgresql+psycopg2://%s:%s@/%s?host=%s" % (DATABASE_USERNAME, DATABASE_PASSWORD, DATABASE_NAME, DATABASE_HOST)

#Zookeeper settings
ZOOKEEPER_HOSTS = ["localhost:2181"]

#Riak settings
RIAK_HOST = "localhost"
RIAK_PORT = 8087
RIAK_SESSION_BUCKET = "tr_sessions"
RIAK_SESSION_POOL_SIZE = 4

#Tokbox settings
TOKBOX_API_KEY = '15889991'
TOKBOX_API_SECRET = '19a6fb225790a2cf3e048c58ef2fdcc425e7b599'

#Scheduler settings
SCHEDULER_THREADS = 4
SCHEDULER_POLL_SECONDS = 60
SCHEDULER_MIN_GROUP_SIZE = 2
SCHEDULER_MAX_GROUP_SIZE = 3


#Logging settings
LOGGING = {
    "version": 1,

    "formatters": {
        "brief_formatter": {
            "format": "%(levelname)s: %(message)s"
        },

        "long_formatter": {
            "format": "%(asctime)s %(levelname)s: %(name)s %(message)s"
        }
    },

    "handlers": {

        "console_handler": {
            "level": "ERROR",
            "class": "logging.StreamHandler",
            "formatter": "brief_formatter",
            "stream": "ext://sys.stdout"
        },

        "file_handler": {
            "level": "DEBUG",
            "class": "logging.handlers.TimedRotatingFileHandler",
            "formatter": "long_formatter",
            "filename": "/opt/tr/data/%s/logs/%s.%s.log" % (SERVICE, SERVICE, ENV),
            "when": "midnight",
            "interval": 1,
            "backupCount": 7
        }
    },
    
    "root": {
        "level": "DEBUG",
        "handlers": ["console_handler", "file_handler"]
    }
}

