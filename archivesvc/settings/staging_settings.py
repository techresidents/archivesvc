import socket

from default_settings import *

ENV = "staging"

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
DATABASE_NAME = "staging_techresidents"
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

#Archiver settings
ARCHIVER_THREADS = 1
ARCHIVER_POLL_SECONDS = 60
ARCHIVER_JOB_RETRY_SECONDS = 300
ARCHIVER_TIMESTAMP_FILENAMES = True

#Cloudfiles storge settings
CLOUDFILES_USERNAME = "trdev"
CLOUDFILES_API_KEY = None
CLOUDFILES_PASSWORD = "B88mMJqh"
CLOUDFILES_SERVICENET = False
CLOUDFILES_PUBLIC_CONTAINER_NAME = "trdev_public"
CLOUDFILES_PRIVATE_CONTAINER_NAME = "trdev_private"
CLOUDFILES_TIMEOUT = 10
CLOUDFILES_RETRIES = 2
CLOUDFILES_DEBUG_LEVEL = 0
CLOUDFILES_STORAGE_POOL_SIZE = 1

#Filesystem storage settings
FILESYSTEM_STORAGE_LOCATION = "/opt/tr/data/archivesvc/storage"

#Stitch settings
STITCH_FFMPEG_PATH = "/opt/3ps/bin/ffmpeg"
STITCH_SOX_PATH = "/opt/3ps/bin/sox"
STITCH_WORKING_DIRECTORY = "/opt/tr/data/archivesvc/storage"

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
