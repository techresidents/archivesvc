import abc
import logging
import urllib2

from twilio.rest import TwilioRestClient

from stream import ArchiveStreamManifest, ArchiveStream, ArchiveStreamType

class ArchiveFetcherException(Exception):
    """Archive fetcher exception."""
    pass


class ArchiveFetcher(object):
    """Archive fetcher abstract base class.

    Archive fetcher is responsible for fetching (downloading)
    media streams from our video chat vendor.
    """
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractmethod
    def fetch(self, chat_id, output_filename):
        """Fetch media streams for the specified chat id.

        Args:
            chat_id: chat id
            output_filename: output base filename to be used
                to construct archive stream filenames.
        Returns:
            ArchiveStreamManifest object containing references
            to all downloaded media streams.
        Raises:
            ArchiveFetcherException
        """
        return

    @abc.abstractmethod
    def delete(self, chat_id):
        """Delete media streams from video chat vendor.

        Args:
            chat_id: chat id
        Raises:
        ArchiveFetcherException
        """
        return


class TwilioFetcher(ArchiveFetcher):
    """Twilio archive fetcher.

    Fetches (downloads) recordings from Twilio
    """

    def __init__(self,
            db_session_factory,
            storage_pool,
            twilio_account_sid,
            twilio_auth_token,
            twilio_application_sid):
        """Twilio fetcher constructor.

        Args:
            db_session_factory: callable return a sqlaclhemy Session object
            storage_pool: Pool object of Storage objects to use to store
                media streams.
            twilio_account_sid: Twilio account sid
            twilio_auth_token: Twilio auth token
            twilio_application_sid: Twilio applicatoin sid
        """
        self.db_session_factory = db_session_factory
        self.twilio_account_sid = twilio_account_sid
        self.twilio_auth_token = twilio_auth_token
        self.twilio_application_sid = twilio_application_sid
        self.storage_pool = storage_pool
        self.log = logging.getLogger("%s.%s" \
                % (__name__, self.__class__.__name__))

        #create twilio client
        self.twilio_client = TwilioRestClient(
                self.twilio_account_sid, self.twilio_auth_token)
    
   
    def _get_call_sids(self, chat_session):
        """Get Twilio call sids from chat session data

        Args:
            chat_session: chat session data dict
        Returns:
            list of Twilio call sids for chat
        """
        call_sids = []
        if "twilio_data" in chat_session:
            users_data = chat_session["twilio_data"].get("users", {})
            for user_id, data in users_data.items():
                calls = data.get("calls", {})
                call_sids.extend(calls.keys())
        return call_sids

    def _get_recording(self, call_sid):
        """Get Twilio Recording resource object for given call.

        Args:
            call_sid: Twilio call_sid
        Returns:
            Twilio Recording resource object if recording exists, 
            None otherwise.
        """
        result = None
        twilio_recordings = self.twilio_client.recordings.list(
                call_sid=call_sid)
        if twilio_recordings:
            result = twilio_recordings[0]
        return result

    def _fetch_recording(self, call_sid, output_filename):
        """Fetch Twilio recording for given call.
        
        Fetches the Twilio audio stream file and stores it in
        self.storage_pool as output_filename.

        Args:
            call_sid: Twilio call_sid
        Raises:
            urllib2.HTTPError, StorageException, ArchiveFetcherException
        """

        with self.storage_pool.get() as storage_backend:
            if not storage_backend.exists(output_filename):
                recording = self._get_recording(call_sid)
                if recording is None:
                    msg = "no recording for call %s" % call_sid
                    raise ArchiveFetcherException(msg)
                url = recording.formats["mp3"]
                self.log.info("Downloading recording from %s" % url)

                request = urllib2.Request(url)
                result = urllib2.urlopen(request)
                storage_backend.save(output_filename, result)

    def fetch(self, chat_id, chat_session, output_filename):
        """Fetch Tokbox media streams for the specified chat id.

        Fetches the Tokbox manifest file and  video stream files, storing
        them in self.storage_pool using output_filename as the base
        filename.
        
        Args:
            chat_id: chat id
            chat_session: Session data for the chat which must contain
                the twilio_data
            output_filename: output base filename to be used
                to construct archive stream filenames.
        Returns:
            ArchiveStreamManifest object containing references
            to all downloaded media streams.
        Raises:
            ArchiveFetcherException
        """

        try:
            #fetch archive streams
            archive_streams = []
            call_sids = self._get_call_sids(chat_session)
            for call_sid in call_sids:
                audio_filename = "%s-%s.mp3" % (output_filename, call_sid)
                self._fetch_recording(call_sid, audio_filename)
                stream = ArchiveStream(
                        filename=audio_filename,
                        type=ArchiveStreamType.USERS_AUDIO_STREAM,
                        length=None,
                        users=[],
                        offset=0)
                archive_streams.append(stream)
            archive_streams.sort(key=lambda stream: stream.offset)

            return ArchiveStreamManifest(
                    archive_streams=archive_streams)

        except ArchiveFetcherException as error:
            raise
        except Exception as error:
            self.log.exception(error)
            raise ArchiveFetcherException(str(error))
    
    def delete(self, chat_id, chat_session):
        """Delete Tokbox media streams.

        Args:
            chat_id: chat id
        Raises:
        ArchiveFetcherException
        """

        try:
            call_sids = self._get_call_sids(chat_session)
            for call_sid in call_sids:
                recording = self._get_recording(call_sid)
                if recording is None:
                    continue

                self.log.info("Deleting recording %s for call %s" %
                        (recording.sid, recording.call_sid))
                recording.delete_instance()

        except Exception as error:
            self.log.exception(error)
            raise ArchiveFetcherException(str(error))
