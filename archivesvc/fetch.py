import abc
import logging
import urllib2
from xml.dom.minidom import parseString

import OpenTokSDK

from trpycore.thrift.serialization import deserialize
from trsvcscore.db.models import ChatSession, ChatMessage, ChatMessageFormatType
from trchatsvc.gen.ttypes import Message, MessageType, MarkerType
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
    def fetch(self, chat_session_id, output_filename):
        """Fetch media streams for the specified chat session id.

        Args:
            chat_session_id: chat session id
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
    def delete(self, chat_session_id):
        """Delete media streams from video chat vendor.

        Args:
            chat_session_id: chat session id
        Raises:
        ArchiveFetcherException
        """
        return


class TokboxFetcher(ArchiveFetcher):
    """Tokbox archive fetcher.

    Fetches (downloads) individual video chat streams from Tokbox.
    """

    def __init__(self,
            db_session_factory,
            storage_pool,
            tokbox_api_key,
            tokbox_api_secret,
            tokbox_url=None):
        """Tokbox fetcher constructor.

        Args:
            db_session_factory: callable return a sqlaclhemy Session object
            storage_pool: Pool object of Storage objects to use to store
                media streams.
            tokbox_api_key: Tokbox api key
            tokbox_api_secret: Tokbox api secret
            tokbox_url: optional Tokbox url
        """
        self.db_session_factory = db_session_factory
        self.tokbox_api_key = tokbox_api_key
        self.tokbox_api_secret = tokbox_api_secret
        self.tokbox_url = tokbox_url or "https://api.opentok.com"
        self.storage_pool = storage_pool
        self.log = logging.getLogger("%s.%s" \
                % (__name__, self.__class__.__name__))

        #create on demand since require network activity
        self._opentok = None       
    
    @property
    def opentok(self):
        """Return OpenTokSDK object.

        Returns:
            OpenTokSDK object
        """
        if not self._opentok:
            self._opentok = OpenTokSDK.OpenTokSDK(
                    self.tokbox_api_key,
                    self.tokbox_api_secret)
        return self._opentok

    def _get_chat_session_token(self, chat_session_id):
        """Get Tokbox chat session token for specified chat session id.
        
        Args:
            chat_session_id: chat session id
        Returns:
            Tokbox chat session token string for specified chat session id.
        """
        try:
            session = self.db_session_factory()
            chat_session = session.query(ChatSession).get(chat_session_id)
            return chat_session.token
        finally:
            if session:
                session.close()
    
    def _get_tokbox_auth_token(self, chat_session_token):
        """Get Tokbox moderator auth token for Tokbox chat session.
        
        Tokbox moderator auth token is required for fetch operations.

        Args:
            chat_session_token: Tokbox chat session token string
        Returns:
            Tokbox moderator auth token string
        """
        return self.opentok.generate_token(
                chat_session_token,
                role=OpenTokSDK.RoleConstants.MODERATOR)

    def _get_tokbox_archive_id(self, chat_session_id):
        """Get Tokbox archive id for specified chat session id.
        
        The tokbox archive id is the primary key for fetch
        related operations.

        Args:
            chat_session_id: chat session id
        Returns:
            Tokbox archive id string
        """
        result = None

        try:
            session = self.db_session_factory()
            messages = session.query(ChatMessage)\
                    .join(ChatMessage.format_type)\
                    .filter(ChatMessage.chat_session_id == chat_session_id)\
                    .filter(ChatMessageFormatType.name == "THRIFT_BINARY_B64")\
                    .order_by(ChatMessage.timestamp)\
                    .all()
            
            for message in messages:
                msg = deserialize(Message(), message.data)
                if msg.header.type == MessageType.MARKER_CREATE:
                    marker_msg = msg.markerCreateMessage
                    if marker_msg.marker.type == MarkerType.RECORDING_STARTED_MARKER:
                        marker = marker_msg.marker.recordingStartedMarker
                        result = marker.archiveId
                        break

        except Exception as error:
            self.log.exception(error)
        finally:
            if session:
                session.close()

        return result
    
    def _parse_manifest(self, manifest):
        """Parse Tokbox manifest file.

        Args:
            manifest: contents of Tokbox manifest file.
        Returns:
            dict of {video_id: video_dict} where video_dict
            is a dict containing the video's attributes.    
        """
        videos = {}
        dom = parseString(manifest)
        video_elements = dom.getElementsByTagName("video")
        for video in video_elements:
            id = video.getAttribute("id")
            user_id = video.getAttribute("name")
            users = [user_id] if user_id else []
            videos[id] = {
                "id": id,
                "length": int(video.getAttribute("length")),
                "name": video.getAttribute("name"),
                "users": users
            }
        
        event_elements = dom.getElementsByTagName("event")
        for event in event_elements:
            if event.getAttribute("type") == "PLAY":
                video_id = event.getAttribute("id")
                offset = int(event.getAttribute("offset"))
                videos[video_id]["offset"] = offset
        
        return videos

    def _fetch_manifest(self, archive_id, tokbox_auth_token, output_filename):
        """Fetch Tokbox manifest file.
        
        Fetches the Tokbox manifest file and stores it in self.storage_pool
        as output_filename.

        Args:
            archive_id: Tokbox archive id string
            tokbox_auth_token: Tokbox moderator auth token string
            output_filename: output filename
        Raises:
            urllib2.HTTPError, StorageException, ArchiveFetcherException
        """
        with self.storage_pool.get() as storage_backend:
            if not storage_backend.exists(output_filename):
                url = "%s/hl/archive/getmanifest/%s" \
                        % (self.tokbox_url, archive_id)
                headers = {
                    "X-TB-TOKEN-AUTH": tokbox_auth_token
                }
                
                self.log.info("Downloading manifest from %s" % url)
                request = urllib2.Request(url, headers=headers)
                result = urllib2.urlopen(request)
                
                #The following is necessary since Tokbox does not properly return
                #an HTTP 404 for invalid or deleted archive id. Instead they
                #return an HTTP 200 with error elements in the manifest.
                #TODO remove this when Tokbox fixes this issue.
                manifest = result.read()
                dom = parseString(manifest)
                errors = dom.getElementsByTagName("error")
                if errors:
                    raise ArchiveFetcherException("manifest error (%s): '%s'" \
                            % (url, manifest))

                #storage_backend.save(output_filename, result)
                storage_backend.save(output_filename, manifest)

    def _fetch_video(self, archive_id, video_id, tokbox_auth_token, output_filename):
        """Fetch Tokbox video stream file.
        
        Fetches the Tokbox video stream file and stores it in self.storage_pool
        as output_filename.

        Args:
            archive_id: Tokbox archive id string
            video_id: Tokbox video id obtained from manifest
            tokbox_auth_token: Tokbox moderator auth token string
            output_filename: output filename
        Raises:
            urllib2.HTTPError, StorageException
        """
        with self.storage_pool.get() as storage_backend:
            if not storage_backend.exists(output_filename):
                url = "%s/hl/archive/url/%s/%s" \
                        % (self.tokbox_url, archive_id, video_id)
                headers = {

                    "X-TB-TOKEN-AUTH": tokbox_auth_token
                }

                self.log.info("Downloading video from %s" % url)

                request = urllib2.Request(url, headers=headers)
                result = urllib2.urlopen(request)
                download_url = result.read()
                result = urllib2.urlopen(download_url)
                storage_backend.save(output_filename, result)

    def fetch(self, chat_session_id, output_filename):
        """Fetch Tokbox media streams for the specified chat session id.

        Fetches the Tokbox manifest file and  video stream files, storing
        them in self.storage_pool using output_filename as the base
        filename.
        
        Args:
            chat_session_id: chat session id
            output_filename: output base filename to be used
                to construct archive stream filenames.
        Returns:
            ArchiveStreamManifest object containing references
            to all downloaded media streams.
        Raises:
            ArchiveFetcherException
        """

        try:
            archive_id = self._get_tokbox_archive_id(chat_session_id)
            if archive_id is None:
                return None
            
            chat_session_token = self._get_chat_session_token(chat_session_id)
            tokbox_auth_token = self._get_tokbox_auth_token(chat_session_token)
            
            #fetch manifest
            manifest_filename = "%s-%s.manifest" \
                    % (output_filename, archive_id)
            self._fetch_manifest(
                    archive_id,
                    tokbox_auth_token,
                    manifest_filename)
            
            #parse manifest
            with self.storage_pool.get() as storage_backend:
                manifest = storage_backend.open(manifest_filename, "r").read()
            videos = self._parse_manifest(manifest)   
            
            #fetch archive streams
            archive_streams = []
            for video in videos.values():
                video_filename = "%s-%s.flv" % (output_filename, video["id"])
                self._fetch_video(archive_id, video["id"], tokbox_auth_token, video_filename)
                stream = ArchiveStream(
                        filename=video_filename,
                        type=ArchiveStreamType.USER_VIDEO_STREAM,
                        length=video["length"],
                        users=video["users"],
                        offset=video["offset"])
                archive_streams.append(stream)
            archive_streams.sort(key=lambda stream: stream.offset)

            return ArchiveStreamManifest(
                    filename=manifest_filename,
                    archive_streams=archive_streams)

        except ArchiveFetcherException as error:
            raise
        except Exception as error:
            self.log.exception(error)
            raise ArchiveFetcherException(str(error))
    
    def delete(self, chat_session_id):
        """Delete Tokbox media streams.

        Args:
            chat_session_id: chat session id
        Raises:
        ArchiveFetcherException
        """

        try:
            archive_id = self._get_tokbox_archive_id(chat_session_id)
            chat_session_token = self._get_chat_session_token(chat_session_id)
            tokbox_auth_token = self._get_tokbox_auth_token(chat_session_token)

            url = "%s/hl/archive/delete/%s" \
                    % (self.tokbox_url, archive_id)
            headers = {
                "X-TB-TOKEN-AUTH": tokbox_auth_token
            }

            self.log.info("Deleting archive at %s" % url)

            request = urllib2.Request(url, data="", headers=headers)
            urllib2.urlopen(request)

        except Exception as error:
            self.log.exception(error)
            raise ArchiveFetcherException(str(error))
