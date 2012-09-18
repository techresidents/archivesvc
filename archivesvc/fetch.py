import abc
import logging
import urllib2
from xml.dom.minidom import parseString

import OpenTokSDK

from trpycore.thrift.serialization import deserialize
from trsvcscore.db.models import ChatSession, ChatMessage, ChatMessageFormatType
from trchatsvc.gen.ttypes import Message, MessageType, MarkerType
from stream import ArchiveStreamManifest, ArchiveStream, ArchiveStreamType

class ArchiveFetcher(object):
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractmethod
    def fetch(self, chat_session_id, output_filename):
        return

class TokboxFetcher(ArchiveFetcher):

    def __init__(self,
            db_session_factory,
            storage_pool,
            tokbox_api_key,
            tokbox_api_secret,
            tokbox_url=None):
        self.db_session_factory = db_session_factory
        self.tokbox_api_key = tokbox_api_key
        self.tokbox_api_secret = tokbox_api_secret
        self.tokbox_url = tokbox_url or "https://api.opentok.com"
        self.storage_pool = storage_pool

        #create on demand since require network activity
        self._opentok = None       
    
    @property
    def opentok(self):
        if not self._opentok:
            self._opentok = OpenTokSDK.OpenTokSDK(
                    self.tokbox_api_key,
                    self.tokbox_api_secret)
        return self._opentok

    def _get_chat_session_token(self, chat_session_id):
        try:
            session = self.db_session_factory()
            chat_session = session.query(ChatSession).get(chat_session_id)
            return chat_session.token
        finally:
            if session:
                session.close()
    
    def _get_tokbox_auth_token(self, chat_session_token):
        return self.opentok.generate_token(
                chat_session_token,
                role=OpenTokSDK.RoleConstants.MODERATOR)

    def _get_tokbox_archive_id(self, chat_session_id):
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
            logging.exception(error)
        finally:
            if session:
                session.close()

        return result
    
    def _parse_manifest(self, manifest):
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
        with self.storage_pool.get() as storage_backend:
            if not storage_backend.exists(output_filename):
                url = "%s/hl/archive/getmanifest/%s" \
                        % (self.tokbox_url, archive_id)
                headers = {
                    "X-TB-TOKEN-AUTH": tokbox_auth_token
                }

                request = urllib2.Request(url, headers=headers)
                result = urllib2.urlopen(request)
                storage_backend.save(output_filename, result)

    def _fetch_video(self, archive_id, video_id, tokbox_auth_token, output_filename):
        with self.storage_pool.get() as storage_backend:
            if not storage_backend.exists(output_filename):
                url = "%s/hl/archive/url/%s/%s" \
                        % (self.tokbox_url, archive_id, video_id)
                headers = {

                    "X-TB-TOKEN-AUTH": tokbox_auth_token
                }

                request = urllib2.Request(url, headers=headers)
                result = urllib2.urlopen(request)
                download_url = result.read()
                result = urllib2.urlopen(download_url)
                storage_backend.save(output_filename, result)

    def fetch(self, chat_session_id, output_filename):
        archive_id = self._get_tokbox_archive_id(chat_session_id)
        chat_session_token = self._get_chat_session_token(chat_session_id)
        tokbox_auth_token = self._get_tokbox_auth_token(chat_session_token)
        
        manifest_filename = "%s-%s.manifest" % (output_filename, archive_id)
        self._fetch_manifest(archive_id, tokbox_auth_token, manifest_filename)
        with self.storage_pool.get() as storage_backend:
            manifest = storage_backend.open(manifest_filename, "r").read()
        
        archive_streams = []
        videos = self._parse_manifest(manifest)   
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
