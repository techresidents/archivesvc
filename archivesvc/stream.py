
class ArchiveStreamManifest(object):
    """Archive steam manifest."""
    def __init__(self, filename, archive_streams):
        self.filename = filename
        self.archive_streams = archive_streams
    
    def __repr__(self):
        return "%s(filename=%r)" % \
            (self.__class__.__name__, self.filename)

class ArchiveStreamType(object):
    """Archive steam type."""
    USER_VIDEO_STREAM = "USER_VIDEO_STREAM"
    USER_AUDIO_STREAM = "USER_AUDIO_STREAM"
    STITCHED_AUDIO_STREAM = "STITCHED_AUDIO_STREAM"

class ArchiveStream(object):
    """Archive stream."""
    def __init__(self,
            filename,
            type,
            length,
            users=None,
            offset=0):
        self.filename = filename
        self.type = type
        self.length = length
        self.users = users or []
        self.offset = offset
    
    def __repr__(self):
        return "%s(filename=%r, type=%r, length=%r, offset=%r)" % (\
                self.__class__.__name__,
                self.filename, self.type, self.length, self.offset)
