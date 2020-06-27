import os
from urllib.parse import urlparse
import httpio
import google.auth
import google.auth.transport.requests as tr_requests
from google.resumable_media.requests import Download
from google.cloud import storage
from fnmatch import fnmatchcase

import io


class RemotePath:

    def __init__(self, string):
        parse = urlparse(string)
        self._scheme = parse.scheme
        self._bucket = parse.netloc
        self._parts = parse_parts(parse.path)
    
    @classmethod
    def _from_parts(cls, scheme, bucket, parts):
        self = object.__new__(cls)
        self._scheme = scheme
        self._bucket = bucket
        self._parts = parts
        return self
    
    @property
    def path(self):
        return '/'.join(self._parts)
    
    @property
    def bucket(self):
        return self._bucket

    def join(self, other):
        if isinstance(other, str):
            other = RemotePath(other)
        elif isinstance(other, RemotePath):
            pass
        else:
            raise TypeError('{type(other)} not implemented')

        return type(self)._from_parts(
                self._scheme,
                self._bucket,
                self._parts + other._parts)

    def __div__(self, other):
        return self.join(other)
    
    @property
    def parent(self):
        return type(self)._from_parts(
                self._scheme,
                self._bucket,
                self._parts[:-1])
    
    @property
    def parents(self):
        parent = self.parent
        while parent._parts:
            yield parent
            parent = parent.parent
    
    @property
    def name(self):
        return self._parts[-1]
    
    @property
    def stem(self):
        return self.name.split('.')[0]

    @property
    def suffix(self):
        return '.' + self.name.split('.')[-1]

    @property
    def suffixes(self):
        raise NotImplementedError

    def __repr__(self):
        return f"{type(self).__name__}('{self.__str__()}')"

    def __str__(self):
        return f"{self._scheme}://{self.bucket}/{'/'.join(self._parts)}"


class GCSPath(RemotePath):

    _storage_client = storage.Client() 
    _download_url_template = (
            'https://storage.googleapis.com/download/storage/v1/'
            '{bucket}/{path}?alt=media')

    def glob(self, pattern):
        bucket = self.storage_client.bucket(self.bucket)
        it = bucket.list_blobs(prefix='/'.join(self._parts))
        for blob in it:
            if fnmatchcase(blob.name, pattern):
                yield self._from_parts(
                        'gs',
                        self.bucket,
                        parse_parts(blob.name))

    def open(self, mode='r'):
        # Gross stuff from google.storage python client
        blob = self._storage_client.bucket(self.bucket).blob(self.path)
        client = blob._require_client(blob.client)
        transport = blob._get_transport(client)
        media_url = blob._get_download_url()
        r = Download(media_url).consume(transport)
        return io.BytesIO(r.content)
    
    @property
    def download_url(self):
        return self._download_url_template.format(
                bucket=self.bucket, path=self.path)

    @property
    def public_url(self):
        raise NotImplementedError


def parse_parts(path, sep='/'):
    path.strip(sep)
    return [p for p in path.split(sep) if p and not p == '.']

        

if __name__ == "__main__":
    string = 'gs://za-images/nearmap/bdaf75f298424c5aa4076cc4515e67df_trans_merc-px_2048_zoom-21_zoom-21_orient-v_date-na.tif'
    path = RemotePath(string)
    gcs_path = GCSPath(string)
