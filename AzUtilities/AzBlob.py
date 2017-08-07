"""
Class for file-like communication with Azure Blobstorage

requirements
azure==2.0.0
azure-batch==3.0.0
azure-common==1.1.7
azure-servicefabric==5.6.130
azure-servicemanagement-legacy==0.20.6
azure-storage==0.34.3
tempfile==
warnings
"""

import tempfile as tmp
from azure.storage.blob import BlockBlobService
from azure.storage.blob import AppendBlobService
import os
import io
import warnings


class AzBlob:
    """Class to communicate with Azure Blob storage as file like object. It works with block and append blobs

    The idea is to work with temporary files in the the local temp folder and upload or download them
    as needed. The temporary file will be deleted each time open(), close() or delete_temp() are called"""

    def __init__(self, account_name, account_key):
        """

        :param account_name: name of the blob account
        :param account_key: key of the blob account
        """
        self.block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)
        self.append_blob_service = AppendBlobService(account_name=account_name, account_key=account_key)
        self.name = None
        self.mode = None
        self.f = None
        self.container = None
        self.blob = None
        # FIXME I do not fully understand + here. Might be redundant for us
        self._read_modes = set(['r', 'rb', 'r+b'])
        self._write_modes = set(['w', 'wb', 'w+b'])
        self._append_modes = set(['a', 'ab', 'a+b'])
        self._appendblob_modes = set(['blobat', 'blobab', 'blobap', 'blobas'])

    def _get_temp_path(self):
        tmp_file = tmp.TemporaryFile()
        self.name = tmp_file.name  # tmp.tempdir + '/tmp' + ''.join(np.random.choice(['a', 'v', 'b', 'c', 'm', 'n'], 5))
        tmp_file.close()

    def _open_read_or_append(self):
        self._get_temp_path()
        self.block_blob_service.get_blob_to_path(container_name=self.container,
                                                 blob_name=self.blob,
                                                 file_path=self.name)
        self.f = open(self.name, mode=self.mode)

    def _open_write_mode(self):
        files_set = self.list_files(self.container, do_print=False, as_set=True)
        if self.blob in files_set:
            warnings.warn('The blob already exist in the container. Will be overwritten if you close. '
                          'Run delete_temp() to delete the temp file without uploading')
            # answer = input('The blob already exist in the container. Would you like to overwrite it? (y/N)', end='')
            # answer = sys.stdin.read()
            # print('')
            # if answer != 'y':
            #     return
        self.name = tmp.gettempdir() + '/' + self.blob
        self.f = open(self.name, self.mode)

    def _open_append_blob(self, container, blob):
        self._append_container = container
        self._append_blob = blob

    def open(self, container, blob, mode='r'):
        """
        Open a file like object and return it if it was opened in read mode. The ideal was not to return that and
        only work with the class. But it did not work with pd.read_csv and json.load. So the output of them should
        be feed to these functions instead. **remeber to close the class not the output of this function** if you
        do the temporary file would remain on the system.

        :param container:
        :param blob:
        :param mode: str
            Will feed to open()
        :return:
        """
        if self.f is not None:
            self.close()
        if mode == 'bloba':
            mode = 'blobat'

        self.container = container
        self.blob = blob
        self.mode = mode

        if mode in self._read_modes:
            self._open_read_or_append()
            return self.f

        elif mode in self._append_modes:
            self._open_read_or_append()

        elif mode in self._write_modes:
            self._open_write_mode()
        elif mode in self._appendblob_modes:
            self._open_append_blob(container=container, blob=blob)

    def _read_append_blob(self, **kwargs):
        if self.mode == 'blobat':
            return self.append_blob_service.get_blob_to_text(self._append_container,
                                                             self._append_blob,
                                                             **kwargs).content

        if self.mode == 'blobap':
            return self.append_blob_service.get_blob_to_path(self._append_container,
                                                             self._append_blob,
                                                             **kwargs).content

        if self.mode == 'blobab':
            return self.append_blob_service.get_blob_to_bytes(self._append_container,
                                                              self._append_blob,
                                                              **kwargs).content

        if self.mode == 'blobas':
            return self.append_blob_service.get_blob_to_stream(self._append_container,
                                                               self._append_blob,
                                                               **kwargs).content

    def read(self):
        """
        Read the content of file

        :return:
        """
        if self.mode in self._appendblob_modes:
            return self._read_append_blob()
        else:
            return self.f.read()

    def _write_to_append_blob(self, sth, **kwargs):
        if self.mode == 'blobat':
            self.append_blob_service.append_blob_from_text(self._append_container,
                                                           self._append_blob,
                                                           sth,
                                                           **kwargs)

        if self.mode == 'blobap':
            self.append_blob_service.append_blob_from_path(self._append_container,
                                                           self._append_blob,
                                                           sth,
                                                           **kwargs)

        if self.mode == 'blobab':
            self.append_blob_service.append_blob_from_bytes(self._append_container,
                                                            self._append_blob,
                                                            sth,
                                                            **kwargs)

        if self.mode == 'blobas':
            self.append_blob_service.append_blob_from_stream(self._append_container,
                                                             self._append_blob,
                                                             sth,
                                                             **kwargs)

    def write(self, sth, **kwargs):
        """
        Write sth to file

        :param sth:
        """
        assert self.mode in self._write_modes.union(self._append_modes.union(self._appendblob_modes)),\
            'File is not writable'
        if self.mode in self._appendblob_modes:
            self._write_to_append_blob(sth, **kwargs)
        else:
            self.f.write(sth)

    # FIXME implement secure delete using srm
    def delete_temp(self, name):
        """
        Delete the temp file

        :param name:
        """
        os.remove(name)

    def close(self, **kwargs):
        """
        Close the file-like object. Upload the file to bob storage if it was opened in write mode and delete the
        temporary file

        :param kwargs:
        """
        self.f.close()
        if self.mode in self._write_modes.union(self._append_modes):
            self.upload(self.container,
                        self.blob,
                        self.name, **kwargs)

        self.f = None
        self.delete_temp(self.name)
        self.name = None
        self.container = None
        self.blob = None

    def list_files(self, container, do_print=True, as_set=False, as_list=False):
        """
        List the blob inside the container as generator, set or list

        :param container:
        :param do_print:
        :param as_set:
        :param as_list:
        :return:
        """
        generator = self.block_blob_service.list_blobs(container)

        if do_print:
            for blob in generator:
                print(blob.name)

        if as_set:
            s = set()
            for blob in generator:
                s.add(blob.name)
                return s

        if as_list:
            l = []
            for blob in generator:
                l.append(blob.name)
                return l

        return generator

    def upload(self, container, blob, file, **kwargs):
        """
        Upload to blob storage

        :param container:
        :param blob:
        :param file:
        :param kwargs:
        """
        if isinstance(file, str):
            self.block_blob_service.create_blob_from_path(container_name=container,
                                                          blob_name=blob,
                                                          file_path=file, **kwargs)
        elif isinstance(file, io.IOBase):
            self.block_blob_service.create_blob_from_stream(container_name=container,
                                                            blob_name=blob,
                                                            stream=file, **kwargs)

    def create_container(self, container, **kwargs):
        self.block_blob_service.create_container(container, **kwargs)

    def delete_blob(self,  container, blob):
        self.block_blob_service.delete_blob(container, blob)

    def create_append_blob(self, container, blob):
        self.append_blob_service.create_blob(container_name=container,
                                             blob_name=blob)
