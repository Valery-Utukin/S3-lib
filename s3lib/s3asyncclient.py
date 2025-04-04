import os
import asyncio
import aioboto3
from asyncio import Lock
from aiobotocore.config import AioConfig
from botocore.exceptions import ClientError
from contextlib import asynccontextmanager


class AsyncS3Client:
    """
    Asynchronous client for S3 storage.
    """
    def __init__(
            self,
            *,
            access_key: str,
            secret_key: str,
            endpoint_url: str,
            bucket_name: str,
    ):
        """
        Initialize asynchronous client.

        :param access_key: Access key to S3-storage.
        :type access_key: str
        :param secret_key: Secret access key to S3-storage.
        :type secret_key: str
        :param endpoint_url: An url link to S3 storage.
        :type endpoint_url: str
        :param bucket_name: The name of bucket inside S3-storage.
        :type bucket_name: str
        :raises TypeError: If any of args are not str type.
        :raises ValueError: If any of args are empty strings.
        """
        self._validate_str_param(value=access_key, value_name="access_key")
        self._validate_str_param(value=secret_key, value_name="secret_key")
        self._validate_str_param(value=endpoint_url, value_name="endpoint_url")
        self._validate_str_param(value=bucket_name, value_name='bucket_name')
        self.config = {
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
            "endpoint_url": endpoint_url
        }

        self._bucket_name = bucket_name
        self.session = aioboto3.Session()

        self.lock = Lock()  # Mutex for switching buckets in progress
        self.semaphore = asyncio.Semaphore(5)
        self.s3_config = AioConfig(max_pool_connections=5)

    @asynccontextmanager
    async def get_client(self):
        """
        Yield async context manager for async communications with S3-storage.

        :return: Async S3 Client
        :rtype: aiobotocore.client.AioBaseClient
        """
        async with self.session.client("s3", **self.config, config=self.s3_config) as client:
            yield client

    @staticmethod
    def _validate_str_param(*, value: str, value_name: str) -> None:
        """
        Ensures given str has type str and non-empty. Otherwise, raise corresponding error.

        :param value: String to be checked.
        :type value: str
        :param value_name: The name of string.
        :type value_name: str
        :rtype: None
        :raises TypeError: If 'string' is not str type.
        :raises ValueError: If 'string' is empty string.
        """
        if not isinstance(value, str):
            raise TypeError(f"Parameter '{value_name}' must be string, not {type(value)}")
        if not value.strip():
            raise ValueError(f"Parameter '{value_name}' must be non-empty string")

    @property
    def bucket_name(self) -> str:
        """
        The name of currently using bucket.

        :return: The name of currently using bucket.
        :rtype: str
        """
        return self._bucket_name

    async def copy_file(
            self,
            *,
            source_key: str,
            destination_key: str = None,
            destination_bucket: str = None
    ) -> None:
        """
        Creates a copy of an object.

        :param source_key: Key of object to be copied.
        :type source_key: str
        :param destination_key: Key of object copy. May contain folder prefix - test_folder/object_copy.txt.
                                If not specified uses _copy postfix in object copy name.
        :type destination_key: str | None
        :param destination_bucket: Bucket name to copy to. If not specified uses current bucket.
        :type destination_bucket: str | None
        :raises TypeError: If 'source_key', 'destination_key' or 'destination_bucket' are not str type.
        :raises ValueError: If 'source_key', 'destination_key' or 'destination_bucket' are empty string.
        """
        self._validate_str_param(value=source_key, value_name="source_key")
        if destination_key is None:
            source_list = [value for value in source_key.split(".")]  # Separate source_key -> ['text', 'pdf']
            destination_key = f"{source_list[0]}_copy.{source_list[1]}"
        else:
            self._validate_str_param(value=destination_key, value_name="destination_key")
        if destination_bucket is None:
            destination_bucket = self.bucket_name
        else:
            self._validate_str_param(value=destination_bucket, value_name="destination_bucket")

        copy_source = {
            "Bucket": self._bucket_name,
            "Key": source_key
        }

        async with self.semaphore:
            async with self.get_client() as s3:
                await s3.copy_object(
                    CopySource=copy_source,
                    Bucket=destination_bucket,
                    Key=destination_key
                )

    async def copy_file_prefix(
            self,
            *,
            prefix: str,
            destination_prefix: str = None,
            destination_bucket: str = None
    ) -> None:
        """
        Creates a copy of all objects that begin with specified prefix.

        :param prefix: Prefix to search over objects to be copied.
        :type prefix: str
        :param destination_prefix: Prefix of object copies. Suppose to be a folder name for created copies
                                   like test_folder/object_copy.txt. 'test_folder/' is destination prefix.
                                   Copied objects will have _copy postfix. Must ends with '/'.
                                   Otherwise, raise ValueError. If not specified uses _copy postfix and creates copies
                                   in the root of current or given bucket.
        :type destination_prefix: str | None
        :param destination_bucket: Bucket name to copy to. If not specified uses current bucket.
        :type destination_bucket: str | None
        :raises ValueError: If 'destination_prefix' does not end with backslash '/'.
        """
        self._validate_str_param(value=prefix, value_name="prefix")
        if destination_prefix is None:
            destination_prefix = ""
        else:
            self._validate_str_param(value=destination_prefix, value_name=destination_prefix)
            if not destination_prefix.endswith('/'):
                raise ValueError(f"Parameter 'destination_prefix' must ends with '/': {destination_prefix}")
        if destination_bucket is None:
            destination_bucket = self.bucket_name
        else:
            self._validate_str_param(value=destination_bucket, value_name='destination_bucket')

        object_prefix_list = await self.get_keys_prefix(prefix)

        tasks = []
        for obj in object_prefix_list:
            source_list = [value for value in obj.rsplit(".", 1)]  # Separate source_key -> ['text', 'pdf']
            destination_key = f"{destination_prefix}{source_list[0]}_copy.{source_list[1]}"
            task = asyncio.create_task(
                self.copy_file(
                    source_key=obj,
                    destination_key=destination_key,
                    destination_bucket=destination_bucket
                )
            )
            tasks.append(task)
        await asyncio.gather(*tasks)

    async def download_entire_file(self, object_key: str, local_file: str) -> None:
        """
        Download file to the current working directory.

        :param object_key: Key of object in S3-storage.
        :type object_key: str
        :param local_file: The name of downloaded file.
        :type local_file: str
        :rtype: None
        :raises TypeError: If 'object_key' or 'local_file' are not str type.
        :raises ValueError: If 'object_key' or 'local_file' are empty string.
        """
        self._validate_str_param(value=object_key, value_name='object_key')
        self._validate_str_param(value=local_file, value_name='local_file')
        async with self.get_client() as s3:
            await s3.download_file(self.bucket_name, object_key, local_file)

    async def generate_download_object_url(self, object_key: str) -> str:
        """
        Returns an url link for downloading the object.

        :param object_key: Key of object in S3-storage.
        :type object_key: str
        :return: The url link for downloading the file
        :rtype: str
        :raises TypeError: If 'object_key' is not str type.
        :raises ValueError: If 'object_key' is empty string.
        """
        self._validate_str_param(value=object_key, value_name='object_key')
        async with self.get_client() as s3:
            url = await s3.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket_name, 'Key': object_key},
                ExpiresIn=3600
            )
        return url

    async def get_keys_prefix(self, prefix: str = "") -> list[str]:
        """
        Returns a list of keys with given prefix. If prefix not given returns all keys up to 1000 objs.

        :param prefix: Prefix to search over objects. Empty string by default ("").
        :type prefix: str
        :return: List of keys with specified prefix. List may be empty.
        :rtype: list[str]
        :raises TypeError: If given prefix is not str type.
        """
        if prefix != "":
            self._validate_str_param(value=prefix, value_name='prefix')
        async with self.get_client() as s3:
            response = await s3.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
        return [obj["Key"] for obj in response.get("Contents", [])]

    async def get_num_keys_prefix(self, prefix: str) -> int:
        """
        Returns a number of keys with given prefix.

        :param prefix: Prefix to search over objects.
        :type prefix: str
        :return: The number of objects that begin with specified prefix.
        :rtype: int
        :raises TypeError: If 'prefix' is not str type.
        :raises ValueError: If 'prefix' is empty string.
        """
        self._validate_str_param(value=prefix, value_name='prefix')
        keys = await self.get_keys_prefix(prefix=prefix)
        return len(keys)

    async def get_object_size(self, object_key: str) -> int:
        """
        Returns a size of object in bytes.

        :param object_key: Key of object in S3-storage.
        :type object_key: str
        :return: The size of object in bytes
        :rtype: int
        :raises TypeError: If 'object_key' is not str type.
        :raises ValueError: If 'object_key' is empty string.
        """
        self._validate_str_param(value=object_key, value_name='object_key')
        async with self.get_client() as s3:
            metadata = await s3.get_object_attributes(
                Bucket=self.bucket_name,
                Key=object_key,
                ObjectAttributes=['ObjectSize']
            )
            object_size = metadata.get('ObjectSize', 0)
            return object_size

    async def set_bucket_name(self, name: str) -> None:
        """
        Set current bucket name.

        :param name: The name of bucket
        :type name: str
        :rtype: None
        :raises TypeError: If 'name' is not str type.
        :raises ValueError: If 'name' is empty string.
        """
        self._validate_str_param(value=name, value_name='bucket_name')
        async with self.lock:
            self._bucket_name = name

    async def upload_file(
            self,
            *,
            file_path: str,
            object_key: str = None
    ) -> None:
        """
        Upload file to the current bucket.

        :param file_path: Absolute or local path to uploaded file.
        :type file_path: str
        :param object_key: Key of object in S3-storage.
        :type object_key: str | None
        :rtype: None
        :raises TypeError: If 'file_path' is not str type.
        :raises ValueError: If 'file_path' is empty string.
        """
        self._validate_str_param(value=file_path, value_name='file_path')
        if object_key is None:
            object_key = file_path.split("/")[-1]
        else:
            self._validate_str_param(value=object_key, value_name='object_key')
        async with self.get_client() as s3:
            await s3.upload_file(file_path, self._bucket_name, object_key)

    @staticmethod
    async def _read_file_chunks(file_path: str, part_size: int):
        def read_chunks():
            with open(file_path, "rb") as f:
                while chunk := f.read(part_size):
                    yield chunk

        for chunk in await asyncio.to_thread(lambda: read_chunks()):
            yield chunk

    async def upload_file_multipart(
            self,
            *,
            file_path: str,
            object_key: str = None
    ) -> None:
        """
        Uploads file to the current bucket using multipart upload.
        :param file_path: Absolute or local path to uploaded file.
        :type file_path: str
        :param object_key: Key of object in S3-storage.
        :type object_key: str | None
        :rtype: None
        :raises TypeError: If 'file_path' or 'object_key' is not str type.
        :raises ValueError: If 'file_path' or 'object_key' is empty string.
        """
        min_part_size = 5 * 1024 * 1024  # 5 MB - minimal chunk size (google "Amazon S3 multipart upload limits")
        file_size = os.path.getsize(file_path)
        if file_size < min_part_size:  # If file_size < 5 MB use self.upload_file()
            await self.upload_file(file_path=file_path, object_key=object_key)
            return None
        async with self.get_client() as s3:
            try:
                if object_key is None:
                    object_key = file_path.split("/")[-1]
                else:
                    self._validate_str_param(value=object_key, value_name='object_key')

                res = await s3.create_multipart_upload(Bucket=self.bucket_name, Key=object_key)
                upload_id = res['UploadId']

                # Calculate optimal part size in bytes
                # 10 000 - maximum amount of parts per upload
                part_size = max(min_part_size, file_size // 10_000)

                parts = []
                part_number = 1
                async for chunk in self._read_file_chunks(file_path, part_size):
                    response = await s3.upload_part(
                        Bucket=self.bucket_name,
                        Key=object_key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=chunk
                    )
                    parts.append({"ETag": response["ETag"], "PartNumber": part_number})
                    part_number += 1
                await s3.complete_multipart_upload(
                    Bucket=self.bucket_name,
                    Key=object_key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts},
                )
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == "EntityTooSmall":
                    print("Somehow part_size < 5 MB")
                else:
                    print(f"Unknown error: {e.response['Error']['Message']}")
                await s3.abort_multipart_upload(
                    Bucket=self.bucket_name,
                    Key=object_key,
                    UploadId=upload_id
                )
