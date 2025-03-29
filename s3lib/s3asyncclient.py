import aioboto3
from contextlib import asynccontextmanager
from asyncio import Lock


class AsyncS3Client:
    def __init__(
            self,
            *,
            access_key: str,
            secret_key: str,
            endpoint_url: str,
            bucket_name: str,
    ):
        self.config = {
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
            "endpoint_url": endpoint_url
        }

        self._validate_str_param(
            param=bucket_name,
            param_name='bucket_name'
        )  # validate bucket_name first, then create client

        self._bucket_name = bucket_name
        self.session = aioboto3.Session()
        self.lock = Lock()  # Mutex for switching buckets in progress

    @asynccontextmanager
    async def get_client(self):
        async with self.session.client("s3", **self.config) as client:
            yield client

    @staticmethod
    def _validate_str_param(*, param: str, param_name: str) -> None:
        """
        Ensures given str has type str and non-empty. Otherwise, raise corresponding error
        """
        if not isinstance(param, str):
            raise TypeError(f"Parameter '{param_name}' must be string, not {type(param)}")
        if not param.strip():
            raise ValueError(f"Parameter '{param_name}' must be non-empty string")

    @property
    def bucket_name(self) -> str:
        return self._bucket_name

    async def download_entire_file(self, object_key: str, local_file: str) -> None:
        """
        Download file to the current working directory
        """
        if not isinstance(object_key, str) or not isinstance(local_file, str):
            raise TypeError(f"Parameters 'object_key' and 'local_file' must be string, "
                            f"got {type(object_key)} and {type(local_file)}")
        if not object_key.strip() or not local_file.strip():
            raise ValueError(f"Parameter 'object_key' and 'local_file' must be non-empty string, "
                             f"got '{object_key}' and '{local_file}'")
        async with self.get_client() as s3:
            await s3.download_file(self.bucket_name, object_key, local_file)

    async def generate_download_object_url(self, object_key: str) -> str:
        """
        Returns an url link for downloading the object
        """
        self._validate_str_param(param=object_key, param_name='object_key')
        async with self.get_client() as s3:
            url = await s3.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket_name, 'Key': object_key},
                ExpiresIn=3600
            )
        return url

    async def get_keys_prefix(self, prefix: str) -> list[str]:
        """
        Returns a list of keys with given prefix
        """
        self._validate_str_param(param=prefix, param_name="prefix")
        async with self.get_client() as s3:
            response = await s3.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
        return [obj["Key"] for obj in response.get("Contents", [])]

    async def get_num_keys_prefix(self, prefix: str) -> int:
        """
        Returns a number of keys with given prefix. This func based on get_keys_prefix()
        """
        keys = await self.get_keys_prefix(prefix)
        return len(keys)

    async def get_object_size(self, object_key: str) -> int:
        """
        Returns a size of object in bytes
        """
        self._validate_str_param(param=object_key, param_name='object_key')
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
        Set currently using bucket
        """
        self._validate_str_param(param=name, param_name='bucket_name')
        async with self.lock:
            self._bucket_name = name

    async def upload_file(self, file_path: str):
        """
        Upload file to the currently using bucket
        """
        self._validate_str_param(param=file_path, param_name='file_path')
        object_name = file_path.split("/")[-1]
        async with self.get_client() as s3:
            await s3.upload_file(file_path, self._bucket_name, object_name)