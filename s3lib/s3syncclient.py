import boto3


class SyncS3Client:
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
        self.bucket_name = bucket_name

        self.client = boto3.client("s3", **self.config)

    @property
    def bucket_name(self) -> str:
        """The name of currently using bucket"""
        return self._bucket_name

    @bucket_name.setter
    def bucket_name(self, name: str) -> None:
        if not isinstance(name, str):
            raise TypeError(f"Parameter 'bucket_name' must be string, not {type(name)}")
        if not name.strip():
            raise ValueError(f"Parameter 'bucket_name' must be not empty string")
        self._bucket_name = name

    def download_entire_file(self, *, object_key: str, local_file: str) -> None:
        """
        Download file to the current working directory
        """
        if isinstance(object_key, str) and isinstance(local_file, str):
            self.client.download_file(self.bucket_name, object_key, local_file)
        else:
            raise TypeError(f"Parameters 'object_key' and 'local_file' must be string, "
                            f"got {type(object_key)} and {type(local_file)}")

    def generate_download_object_url(self, object_key: str) -> str:
        """
        Returns an url link for downloading the object
        """
        if isinstance(object_key, str):
            url = self.client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket_name, 'Key': object_key},
                ExpiresIn=3600
            )
            return url
        else:
            raise TypeError(f"Parameter 'object_key' must be string, not {type(object_key)}")

    def get_keys_prefix(self, prefix: str = "") -> list[str]:
        """
        Returns a list of keys with given prefix. If prefix not given returns all keys up to 1000 objs.
        """
        if isinstance(prefix, str):
            response = self.client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            return [obj["Key"] for obj in response.get("Contents", [])]
        else:
            raise TypeError(f"Parameter 'prefix' must be string, not {type(prefix)}")

    def get_num_keys_prefix(self, prefix: str) -> int:
        """
        Returns a number of keys with given prefix.
        """
        if isinstance(prefix, str):
            return len(self.get_keys_prefix(prefix=prefix))
        else:
            raise TypeError(f"Parameter 'prefix' must be string, not {type(prefix)}")

    def get_file_size(self, object_key: str) -> int:
        """
        Returns a size of object in bytes
        """
        if isinstance(object_key, str):
            metadata = self.client.get_object_attributes(
                Bucket=self.bucket_name,
                Key=object_key,
                ObjectAttributes=['ObjectSize']
            )
            object_size = metadata.get('ObjectSize', 0)
            return object_size
        else:
            raise TypeError(f"Parameter 'object_key' must be string, not {type(object_key)}")

    def upload_file(self, file_path: str) -> None:
        """
        Upload file to the currently using bucket
        """
        object_name = file_path.split("/")[-1]

        self.client.upload_file(file_path, self.bucket_name, object_name)
