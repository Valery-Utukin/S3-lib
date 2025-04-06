import boto3
from botocore.exceptions import ClientError
import os


class SyncS3Client:
    """
    Synchronous client for S3 storage.
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
        Initialize synchronous client.

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
        self._validate_str_param(value=bucket_name, value_name="bucket_name")
        self.config = {
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
            "endpoint_url": endpoint_url
        }
        self.bucket_name = bucket_name

        self.client = boto3.client("s3", **self.config)

    @property
    def bucket_name(self) -> str:
        """
        The name of currently using bucket.

        :return: The name of currently using bucket.
        :rtype: str
        """
        return self._bucket_name

    @bucket_name.setter
    def bucket_name(self, name: str) -> None:
        """
        Set current bucket name.

        :param name: The name of bucket.
        :type name: str
        :rtype: None
        :raises TypeError: If 'name' is not str type.
        :raises ValueError: If 'name' is empty string.
        """
        self._validate_str_param(value=name, value_name="bucket_name")
        self._bucket_name = name

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

    def copy_file(
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

        self.client.copy_object(
            CopySource=copy_source,
            Bucket=destination_bucket,
            Key=destination_key
        )

    def copy_file_prefix(
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

        object_prefix_list = self.get_keys_prefix(prefix)
        for obj in object_prefix_list:
            source_list = [value for value in obj.split(".")]  # Separate source_key -> ['text', 'pdf']
            destination_key = destination_prefix + f"{source_list[0]}_copy.{source_list[1]}"
            self.copy_file(
                source_key=obj,
                destination_key=destination_key,
                destination_bucket=destination_bucket
            )

    def download_entire_file(self, *, object_key: str, local_file: str) -> None:
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
        self._validate_str_param(value=object_key, value_name="object_key")
        self._validate_str_param(value=local_file, value_name="local_file")

        self.client.download_file(self.bucket_name, object_key, local_file)

    def generate_download_object_url(self, object_key: str) -> str:
        """
        Returns an url link for downloading the object

        :param object_key: Key of object in S3-storage.
        :type object_key: str
        :return: The url link for downloading the file
        :rtype: str
        :raises TypeError: If 'object_key' is not str type.
        :raises ValueError: If 'object_key' is empty string.
        """
        self._validate_str_param(value=object_key, value_name='object_key')
        url = self.client.generate_presigned_url(
            'get_object',
            Params={'Bucket': self.bucket_name, 'Key': object_key},
            ExpiresIn=3600
        )
        return url

    def get_keys_prefix(self, prefix: str = "") -> list[str]:
        """
        Returns a list of keys with given prefix. If prefix not given returns all keys.

        :param prefix: Prefix to search over objects. Empty string by default ("").
        :type prefix: str
        :return: List of keys with specified prefix.
        :rtype: list[str]
        :raises TypeError: If given prefix is not str type.
        """
        if prefix != "":
            self._validate_str_param(value=prefix, value_name='prefix')
        keys = []
        response = self.client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix, MaxKeys=100)
        while response.get("Contents", []):
            keys += [obj["Key"] for obj in response.get("Contents", [])]
            response = self.client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                StartAfter=keys[-1],
                MaxKeys=100,
            )
        return keys

    def get_num_keys_prefix(self, prefix: str) -> int:
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
        return len(self.get_keys_prefix(prefix=prefix))

    def get_object_size(self, object_key: str) -> int:
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
        metadata = self.client.get_object_attributes(
            Bucket=self.bucket_name,
            Key=object_key,
            ObjectAttributes=['ObjectSize']
        )
        object_size = metadata.get('ObjectSize', 0)
        return object_size

    def is_object_exists(self, object_key: str) -> bool:
        """
        Checks if object exists in the current bucket.

        :param object_key: Key of object in S3-storage.
        :type object_key: str
        :return: True if object exists in current bucket. False if it doesn't.
        :rtype: bool
        :raises TypeError: If 'object_key' is not str type.
        :raises ValueError: If 'object_key' is empty string.
        """
        self._validate_str_param(value=object_key, value_name='object_key')
        object_keys = self.get_keys_prefix()
        return True if object_key in object_keys else False

    def upload_file(
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
        :raises TypeError: If 'file_path' or 'object_key' is not str type.
        :raises ValueError: If 'file_path' or 'object_key' is empty string.
        """
        self._validate_str_param(value=file_path, value_name='file_path')
        if object_key is None:
            object_key = file_path.split("/")[-1]
        else:
            self._validate_str_param(value=object_key, value_name='object_key')
        self.client.upload_file(file_path, self.bucket_name, object_key)

    def upload_file_multipart(
            self,
            *,
            file_path: str,
            object_key: str = None,
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
            self.upload_file(file_path=file_path, object_key=object_key)
            return None
        try:
            if object_key is None:
                object_key = file_path.split("/")[-1]
            else:
                self._validate_str_param(value=object_key, value_name='object_key')
            res = self.client.create_multipart_upload(Bucket=self.bucket_name, Key=object_key)
            upload_id = res['UploadId']

            # Calculate optimal part size in bytes
            # 10 000 - maximum amount of parts per upload
            part_size = max(min_part_size, file_size // 10_000)

            parts = []
            part_number = 1
            with open(file_path, 'rb') as f:
                while part := f.read(part_size):
                    part_response = self.client.upload_part(
                        Bucket=self.bucket_name,
                        Key=object_key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=part
                    )
                    parts.append({'PartNumber': part_number, 'ETag': part_response['ETag']})
                    part_number += 1

            self.client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=object_key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
            print(f"File {object_key} multipart uploading finished!")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == "EntityTooSmall":
                print("Somehow part_size < 5 MB")
            else:
                print(f"Unknown error: {e.response['Error']['Message']}")
            self.client.abort_multipart_upload(
                Bucket=self.bucket_name,
                Key=object_key,
                UploadId=upload_id
            )
