"""
Local replacement for CaseHarvester's AWS-dependent config.py
Replaces:
  - S3  -> MinIO (S3-compatible, via boto3 pointing to localhost)
  - SQS -> Redis-backed queue with SQS-compatible interface
  - SNS/Lambda/DynamoDB/CloudWatch -> no-ops or local equivalents
"""

from sqlalchemy import create_engine
import os
import logging
import json
import time
import uuid

logger = logging.getLogger('mjcs')

# ---------------------------------------------------------------------------
# Redis-backed SQS-compatible Queue
# ---------------------------------------------------------------------------

class RedisMessage:
    def __init__(self, body, queue_key, redis_client, receipt_handle):
        self.body = body
        self._queue_key = queue_key
        self._redis = redis_client
        self.receipt_handle = receipt_handle

    def delete(self):
        # Already popped from Redis on receive; nothing to do
        pass


class RedisQueue:
    """SQS-compatible queue backed by Redis lists."""

    def __init__(self, redis_client, queue_name):
        self._redis = redis_client
        self._key = f"queue:{queue_name}"
        self._inflight_key = f"queue:{queue_name}:inflight"

    def send_messages(self, Entries):
        pipe = self._redis.pipeline()
        for entry in Entries:
            pipe.rpush(self._key, entry['MessageBody'])
        pipe.execute()

    def receive_messages(self, WaitTimeSeconds=5, MaxNumberOfMessages=10):
        messages = []
        # Try to get messages; if empty, wait up to WaitTimeSeconds
        deadline = time.time() + WaitTimeSeconds
        while time.time() < deadline:
            pipe = self._redis.pipeline()
            for _ in range(MaxNumberOfMessages):
                pipe.lpop(self._key)
            results = pipe.execute()
            for raw in results:
                if raw is not None:
                    body = raw.decode('utf-8') if isinstance(raw, bytes) else raw
                    receipt = str(uuid.uuid4())
                    messages.append(RedisMessage(body, self._key, self._redis, receipt))
            if messages:
                break
            time.sleep(0.5)
        return messages

    def delete_messages(self, Entries):
        # Messages are already popped, nothing to do
        pass

    def load(self):
        pass  # For get_queue_count compatibility

    @property
    def attributes(self):
        count = self._redis.llen(self._key)
        return {'ApproximateNumberOfMessages': str(count)}


# ---------------------------------------------------------------------------
# MinIO S3-compatible Bucket wrapper
# ---------------------------------------------------------------------------

class S3ObjectVersion:
    def __init__(self, s3_resource, bucket_name, key, version_id):
        self._s3 = s3_resource
        self._bucket_name = bucket_name
        self._key = key
        self._version_id = version_id

    def delete(self):
        try:
            self._s3.meta.client.delete_object(
                Bucket=self._bucket_name,
                Key=self._key,
                VersionId=self._version_id
            )
        except Exception as e:
            logger.warning(f"Failed to delete S3 object version: {e}")


class S3Object:
    def __init__(self, s3_resource, bucket_name, key):
        self._s3 = s3_resource
        self._bucket_name = bucket_name
        self._key = key

    def get(self):
        return self._s3.meta.client.get_object(Bucket=self._bucket_name, Key=self._key)

    def Version(self, version_id):
        return S3ObjectVersion(self._s3, self._bucket_name, self._key, version_id)


class S3BucketWrapper:
    def __init__(self, s3_resource, bucket_name):
        self._s3 = s3_resource
        self._bucket_name = bucket_name

    def put_object(self, Body, Key, Metadata=None):
        kwargs = {'Bucket': self._bucket_name, 'Key': Key, 'Body': Body}
        if Metadata:
            kwargs['Metadata'] = Metadata
        response = self._s3.meta.client.put_object(**kwargs)

        class PutResult:
            def __init__(self, resp):
                self._resp = resp
            @property
            def version_id(self):
                return self._resp.get('VersionId', Key)  # fallback to key if versioning disabled

        return PutResult(response)

    def Object(self, key):
        return S3Object(self._s3, self._bucket_name, key)


# ---------------------------------------------------------------------------
# No-op SNS Topic
# ---------------------------------------------------------------------------

class NoOpTopic:
    def publish(self, **kwargs):
        logger.debug(f"[NoOp SNS] publish called with {kwargs}")


# ---------------------------------------------------------------------------
# No-op Lambda client
# ---------------------------------------------------------------------------

class NoOpLambda:
    def invoke(self, **kwargs):
        logger.debug(f"[NoOp Lambda] invoke called with {kwargs}")
        return {}


# ---------------------------------------------------------------------------
# No-op CloudWatch client
# ---------------------------------------------------------------------------

class NoOpCloudWatch:
    def put_metric_data(self, **kwargs):
        logger.debug("[NoOp CloudWatch] put_metric_data called")


# ---------------------------------------------------------------------------
# No-op boto3 session shim
# ---------------------------------------------------------------------------

class LocalBoto3Session:
    def __init__(self, s3_resource, redis_client):
        self._s3 = s3_resource
        self._redis = redis_client
        self._cw = NoOpCloudWatch()

    def client(self, service_name, **kwargs):
        if service_name == 'cloudwatch':
            return self._cw
        if service_name == 'logs':
            return NoOpCloudWatch()
        logger.warning(f"[LocalBoto3Session] Unhandled client service: {service_name}")
        return NoOpCloudWatch()

    def resource(self, service_name, **kwargs):
        logger.warning(f"[LocalBoto3Session] Unhandled resource: {service_name}")
        return None


# ---------------------------------------------------------------------------
# Main Config
# ---------------------------------------------------------------------------

class Config:
    def __getattr__(self, name):
        if self.__getattribute__('initialized') == False:
            raise Exception('Tried to access configuration value before initialization')
        return self.__getattribute__(name)

    def __init__(self):
        self.initialized = False
        self.aws_profile = None
        self.environment = None
        if os.getenv('AWS_LAMBDA_FUNCTION_NAME'):
            self.initialize_from_environment()

    def initialize_from_environment(self, environment=None, aws_profile=None):
        if aws_profile and not self.__getattribute__('aws_profile'):
            self.aws_profile = aws_profile

        # Set up logging
        log = logging.getLogger('mjcs')
        formatter = logging.Formatter('[%(asctime)s] %(levelname)s:%(name)s: %(message)s')
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        if not log.handlers:
            log.addHandler(handler)
        if not log.level:
            log.setLevel(logging.INFO)
        if os.getenv('VERBOSE'):
            log.setLevel(logging.DEBUG)

        if environment:
            self.environment = environment
            from dotenv import load_dotenv
            env_dir = os.path.join(os.path.dirname(__file__), '..', 'env')
            load_dotenv(dotenv_path=os.path.join(env_dir, 'base.env'))
            if environment in ('dev', 'development'):
                load_dotenv(dotenv_path=os.path.join(env_dir, 'development.env'))
            elif environment in ('prod', 'production'):
                load_dotenv(dotenv_path=os.path.join(env_dir, 'production.env'))
            else:
                raise Exception('Invalid environment %s' % environment)

        # General options
        self.MJCS_DOMAIN = os.getenv('MJCS_DOMAIN', 'casesearch.courts.state.md.us')
        self.MJCS_SITE = os.getenv('MJCS_SITE', f'https://{self.MJCS_DOMAIN}')
        self.MJCS_BASE_URL = os.getenv('MJCS_BASE_URL', f'{self.MJCS_SITE}/casesearch')
        self.CASE_BATCH_SIZE = int(os.getenv('CASE_BATCH_SIZE', 1000))
        self.QUERY_TIMEOUT = int(os.getenv('QUERY_TIMEOUT', 135))
        self.QUEUE_WAIT = int(os.getenv('QUEUE_WAIT', 5))
        self.AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        self.CLOUDWATCH_RETENTION_DAYS = os.getenv('CLOUDWATCH_RETENTION_DAYS', 30)

        # Spider options
        self.SPIDER_DAYS_PER_QUERY = int(os.getenv('SPIDER_DAYS_PER_QUERY', 16))

        # Scraper options
        self.MAX_SCRAPE_AGE = int(os.getenv('MAX_SCRAPE_AGE', 14))
        self.MAX_SCRAPE_AGE_INACTIVE = int(os.getenv('MAX_SCRAPE_AGE_INACTIVE', 90))
        self.RESCRAPE_COEFFICIENT = float(os.getenv('RESCRAPE_COEFFICIENT', self.MAX_SCRAPE_AGE / (365 * 4 + 1)))
        self.SCRAPE_QUEUE_THRESHOLD = int(os.getenv('SCRAPE_QUEUE_THRESHOLD', 5000000))

        # Infrastructure identifiers
        self.MJCS_DATABASE_URL = os.getenv('MJCS_DATABASE_URL')
        self.CASE_DETAILS_BUCKET = os.getenv('CASE_DETAILS_BUCKET', 'mjcs-case-details')
        self.SPIDER_QUEUE_NAME = os.getenv('SPIDER_QUEUE_NAME', 'spider-queue')
        self.SCRAPER_QUEUE_NAME = os.getenv('SCRAPER_QUEUE_NAME', 'scraper-queue')
        self.PARSER_FAILED_QUEUE_NAME = os.getenv('PARSER_FAILED_QUEUE_NAME', 'parser-failed-queue')
        self.PARSER_QUEUE_NAME = os.getenv('PARSER_QUEUE_NAME', 'parser-queue')
        self.PARSER_TRIGGER_ARN = os.getenv('PARSER_TRIGGER_ARN', '')
        self.VPC_SUBNET_1_ID = os.getenv('VPC_SUBNET_1_ID', '')
        self.VPC_SUBNET_2_ID = os.getenv('VPC_SUBNET_2_ID', '')
        self.ECS_CLUSTER_ARN = os.getenv('ECS_CLUSTER_ARN', '')

        # SQLAlchemy database engine
        if self.MJCS_DATABASE_URL:
            self.db_engine = create_engine(self.MJCS_DATABASE_URL, future=True)

        # Redis for queues
        import redis as redis_lib
        redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
        self._redis_client = redis_lib.from_url(redis_url)

        # MinIO/S3-compatible storage via boto3
        import boto3 as real_boto3
        minio_endpoint = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
        minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
        minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
        self._s3_resource = real_boto3.resource(
            's3',
            endpoint_url=minio_endpoint,
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
            region_name='us-east-1'
        )
        self.s3 = self._s3_resource

        # Ensure bucket exists
        try:
            self._s3_resource.meta.client.head_bucket(Bucket=self.CASE_DETAILS_BUCKET)
        except Exception:
            try:
                self._s3_resource.create_bucket(Bucket=self.CASE_DETAILS_BUCKET)
                logger.info(f"Created MinIO bucket: {self.CASE_DETAILS_BUCKET}")
            except Exception as e:
                logger.warning(f"Could not create bucket {self.CASE_DETAILS_BUCKET}: {e}")

        # boto3 session shim
        self.boto3_session = LocalBoto3Session(self._s3_resource, self._redis_client)

        # No-ops for unused AWS services
        self.dynamodb = None
        self.sns = type('sns', (), {'Topic': lambda self, arn: NoOpTopic()})()
        self.lambda_ = NoOpLambda()

        self.initialized = True

    @property
    def case_details_bucket(self):
        return S3BucketWrapper(self._s3_resource, self.CASE_DETAILS_BUCKET)

    @property
    def spider_queue(self):
        return RedisQueue(self._redis_client, self.SPIDER_QUEUE_NAME)

    @property
    def scraper_queue(self):
        return RedisQueue(self._redis_client, self.SCRAPER_QUEUE_NAME)

    @property
    def parser_trigger(self):
        return NoOpTopic()

    @property
    def parser_failed_queue(self):
        return RedisQueue(self._redis_client, self.PARSER_FAILED_QUEUE_NAME)

    @property
    def parser_queue(self):
        return RedisQueue(self._redis_client, self.PARSER_QUEUE_NAME)


config = Config()