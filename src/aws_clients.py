"""
AWS Service Clients Module
Manages connections to AWS services used in the video processing pipeline
"""
import boto3
import logging
from botocore.exceptions import ClientError
from config import AWSConfig

logger = logging.getLogger(__name__)

class AWSServiceClients:
    """Manages AWS service clients with proper error handling"""
    
    def __init__(self):
        self.config = AWSConfig()
        self.config.validate_config()
        
        # Initialize clients as None
        self._s3_client = None
        self._transcribe_client = None
        self._comprehend_client = None
        self._bedrock_client = None
        self._sqs_client = None
        self._dynamodb_client = None
        
    @property
    def s3_client(self):
        """Lazy-loaded S3 client"""
        if self._s3_client is None:
            try:
                self._s3_client = boto3.client(
                    's3',
                    aws_access_key_id=self.config.ACCESS_KEY_ID,
                    aws_secret_access_key=self.config.SECRET_ACCESS_KEY,
                    aws_session_token=self.config.SESSION_TOKEN,
                    region_name=self.config.REGION
                )
                logger.info("S3 client initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize S3 client: {str(e)}")
                raise
        return self._s3_client
    
    @property
    def transcribe_client(self):
        """Lazy-loaded Transcribe client"""
        if self._transcribe_client is None:
            try:
                self._transcribe_client = boto3.client(
                    'transcribe',
                    aws_access_key_id=self.config.ACCESS_KEY_ID,
                    aws_secret_access_key=self.config.SECRET_ACCESS_KEY,
                    aws_session_token=self.config.SESSION_TOKEN,
                    region_name=self.config.REGION
                )
                logger.info("Transcribe client initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Transcribe client: {str(e)}")
                raise
        return self._transcribe_client
    
    @property
    def bedrock_client(self):
        """Lazy-loaded Bedrock client (optional, for advanced AI features)"""
        if self._bedrock_client is None:
            try:
                self._bedrock_client = boto3.client(
                    'bedrock-runtime',
                    aws_access_key_id=self.config.ACCESS_KEY_ID,
                    aws_secret_access_key=self.config.SECRET_ACCESS_KEY,
                    aws_session_token=self.config.SESSION_TOKEN,
                    region_name=self.config.REGION
                )
                logger.info("Bedrock client initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Bedrock client: {str(e)}")
                # Bedrock is optional, so we don't raise here
                logger.warning("Bedrock client unavailable - advanced AI features will be disabled")
        return self._bedrock_client
    
    @property
    def sqs_client(self):
        """Lazy-loaded SQS client"""
        if self._sqs_client is None:
            try:
                self._sqs_client = boto3.client(
                    'sqs',
                    aws_access_key_id=self.config.ACCESS_KEY_ID,
                    aws_secret_access_key=self.config.SECRET_ACCESS_KEY,
                    aws_session_token=self.config.SESSION_TOKEN,
                    region_name=self.config.REGION
                )
                logger.info("SQS client initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize SQS client: {str(e)}")
                raise
        return self._sqs_client
    
    @property
    def dynamodb_client(self):
        """Lazy-loaded DynamoDB client"""
        if self._dynamodb_client is None:
            try:
                self._dynamodb_client = boto3.client(
                    'dynamodb',
                    aws_access_key_id=self.config.ACCESS_KEY_ID,
                    aws_secret_access_key=self.config.SECRET_ACCESS_KEY,
                    aws_session_token=self.config.SESSION_TOKEN,
                    region_name=self.config.REGION
                )
                logger.info("DynamoDB client initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize DynamoDB client: {str(e)}")
                raise
        return self._dynamodb_client