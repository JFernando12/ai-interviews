"""
AWS Service Clients Module
Manages connections to AWS services used in the video processing pipeline
"""
import boto3
import logging
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
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
    def comprehend_client(self):
        """Lazy-loaded Comprehend client"""
        if self._comprehend_client is None:
            try:
                self._comprehend_client = boto3.client(
                    'comprehend',
                    aws_access_key_id=self.config.ACCESS_KEY_ID,
                    aws_secret_access_key=self.config.SECRET_ACCESS_KEY,
                    aws_session_token=self.config.SESSION_TOKEN,
                    region_name=self.config.REGION
                )
                logger.info("Comprehend client initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Comprehend client: {str(e)}")
                raise
        return self._comprehend_client
    
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
    
    def test_connections(self):
        """Test connections to all AWS services"""
        results = {}
        
        # Test S3
        try:
            self.s3_client.head_bucket(Bucket=self.config.S3_BUCKET)
            results['s3'] = {'status': 'success', 'message': 'S3 bucket accessible'}
        except ClientError as e:
            results['s3'] = {'status': 'error', 'message': f'S3 error: {str(e)}'}
        except Exception as e:
            results['s3'] = {'status': 'error', 'message': f'S3 connection failed: {str(e)}'}
        
        # Test Transcribe
        try:
            self.transcribe_client.list_transcription_jobs(MaxResults=1)
            results['transcribe'] = {'status': 'success', 'message': 'Transcribe service accessible'}
        except Exception as e:
            results['transcribe'] = {'status': 'error', 'message': f'Transcribe connection failed: {str(e)}'}
        
        # Test Comprehend
        try:
            self.comprehend_client.detect_dominant_language(Text="test")
            results['comprehend'] = {'status': 'success', 'message': 'Comprehend service accessible'}
        except Exception as e:
            results['comprehend'] = {'status': 'error', 'message': f'Comprehend connection failed: {str(e)}'}
        
        # Test Bedrock (optional)
        if self._bedrock_client:
            try:
                # This is a simple test - in practice, you'd need to specify a model
                results['bedrock'] = {'status': 'available', 'message': 'Bedrock client initialized'}
            except Exception as e:
                results['bedrock'] = {'status': 'error', 'message': f'Bedrock connection failed: {str(e)}'}
        else:
            results['bedrock'] = {'status': 'unavailable', 'message': 'Bedrock client not initialized'}
        
        return results