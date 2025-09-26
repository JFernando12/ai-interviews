"""
AWS Video Processing Pipeline Configuration
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class AWSConfig:
    """Configuration class for AWS services"""
    
    # AWS Credentials
    ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    SESSION_TOKEN = os.getenv('AWS_SESSION_TOKEN')
    REGION = os.getenv('AWS_REGION', 'us-east-1')
    
    # S3 Configuration
    S3_BUCKET = os.getenv('AWS_S3_BUCKET')
    
    # AWS Service Configuration
    TRANSCRIBE_JOB_PREFIX = 'video-transcription-'
    COMPREHEND_LANGUAGE_CODE = 'en'
    
    # File paths and extensions
    SUPPORTED_VIDEO_FORMATS = ['.mp4', '.avi', '.mov', '.mkv', '.wmv']
    SUPPORTED_AUDIO_FORMATS = ['.wav', '.mp3', '.m4a', '.flac']
    
    # Processing settings
    AUDIO_SAMPLE_RATE = 16000
    AUDIO_CHANNELS = 1
    
    @classmethod
    def validate_config(cls):
        """Validate that required configuration is present"""
        required_vars = [
            'ACCESS_KEY_ID',
            'SECRET_ACCESS_KEY', 
            'S3_BUCKET'
        ]
        
        missing_vars = []
        for var in required_vars:
            if not getattr(cls, var):
                missing_vars.append(var)
        
        if missing_vars:
            raise ValueError(f"Missing required AWS configuration: {', '.join(missing_vars)}")
        
        return True