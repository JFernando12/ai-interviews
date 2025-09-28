"""
Video Processing Module
Handles video upload to S3 and audio extraction
"""
import os
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from moviepy import VideoFileClip
from botocore.exceptions import ClientError
from aws_clients import AWSServiceClients
from config import AWSConfig

logger = logging.getLogger(__name__)

class VideoProcessor:
    """Handles video processing and S3 operations"""
    
    def __init__(self):
        self.aws_clients = AWSServiceClients()
        self.config = AWSConfig()
        
    def validate_video_file(self, file_path: str) -> bool:
        """Validate if the file is a supported video format"""
        if not os.path.exists(file_path):
            logger.error(f"Video file not found: {file_path}")
            return False
        
        file_extension = Path(file_path).suffix.lower()
        if file_extension not in self.config.SUPPORTED_VIDEO_FORMATS:
            logger.error(f"Unsupported video format: {file_extension}")
            return False
        
        return True
    
    def extract_audio_from_video(self, video_path: str, output_path: Optional[str] = None) -> str:
        """
        Extract audio from video file using MoviePy
        
        Args:
            video_path: Path to the input video file
            output_path: Optional output path for audio file
            
        Returns:
            Path to extracted audio file
        """
        try:
            if not self.validate_video_file(video_path):
                raise ValueError(f"Invalid video file: {video_path}")
            
            # Generate output path if not provided
            if output_path is None:
                video_name = Path(video_path).stem
                output_path = f"{video_name}_audio.wav"
            
            logger.info(f"Extracting audio from {video_path}")
            
            # Load video and extract audio
            with VideoFileClip(video_path) as video:
                audio = video.audio
                if audio is None:
                    raise ValueError("No audio track found in the video file")
                
                # Convert to specified format for AWS Transcribe compatibility
                audio.write_audiofile(
                    output_path,
                    fps=self.config.AUDIO_SAMPLE_RATE,
                    nbytes=2,
                    codec='pcm_s16le'
                )
            
            logger.info(f"Audio extracted successfully: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Failed to extract audio from video: {str(e)}")
            raise
    
    def upload_file_to_s3(self, file_path: str, s3_key: Optional[str] = None) -> str:
        """
        Upload file to S3 bucket
        
        Args:
            file_path: Local path to the file
            s3_key: Optional S3 key (path in bucket)
            
        Returns:
            S3 URI of uploaded file
        """
        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")
            
            # Generate S3 key if not provided
            if s3_key is None:
                s3_key = f"uploads/{Path(file_path).name}"
            
            logger.info(f"Uploading {file_path} to S3 bucket {self.config.S3_BUCKET}")
            
            # Upload file
            self.aws_clients.s3_client.upload_file(
                file_path,
                self.config.S3_BUCKET,
                s3_key
            )
            
            # Generate S3 URI
            s3_uri = f"s3://{self.config.S3_BUCKET}/{s3_key}"
            logger.info(f"File uploaded successfully: {s3_uri}")
            
            return s3_uri
            
        except ClientError as e:
            logger.error(f"AWS S3 upload failed: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"File upload failed: {str(e)}")
            raise
    
    def process_video(self, video_path: str) -> Dict[str, str]:
        """
        Complete video processing pipeline: extract audio and upload to S3
        
        Args:
            video_path: Path to input video file
            keep_local_audio: Whether to keep the local audio file after upload
            
        Returns:
            Dictionary containing processing results
        """
        try:
            logger.info(f"Starting video processing for: {video_path}")
            
            # Step 1: Extract audio from video
            audio_path = self.extract_audio_from_video(video_path)
            
            # Step 2: Upload original video to S3
            video_s3_uri = self.upload_file_to_s3(
                video_path, 
                f"videos/{Path(video_path).name}"
            )
            
            # Step 3: Upload audio to S3
            audio_s3_uri = self.upload_file_to_s3(
                audio_path,
                f"audio/{Path(audio_path).name}"
            )
            
            # Clean up local audio file if requested
            os.remove(audio_path)
            logger.info(f"Local audio file removed: {audio_path}")
            
            results = {
                'status': 'success',
                'video_path': video_path,
                'video_s3_uri': video_s3_uri,
                'audio_s3_uri': audio_s3_uri,
            }
            
            logger.info("Video processing completed successfully")
            return results
            
        except Exception as e:
            logger.error(f"Video processing failed: {str(e)}")
            return {
                'status': 'error',
                'video_path': video_path,
                'error_message': str(e)
            }
    
    def get_video_info(self, video_path: str) -> Dict[str, Any]:
        """
        Get basic information about a video file
        
        Args:
            video_path: Path to video file
            
        Returns:
            Dictionary containing video information
        """
        try:
            if not self.validate_video_file(video_path):
                raise ValueError(f"Invalid video file: {video_path}")
            
            with VideoFileClip(video_path) as video:
                info = {
                    'duration_seconds': video.duration,
                    'fps': video.fps,
                    'size': video.size,
                    'has_audio': video.audio is not None,
                    'file_size_bytes': os.path.getsize(video_path)
                }
            
            return info
            
        except Exception as e:
            logger.error(f"Failed to get video info: {str(e)}")
            return {'error': str(e)}