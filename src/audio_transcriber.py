"""
Audio Transcription Module using AWS Transcribe
Handles audio-to-text conversion using AWS Transcribe service
"""
import time
import logging
import uuid
from typing import Dict, Optional, List
from botocore.exceptions import ClientError
from aws_clients import AWSServiceClients
from config import AWSConfig

logger = logging.getLogger(__name__)

class AudioTranscriber:
    """Handles audio transcription using AWS Transcribe"""
    
    def __init__(self):
        self.aws_clients = AWSServiceClients()
        self.config = AWSConfig()
        
    def start_transcription_job(self, 
                              audio_s3_uri: str, 
                              job_name: Optional[str] = None,
                              language_code: str = 'es-ES') -> str:
        """
        Start an AWS Transcribe job
        
        Args:
            audio_s3_uri: S3 URI of the audio file
            job_name: Optional job name (will generate if not provided)
            language_code: Language code for transcription
            
        Returns:
            Transcription job name
        """
        try:
            # Generate job name if not provided
            if job_name is None:
                job_name = f"{self.config.TRANSCRIBE_JOB_PREFIX}{uuid.uuid4().hex[:8]}"
            
            logger.info(f"Starting transcription job: {job_name}")
            
            # Start transcription job
            response = self.aws_clients.transcribe_client.start_transcription_job(
                TranscriptionJobName=job_name,
                Media={'MediaFileUri': audio_s3_uri},
                MediaFormat='wav',  # Assuming WAV format from video processor
                LanguageCode=language_code,
                Settings={
                    'ShowSpeakerLabels': True,
                    'MaxSpeakerLabels': 10,
                    'ShowAlternatives': True,
                    'MaxAlternatives': 2
                }
            )
            
            logger.info(f"Transcription job started successfully: {job_name}")
            return job_name
            
        except ClientError as e:
            logger.error(f"Failed to start transcription job: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Transcription job failed: {str(e)}")
            raise
    
    def check_job_status(self, job_name: str) -> Dict:
        """
        Check the status of a transcription job
        
        Args:
            job_name: Name of the transcription job
            
        Returns:
            Job status information
        """
        try:
            response = self.aws_clients.transcribe_client.get_transcription_job(
                TranscriptionJobName=job_name
            )
            
            job = response['TranscriptionJob']
            status_info = {
                'job_name': job_name,
                'status': job['TranscriptionJobStatus'],
                'creation_time': job.get('CreationTime'),
                'completion_time': job.get('CompletionTime'),
                'failure_reason': job.get('FailureReason')
            }
            
            if job['TranscriptionJobStatus'] == 'COMPLETED':
                status_info['transcript_uri'] = job['Transcript']['TranscriptFileUri']
            
            return status_info
            
        except ClientError as e:
            logger.error(f"Failed to check job status: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Job status check failed: {str(e)}")
            raise
    
    def wait_for_job_completion(self, 
                               job_name: str, 
                               max_wait_time: int = 3600, 
                               poll_interval: int = 30) -> Dict:
        """
        Wait for transcription job to complete
        
        Args:
            job_name: Name of the transcription job
            max_wait_time: Maximum time to wait in seconds
            poll_interval: Time between status checks in seconds
            
        Returns:
            Final job status information
        """
        start_time = time.time()
        
        logger.info(f"Waiting for transcription job to complete: {job_name}")
        
        while time.time() - start_time < max_wait_time:
            status_info = self.check_job_status(job_name)
            status = status_info['status']
            
            if status == 'COMPLETED':
                logger.info(f"Transcription job completed: {job_name}")
                return status_info
            elif status == 'FAILED':
                error_msg = f"Transcription job failed: {status_info.get('failure_reason', 'Unknown error')}"
                logger.error(error_msg)
                raise Exception(error_msg)
            elif status in ['IN_PROGRESS', 'QUEUED']:
                logger.info(f"Transcription job status: {status}. Waiting...")
                time.sleep(poll_interval)
            else:
                logger.warning(f"Unknown job status: {status}")
                time.sleep(poll_interval)
        
        raise TimeoutError(f"Transcription job did not complete within {max_wait_time} seconds")
    
    def get_transcript_text(self, transcript_uri: str) -> Dict:
        """
        Download and parse transcript from S3
        
        Args:
            transcript_uri: URI of the transcript file
            
        Returns:
            Parsed transcript data
        """
        try:
            import json
            import requests
            
            logger.info(f"Downloading transcript from: {transcript_uri}")
            
            # Download transcript JSON
            response = requests.get(transcript_uri)
            response.raise_for_status()
            
            transcript_data = response.json()
            
            # Extract the main transcript text
            transcript_text = transcript_data['results']['transcripts'][0]['transcript']
            
            # Extract detailed results with timestamps and speaker labels
            items = transcript_data['results']['items']
            speakers = transcript_data['results'].get('speaker_labels', {}).get('segments', [])
            
            # Parse detailed transcript with speaker information
            detailed_transcript = []
            current_speaker = None
            current_text = ""
            current_start_time = None
            
            for item in items:
                if item['type'] == 'pronunciation':
                    word = item['alternatives'][0]['content']
                    start_time = float(item['start_time'])
                    end_time = float(item['end_time'])
                    confidence = float(item['alternatives'][0]['confidence'])
                    
                    # Find speaker for this timestamp
                    speaker = self._find_speaker_for_time(speakers, start_time)
                    
                    if speaker != current_speaker:
                        if current_text:
                            detailed_transcript.append({
                                'speaker': current_speaker,
                                'text': current_text.strip(),
                                'start_time': current_start_time
                            })
                        current_speaker = speaker
                        current_text = word
                        current_start_time = start_time
                    else:
                        current_text += f" {word}"
            
            # Add the last segment
            if current_text:
                detailed_transcript.append({
                    'speaker': current_speaker,
                    'text': current_text.strip(),
                    'start_time': current_start_time
                })
            
            return {
                'full_transcript': transcript_text,
                'detailed_transcript': detailed_transcript,
                'raw_data': transcript_data
            }
            
        except requests.RequestException as e:
            logger.error(f"Failed to download transcript: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Failed to parse transcript: {str(e)}")
            raise
    
    def _find_speaker_for_time(self, speakers: List[Dict], timestamp: float) -> Optional[str]:
        """Find the speaker for a given timestamp"""
        for speaker_segment in speakers:
            start_time = float(speaker_segment['start_time'])
            end_time = float(speaker_segment['end_time'])
            
            if start_time <= timestamp <= end_time:
                return speaker_segment['speaker_label']
        
        return None
    
    def transcribe_audio(self, 
                        audio_s3_uri: str, 
                        language_code: str = 'es-ES',
                        wait_for_completion: bool = True) -> Dict:
        """
        Complete audio transcription pipeline
        
        Args:
            audio_s3_uri: S3 URI of the audio file
            language_code: Language code for transcription
            wait_for_completion: Whether to wait for job completion
            
        Returns:
            Transcription results
        """
        try:
            logger.info(f"Starting audio transcription for: {audio_s3_uri}")
            
            # Start transcription job
            job_name = self.start_transcription_job(audio_s3_uri, language_code=language_code)
            
            results = {
                'job_name': job_name,
                'audio_s3_uri': audio_s3_uri,
                'language_code': language_code
            }
            
            if wait_for_completion:
                # Wait for completion
                status_info = self.wait_for_job_completion(job_name)
                results.update(status_info)
                
                if status_info['status'] == 'COMPLETED':
                    # Get transcript text
                    transcript_data = self.get_transcript_text(status_info['transcript_uri'])
                    results.update(transcript_data)
                    results['status'] = 'success'
                else:
                    results['status'] = 'failed'
            else:
                results['status'] = 'job_started'
            
            return results
            
        except Exception as e:
            logger.error(f"Audio transcription failed: {str(e)}")
            return {
                'status': 'error',
                'error_message': str(e),
                'audio_s3_uri': audio_s3_uri
            }
    
    def list_transcription_jobs(self, max_results: int = 50) -> List[Dict]:
        """
        List recent transcription jobs
        
        Args:
            max_results: Maximum number of jobs to return
            
        Returns:
            List of transcription job summaries
        """
        try:
            response = self.aws_clients.transcribe_client.list_transcription_jobs(
                MaxResults=max_results
            )
            
            jobs = []
            for job_summary in response['TranscriptionJobSummaries']:
                jobs.append({
                    'job_name': job_summary['TranscriptionJobName'],
                    'status': job_summary['TranscriptionJobStatus'],
                    'language_code': job_summary['LanguageCode'],
                    'creation_time': job_summary.get('CreationTime'),
                    'completion_time': job_summary.get('CompletionTime')
                })
            
            return jobs
            
        except Exception as e:
            logger.error(f"Failed to list transcription jobs: {str(e)}")
            return []