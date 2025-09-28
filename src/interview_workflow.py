"""
Interview Processing Workflow
Orchestrates the complete SQS-triggered interview processing pipeline
"""
import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

from sqs_handler import SQSHandler
from dynamodb_handler import DynamoDBHandler
from video_processor import VideoProcessor
from audio_transcriber import AudioTranscriber
from question_extractor import QuestionExtractor
from config import AWSConfig
from error_handling import (
    handle_errors, retry_with_backoff, validate_interview_data,
    log_processing_metrics, InterviewProcessingError, ValidationError,
    AWSServiceError, VideoProcessingError
)

logger = logging.getLogger(__name__)

class InterviewProcessingWorkflow:
    """
    Main workflow class that implements the 6-step SQS-triggered interview processing
    """
    
    def __init__(self):
        """Initialize the workflow with all required components"""
        try:
            logger.info("Initializing Interview Processing Workflow")
            
            # Initialize configuration
            self.config = AWSConfig()
            self.config.validate_config()
            
            # Initialize handlers
            self.sqs_handler = SQSHandler()
            self.dynamodb_handler = DynamoDBHandler()
            
            # Initialize existing processing modules
            self.video_processor = VideoProcessor()
            self.audio_transcriber = AudioTranscriber()
            self.question_extractor = QuestionExtractor()
            
            logger.info("Workflow initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize workflow: {str(e)}")
            raise
    
    @handle_errors("Interview processing failed")
    def process_interview_message(self, interview_id: str, sqs_message: Dict[str, Any]) -> bool:
        """
        Complete 6-step interview processing workflow
        
        Args:
            interview_id: UUID of the interview to process
            sqs_message: Original SQS message (for extended visibility timeout if needed)
            
        Returns:
            True if successful, False otherwise
        """
        start_time = datetime.now()
        logger.info(f"Starting interview processing for ID: {interview_id}")
        
        try:
            # Validate interview_id format
            from error_handling import validate_uuid
            validate_uuid(interview_id, "Interview ID")
            
            # Step 1: Already done - SQS message received with interview_id
            logger.info(f"Step 1: [OK] SQS message received for interview: {interview_id}")
            
            # Step 2: Get the interview from DynamoDB
            logger.info("Step 2: Getting interview from DynamoDB")
            interview = self.dynamodb_handler.get_interview_by_id(interview_id)
            
            if not interview:
                raise ValidationError(f"Interview not found in DynamoDB: {interview_id}")
            
            # Validate interview data
            interview = validate_interview_data(interview)
            
            video_path = interview['video_path']
            user_id = interview['user_id']
            interview_type = interview.get('type')  # Optional field
            programming_language = interview.get('programming_language')  # Optional field
            
            logger.info(f"[OK] Retrieved interview: video_path={video_path}, user_id={user_id}, type={interview_type}, programming_language={programming_language}")
            
            # Step 3: Update interview state to 'processing'
            logger.info("Step 3: Updating interview state to 'processing'")
            if not self.dynamodb_handler.update_interview_state(interview_id, 'processing'):
                raise AWSServiceError(f"Failed to update interview state to 'processing': {interview_id}")
            
            logger.info(f"[OK] Interview state updated to 'processing'")
            
            # Step 4: Process the video to extract questions with AI
            logger.info("Step 4: Processing video and extracting questions")
            
            # Extend SQS message visibility for long processing
            if sqs_message:
                self.sqs_handler.change_message_visibility(sqs_message, 1800)  # 30 minutes
            
            questions = self._process_video_and_extract_questions(video_path)
            
            if questions is None:
                raise VideoProcessingError(f"Failed to process video and extract questions: {video_path}")
            
            logger.info(f"[OK] Extracted {len(questions)} questions from video")
            
            # Step 5: Save questions to DynamoDB
            logger.info("Step 5: Saving questions to DynamoDB")
            
            if not self.dynamodb_handler.save_questions_batch(
                interview_id, user_id, questions, interview_type, programming_language
            ):
                raise AWSServiceError(f"Failed to save questions to DynamoDB: {interview_id}")
            
            logger.info(f"[OK] Saved {len(questions)} questions to DynamoDB")
            
            # Step 6: Update interview state to 'completed'
            logger.info("Step 6: Updating interview state to 'completed'")
            if not self.dynamodb_handler.update_interview_state(interview_id, 'completed'):
                raise AWSServiceError(f"Failed to update interview state to 'completed': {interview_id}")
            
            logger.info(f"[OK] Interview state updated to 'completed'")
            
            # Log final success metrics
            end_time = datetime.now()
            log_processing_metrics(start_time, end_time, interview_id, len(questions), True)
            
            logger.info(f"[SUCCESS] Successfully completed interview processing for {interview_id}")
            
            return True
            
        except (ValidationError, AWSServiceError, VideoProcessingError) as e:
            # These are our custom exceptions, log and update state
            logger.error(f"Interview processing failed for {interview_id}: {str(e)}")
            
            # Log failure metrics
            end_time = datetime.now()
            log_processing_metrics(start_time, end_time, interview_id, 0, False)
            
            # Update interview state to 'failed'
            try:
                self.dynamodb_handler.update_interview_state(interview_id, 'failed')
            except Exception as update_error:
                logger.error(f"Failed to update interview state to 'failed': {str(update_error)}")
            
            return False
            
        except Exception as e:
            # Unexpected errors
            logger.error(f"Unexpected error processing interview {interview_id}: {str(e)}")
            
            # Log failure metrics
            end_time = datetime.now()
            log_processing_metrics(start_time, end_time, interview_id, 0, False)
            
            # Update interview state to 'failed' on any error
            try:
                self.dynamodb_handler.update_interview_state(interview_id, 'failed')
            except Exception as update_error:
                logger.error(f"Failed to update interview state to 'failed': {str(update_error)}")
            
            return False
    
    @retry_with_backoff(max_retries=2)
    @handle_errors("Video processing and question extraction failed")
    def _process_video_and_extract_questions(self, video_path: str) -> Optional[list]:
        """
        Process video file and extract questions using existing modules
        
        Args:
            video_path: S3 path or local path to video file (e.g., videos/uuid/uuid/filename.mkv)
            
        Returns:
            List of question dictionaries or None if processing failed
        """
        try:
            logger.info(f"Processing video file: {video_path}")
            
            # Check if video_path is S3 path or local path
            if video_path.startswith('videos/'):
                # This is an S3 key, we need to construct the full S3 URI
                s3_uri = f"s3://{self.config.S3_BUCKET}/{video_path}"
                logger.info(f"Constructed S3 URI: {s3_uri}")
            else:
                # Assume it's a local path or already a full URI
                s3_uri = video_path
            
            # Step 4.1: Process video (extract audio and upload to S3)
            logger.info("Processing video and extracting audio")
            video_results = self.video_processor.process_video(s3_uri)
            
            if video_results['status'] != 'success':
                logger.error(f"Video processing failed: {video_results.get('error_message', 'Unknown error')}")
                return None
            
            # Step 4.2: Audio transcription
            logger.info("Transcribing audio to text")
            transcription_results = self.audio_transcriber.transcribe_audio(
                video_results['audio_s3_uri'],
                language_code='en-US',
                wait_for_completion=True
            )
            
            if transcription_results['status'] != 'success':
                logger.error(f"Audio transcription failed: {transcription_results.get('error_message', 'Unknown error')}")
                return None
            
            # Step 4.3: Question extraction
            logger.info("Extracting questions from transcript")
            full_transcript = transcription_results.get('full_transcript', '')
            
            if not full_transcript:
                logger.error("No transcript available for question extraction")
                return None
            
            question_results = self.question_extractor.extract_questions(full_transcript)
            
            if question_results['status'] != 'success':
                logger.error(f"Question extraction failed: {question_results.get('error_message', 'Unknown error')}")
                return None
            
            # Convert questions to the format expected by DynamoDB
            questions = self._format_questions_for_database(question_results.get('interviewer_questions', []))
            
            return questions
            
        except Exception as e:
            logger.error(f"Error processing video and extracting questions: {str(e)}")
            return None
    
    def _format_questions_for_database(self, raw_questions: list) -> list:
        """
        Format questions from the extractor for database storage
        
        Args:
            raw_questions: Raw questions from question extractor
            
        Returns:
            Formatted questions for database
        """
        formatted_questions = []
        
        for question in raw_questions:
            formatted_question = {}
            
            # Map fields from extractor output to database schema
            if isinstance(question, dict):
                formatted_question['question'] = question.get('question', '')
                formatted_question['answer'] = question.get('professional_answer', question.get('answer', ''))
                formatted_question['context'] = question.get('question_context', question.get('context', ''))
            elif isinstance(question, str):
                # If it's just a string, treat it as the question
                formatted_question['question'] = question
                formatted_question['answer'] = ''
                formatted_question['context'] = ''
            
            # Only add if we have a valid question
            if formatted_question.get('question', '').strip():
                formatted_questions.append(formatted_question)
        
        return formatted_questions
    
    def run_continuous_processing(self):
        """
        Run the continuous SQS message processing loop
        """
        logger.info("Starting continuous interview processing")
        
        def message_processor(interview_id: str, sqs_message: Dict[str, Any]) -> bool:
            """Callback function for processing each SQS message"""
            return self.process_interview_message(interview_id, sqs_message)
        
        # Start the SQS message processor loop
        self.sqs_handler.run_message_processor(message_processor)
    
    def process_single_interview(self, interview_id: str) -> bool:
        """
        Process a single interview by ID (for testing/manual processing)
        
        Args:
            interview_id: UUID of the interview to process
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Processing single interview: {interview_id}")
        return self.process_interview_message(interview_id, None)