"""
AI Interviews Backend - SQS-Triggered Interview Processing Pipeline
Main module that orchestrates the complete SQS-triggered video-to-questions pipeline using AWS services
"""
import os
import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

# Import our custom modules
from config import AWSConfig
from aws_clients import AWSServiceClients
from interview_workflow import InterviewProcessingWorkflow

# Legacy imports for backwards compatibility
from video_processor import VideoProcessor
from audio_transcriber import AudioTranscriber
from question_extractor import QuestionExtractor

# Configure logging with better formatting and UTF-8 encoding
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('interview_processing.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Set boto3 logging to WARNING to reduce noise
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

class VideoQuestionPipeline:
    """
    Main pipeline class that orchestrates the complete video processing workflow
    """
    
    def __init__(self):
        """Initialize the pipeline with all required components"""
        try:
            logger.info("Initializing Video Question Pipeline")
            
            # Initialize configuration
            self.config = AWSConfig()
            self.config.validate_config()
            
            # Initialize service clients
            self.aws_clients = AWSServiceClients()
            
            # Initialize processing modules
            self.video_processor = VideoProcessor()
            self.audio_transcriber = AudioTranscriber()
            self.question_extractor = QuestionExtractor()
            
            logger.info("Pipeline initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize pipeline: {str(e)}")
            raise
    
    def process_video_file(self, 
                          video_path: str,
                          output_dir: Optional[str] = None,
                          language_code: str = 'en-US') -> Dict[str, Any]:
        """
        Process a single video file through the complete pipeline
        
        Args:
            video_path: Path to the input video file
            output_dir: Optional output directory for results
            language_code: Language code for transcription
            
        Returns:
            Complete processing results
        """
        start_time = datetime.now()
        video_name = Path(video_path).stem
        
        logger.info(f"Starting pipeline processing for video: {video_path}")
        
        results = {
            'video_path': video_path,
            'video_name': video_name,
            'start_time': start_time.isoformat(),
            'status': 'processing',
            'steps': {}
        }
        
        try:
            # Step 1: Video Processing (Extract audio and upload to S3)
            logger.info("Step 1: Processing video and extracting audio")
            video_results = self.video_processor.process_video(
                video_path, 
            )
            
            results['steps']['video_processing'] = video_results
            
            if video_results['status'] != 'success':
                raise Exception(f"Video processing failed: {video_results.get('error_message', 'Unknown error')}")
            
            # Step 2: Audio Transcription
            logger.info("Step 2: Transcribing audio to text")
            transcription_results = self.audio_transcriber.transcribe_audio(
                video_results['audio_s3_uri'],
                language_code=language_code,
                wait_for_completion=True
            )
            
            results['steps']['transcription'] = transcription_results
            
            if transcription_results['status'] != 'success':
                raise Exception(f"Audio transcription failed: {transcription_results.get('error_message', 'Unknown error')}")
            
            # Step 3: Question Extraction
            logger.info("Step 3: Extracting questions from transcript")
            full_transcript = transcription_results.get('full_transcript', '')
            
            question_results = self.question_extractor.extract_questions(
                full_transcript,
            )
            
            results['steps']['question_extraction'] = question_results
            
            if question_results['status'] != 'success':
                raise Exception(f"Question extraction failed: {question_results.get('error_message', 'Unknown error')}")
            
            # Step 4: Generate final results
            logger.info("Step 4: Generating final results")
            end_time = datetime.now()
            processing_duration = (end_time - start_time).total_seconds()
            
            # Create simplified results structure
            simplified_results = {
                'video_path': video_path,
                'status': 'success',
                'summary': {
                    'video_duration_seconds': self.video_processor.get_video_info(video_path).get('duration_seconds', 0),
                    'transcript_length': len(full_transcript),
                    'total_questions_found': question_results.get('total_questions', 0),
                    'processing_duration_seconds': processing_duration,
                    'aws_services_used': ['s3', 'transcribe', 'bedrock']
                },
                'questions': question_results.get('interviewer_questions', [])
            }
            
            # Update the main results object for internal use
            results.update({
                'status': 'success',
                'end_time': end_time.isoformat(),
                'processing_duration_seconds': processing_duration,
                'summary': simplified_results['summary']
            })
            
            # Save simplified results to file if output directory specified
            if output_dir:
                self._save_results_to_file(simplified_results, output_dir, video_name)
            
            logger.info(f"Pipeline processing completed successfully in {processing_duration:.2f} seconds")
            return results
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Pipeline processing failed: {error_msg}")
            
            end_time = datetime.now()
            processing_duration = (end_time - start_time).total_seconds()
            
            # Create simplified error results
            simplified_error_results = {
                'video_path': video_path,
                'status': 'error',
                'summary': {
                    'error_message': error_msg,
                    'processing_duration_seconds': processing_duration
                },
                'questions': []
            }
            
            # Update main results for internal use
            results.update({
                'status': 'error',
                'error_message': error_msg,
                'end_time': end_time.isoformat(),
                'processing_duration_seconds': processing_duration
            })
            
            # Save simplified error results to file if output directory specified  
            if output_dir:
                self._save_results_to_file(simplified_error_results, output_dir, video_name)
            
            return results
    
    def _save_results_to_file(self, results: Dict[str, Any], output_dir: str, video_name: str):
        """Save processing results to a JSON file"""
        try:
            os.makedirs(output_dir, exist_ok=True)
            
            output_file = os.path.join(output_dir, f"{video_name}_results.json")
            
            # Make results JSON serializable
            serializable_results = self._make_json_serializable(results)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(serializable_results, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Results saved to: {output_file}")
            
        except Exception as e:
            logger.error(f"Failed to save results to file: {str(e)}")
    
    def _make_json_serializable(self, obj):
        """Convert object to JSON serializable format"""
        if isinstance(obj, dict):
            return {key: self._make_json_serializable(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._make_json_serializable(item) for item in obj]
        elif isinstance(obj, (datetime,)):
            return obj.isoformat()
        elif hasattr(obj, '__dict__'):
            return self._make_json_serializable(obj.__dict__)
        else:
            return obj

def main():
    """Main function for CLI usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='AI Interviews Processing Pipeline')
    parser.add_argument('--mode', choices=['sqs', 'single-video', 'single-interview'], default='sqs',
                       help='Processing mode: sqs (continuous SQS processing), single-video (legacy mode), single-interview (process by interview ID)')
    parser.add_argument('--interview-id', help='Interview ID for single-interview mode')
    parser.add_argument('video_path', nargs='?', help='Path to video file (for single-video mode)')
    parser.add_argument('--output-dir', help='Output directory for results (single-video mode only)')
    parser.add_argument('--language', default='en-US', help='Language code for transcription')
    
    args = parser.parse_args()
    
    try:
        if args.mode == 'sqs':
            # New SQS-triggered continuous processing mode
            logger.info("Starting SQS-triggered continuous processing mode")
            workflow = InterviewProcessingWorkflow()
            workflow.run_continuous_processing()
            
        elif args.mode == 'single-interview':
            # Process a single interview by ID
            if not args.interview_id:
                print("Error: --interview-id is required for single-interview mode")
                return 1
                
            logger.info(f"Starting single interview processing: {args.interview_id}")
            workflow = InterviewProcessingWorkflow()
            success = workflow.process_single_interview(args.interview_id)
            
            if success:
                print(f"✓ Successfully processed interview: {args.interview_id}")
            else:
                print(f"✗ Failed to process interview: {args.interview_id}")
                return 1
                
        elif args.mode == 'single-video':
            # Legacy single video processing mode
            if not args.video_path:
                print("Error: video_path is required for single-video mode")
                return 1
                
            logger.info(f"Starting legacy single video processing: {args.video_path}")
            pipeline = VideoQuestionPipeline()
            results = pipeline.process_video_file(
                args.video_path,
                output_dir=args.output_dir,
                language_code=args.language
            )
            
            # Print summary
            print("\n=== Processing Summary ===")
            if results['status'] == 'success':
                summary = results['summary']
                print(f"✓ Processing completed successfully")
                print(f"  Video duration: {summary['video_duration_seconds']:.1f} seconds")
                print(f"  Questions found: {summary['total_questions_found']}")
                print(f"  Processing time: {results['processing_duration_seconds']:.1f} seconds")
            else:
                print(f"✗ Processing failed: {results['error_message']}")
                return 1
            
            if args.output_dir:
                print(f"Results saved to: {args.output_dir}")
    
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down gracefully")
        print("\nShutdown requested, stopping processing...")
    except Exception as e:
        logger.error(f"Application failed: {str(e)}")
        print(f"Error: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())