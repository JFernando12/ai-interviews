"""
AI Interviews Backend - Video Processing Pipeline
Main module that orchestrates the complete video-to-questions pipeline using AWS services
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
from video_processor import VideoProcessor
from audio_transcriber import AudioTranscriber
from question_extractor import QuestionExtractor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('video_processing.log'),
        logging.StreamHandler()
    ]
)

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
    
    def test_aws_connections(self) -> Dict[str, Any]:
        """
        Test connections to all AWS services
        
        Returns:
            Dictionary containing connection test results
        """
        logger.info("Testing AWS service connections")
        return self.aws_clients.test_connections()
    
    def process_video_file(self, 
                          video_path: str,
                          output_dir: Optional[str] = None,
                          keep_intermediate_files: bool = False,
                          use_bedrock: bool = False,
                          language_code: str = 'en-US') -> Dict[str, Any]:
        """
        Process a single video file through the complete pipeline
        
        Args:
            video_path: Path to the input video file
            output_dir: Optional output directory for results
            keep_intermediate_files: Whether to keep audio files locally
            use_bedrock: Whether to use Bedrock AI for question extraction
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
                keep_local_audio=keep_intermediate_files
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
            transcript_data = {
                'full_transcript': full_transcript,
                'detailed_transcript': transcription_results.get('detailed_transcript', [])
            }
            
            question_results = self.question_extractor.extract_questions(
                full_transcript,
                transcript_data=transcript_data,
                use_bedrock=use_bedrock,
                use_comprehend=False  # Disabled as requested
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
                    'aws_services_used': ['s3', 'transcribe'] + (['bedrock'] if use_bedrock else [])
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
    
    def batch_process_videos(self, 
                           video_directory: str,
                           output_dir: Optional[str] = None,
                           file_extensions: Optional[list] = None,
                           **processing_kwargs) -> Dict[str, Any]:
        """
        Process multiple video files in a directory
        
        Args:
            video_directory: Directory containing video files
            output_dir: Optional output directory for results
            file_extensions: List of file extensions to process
            **processing_kwargs: Additional arguments for process_video_file
            
        Returns:
            Batch processing results
        """
        if file_extensions is None:
            file_extensions = self.config.SUPPORTED_VIDEO_FORMATS
        
        logger.info(f"Starting batch processing of videos in: {video_directory}")
        
        video_files = []
        for ext in file_extensions:
            video_files.extend(Path(video_directory).glob(f"*{ext}"))
        
        if not video_files:
            logger.warning(f"No video files found in directory: {video_directory}")
            return {
                'status': 'warning',
                'message': 'No video files found',
                'results': []
            }
        
        batch_results = {
            'status': 'processing',
            'total_files': len(video_files),
            'processed_files': 0,
            'successful_files': 0,
            'failed_files': 0,
            'results': []
        }
        
        for video_file in video_files:
            try:
                logger.info(f"Processing file {batch_results['processed_files'] + 1}/{len(video_files)}: {video_file.name}")
                
                result = self.process_video_file(
                    str(video_file),
                    output_dir=output_dir,
                    **processing_kwargs
                )
                
                batch_results['results'].append(result)
                batch_results['processed_files'] += 1
                
                if result['status'] == 'success':
                    batch_results['successful_files'] += 1
                else:
                    batch_results['failed_files'] += 1
                    
            except Exception as e:
                logger.error(f"Failed to process {video_file.name}: {str(e)}")
                batch_results['results'].append({
                    'video_path': str(video_file),
                    'status': 'error',
                    'error_message': str(e)
                })
                batch_results['processed_files'] += 1
                batch_results['failed_files'] += 1
        
        batch_results['status'] = 'completed'
        logger.info(f"Batch processing completed: {batch_results['successful_files']}/{batch_results['total_files']} files processed successfully")
        
        return batch_results
    
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
    
    parser = argparse.ArgumentParser(description='AI Interviews Video Processing Pipeline')
    parser.add_argument('video_path', nargs='?', help='Path to video file or directory')
    parser.add_argument('--output-dir', help='Output directory for results')
    parser.add_argument('--batch', action='store_true', help='Process directory of videos')
    parser.add_argument('--keep-files', action='store_true', help='Keep intermediate audio files')
    parser.add_argument('--use-bedrock', action='store_true', default=True, help='Use Bedrock AI for question extraction (default: enabled)')
    parser.add_argument('--no-bedrock', action='store_true', help='Disable Bedrock AI and skip question extraction')
    parser.add_argument('--language', default='en-US', help='Language code for transcription')
    parser.add_argument('--test-connections', action='store_true', help='Test AWS connections only')
    
    args = parser.parse_args()
    
    try:
        # Initialize pipeline
        pipeline = VideoQuestionPipeline()
        
        # Test connections if requested
        if args.test_connections:
            logger.info("Testing AWS connections...")
            connection_results = pipeline.test_aws_connections()
            
            print("\n=== AWS Connection Test Results ===")
            for service, result in connection_results.items():
                status = "✓" if result['status'] == 'success' else "✗"
                print(f"{status} {service}: {result['message']}")
            
            return
        
        # Check if video_path is provided for processing
        if not args.video_path:
            print("Error: video_path is required unless using --test-connections")
            parser.print_help()
            return 1
        
        # Process videos
        if args.batch:
            logger.info(f"Starting batch processing of directory: {args.video_path}")
            results = pipeline.batch_process_videos(
                args.video_path,
                output_dir=args.output_dir,
                keep_intermediate_files=args.keep_files,
                use_bedrock=args.use_bedrock and not args.no_bedrock,
                language_code=args.language
            )
        else:
            logger.info(f"Starting single video processing: {args.video_path}")
            results = pipeline.process_video_file(
                args.video_path,
                output_dir=args.output_dir,
                keep_intermediate_files=args.keep_files,
                use_bedrock=args.use_bedrock and not args.no_bedrock,
                language_code=args.language
            )
        
        # Print summary
        print("\n=== Processing Summary ===")
        if args.batch:
            print(f"Total files: {results['total_files']}")
            print(f"Successful: {results['successful_files']}")
            print(f"Failed: {results['failed_files']}")
        else:
            if results['status'] == 'success':
                summary = results['summary']
                print(f"✓ Processing completed successfully")
                print(f"  Video duration: {summary['video_duration_seconds']:.1f} seconds")
                print(f"  Questions found: {summary['total_questions_found']}")
                print(f"  Processing time: {results['processing_duration_seconds']:.1f} seconds")
            else:
                print(f"✗ Processing failed: {results['error_message']}")
        
        if args.output_dir:
            print(f"Results saved to: {args.output_dir}")
    
    except Exception as e:
        logger.error(f"Application failed: {str(e)}")
        print(f"Error: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())