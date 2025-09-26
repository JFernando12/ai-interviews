"""
Test Script for AI Interviews Video Processing Pipeline
This script tests the basic functionality and AWS connections
"""
import sys
import os
from pathlib import Path

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

try:
    from main import VideoQuestionPipeline
    from config import AWSConfig
    print("✓ All modules imported successfully")
except ImportError as e:
    print(f"✗ Import error: {e}")
    print("Please ensure all dependencies are installed: pip install -r requirements.txt")
    sys.exit(1)

def test_configuration():
    """Test configuration loading"""
    print("\n=== Testing Configuration ===")
    try:
        config = AWSConfig()
        print(f"✓ AWS Region: {config.REGION}")
        print(f"✓ S3 Bucket: {config.S3_BUCKET}")
        
        # Test validation (will raise error if config is missing)
        config.validate_config()
        print("✓ Configuration validation passed")
        return True
    except ValueError as e:
        print(f"✗ Configuration error: {e}")
        print("Please check your .env file and ensure all required AWS settings are provided")
        return False
    except Exception as e:
        print(f"✗ Configuration test failed: {e}")
        return False

def test_aws_connections():
    """Test AWS service connections"""
    print("\n=== Testing AWS Connections ===")
    try:
        pipeline = VideoQuestionPipeline()
        connection_results = pipeline.test_aws_connections()
        
        all_success = True
        for service, result in connection_results.items():
            status = "✓" if result['status'] == 'success' else "✗"
            print(f"{status} {service.upper()}: {result['message']}")
            if result['status'] != 'success':
                all_success = False
        
        return all_success
    except Exception as e:
        print(f"✗ AWS connection test failed: {e}")
        return False

def test_video_info_extraction():
    """Test video information extraction without processing"""
    print("\n=== Testing Video Info Extraction ===")
    try:
        # This test doesn't require a real video file, just tests the import and initialization
        from video_processor import VideoProcessor
        processor = VideoProcessor()
        print("✓ VideoProcessor initialized successfully")
        print("✓ Video processing module is ready")
        return True
    except Exception as e:
        print(f"✗ Video processor test failed: {e}")
        print("Note: MoviePy might not be properly installed or FFmpeg is missing")
        return False

def test_transcription_module():
    """Test transcription module initialization"""
    print("\n=== Testing Transcription Module ===")
    try:
        from audio_transcriber import AudioTranscriber
        transcriber = AudioTranscriber()
        print("✓ AudioTranscriber initialized successfully")
        
        # Test job listing (doesn't require actual transcription)
        jobs = transcriber.list_transcription_jobs(max_results=1)
        print(f"✓ Can connect to Transcribe service - Found {len(jobs)} recent jobs")
        return True
    except Exception as e:
        print(f"✗ Transcription module test failed: {e}")
        return False

def test_question_extraction_module():
    """Test question extraction module"""
    print("\n=== Testing Question Extraction Module ===")
    try:
        from question_extractor import QuestionExtractor
        extractor = QuestionExtractor()
        
        # Test regex extraction with sample text
        sample_text = """
        Hello, welcome to our interview. What is your experience with Python programming?
        How long have you been working in software development?
        Can you tell me about your most challenging project?
        This is not a question.
        Why did you choose this career path?
        """
        
        questions = extractor.extract_questions_regex(sample_text)
        print(f"✓ Question extraction working - Found {len(questions)} questions in sample text")
        
        # Display found questions
        for i, q in enumerate(questions[:3], 1):  # Show first 3 questions
            print(f"  {i}. {q['text']}")
        
        return True
    except Exception as e:
        print(f"✗ Question extraction test failed: {e}")
        return False

def generate_sample_usage():
    """Generate sample usage commands"""
    print("\n=== Sample Usage Commands ===")
    print("After ensuring your AWS credentials are configured, you can use:")
    print()
    print("1. Test AWS connections:")
    print("   python src/main.py --test-connections")
    print()
    print("2. Process a single video:")
    print("   python src/main.py path/to/your/video.mp4 --output-dir results/")
    print()
    print("3. Process multiple videos:")
    print("   python src/main.py path/to/video/directory/ --batch --output-dir results/")
    print()
    print("4. Use advanced AI features:")
    print("   python src/main.py path/to/video.mp4 --use-bedrock --output-dir results/")

def main():
    """Run all tests"""
    print("AI Interviews Video Processing Pipeline - System Test")
    print("=" * 60)
    
    tests = [
        ("Configuration", test_configuration),
        ("AWS Connections", test_aws_connections),
        ("Video Processing", test_video_info_extraction),
        ("Audio Transcription", test_transcription_module),
        ("Question Extraction", test_question_extraction_module)
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"✗ {test_name} test crashed: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "PASS" if result else "FAIL"
        print(f"{status:4} | {test_name}")
    
    print("-" * 60)
    print(f"Overall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Your system is ready to process videos.")
        generate_sample_usage()
    else:
        print("⚠️  Some tests failed. Please check the errors above and:")
        print("  1. Ensure all dependencies are installed")
        print("  2. Configure your .env file with valid AWS credentials")
        print("  3. Verify AWS permissions and S3 bucket access")
        print("  4. Install FFmpeg for video processing")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)