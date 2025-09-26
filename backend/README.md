# AI Interviews Backend - AWS Video Processing Pipeline

This application processes videos to extract questions by converting them to audio, transcribing the audio to text, and using AWS AI services to extract questions from the transcript.

## Features

- **Video to Audio Conversion**: Extract audio from various video formats using MoviePy
- **AWS S3 Integration**: Upload and store video/audio files in S3
- **AWS Transcribe**: Convert audio to text with speaker identification
- **Amazon Bedrock** (Optional): Advanced AI-powered question extraction
- **Question Extraction**: Multiple methods to identify questions in transcripts
- **Batch Processing**: Process multiple videos in a directory
- **Comprehensive Logging**: Detailed logging and error handling
- **Cost Monitoring**: Track and estimate AWS service usage costs

## AWS Services Used

- **Amazon S3**: File storage and management
- **Amazon Transcribe**: Audio-to-text transcription
- **Amazon Comprehend**: Natural language processing
- **Amazon Bedrock** : Advanced AI models for question extraction

## Prerequisites

1. **Python 3.11+** installed on your system
2. **AWS Account** with appropriate IAM permissions
3. **AWS CLI** configured (optional but recommended)
4. **FFmpeg** installed (required by MoviePy for video processing)

### Required AWS IAM Permissions

Your AWS user/role needs the following permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket",
                "s3:HeadBucket"
            ],
            "Resource": [
                "arn:aws:s3:::your-bucket-name",
                "arn:aws:s3:::your-bucket-name/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "transcribe:StartTranscriptionJob",
                "transcribe:GetTranscriptionJob",
                "transcribe:ListTranscriptionJobs"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "bedrock:InvokeModel"
            ],
            "Resource": "*"
        }
    ]
}
```

## Installation

1. **Clone the repository** (if applicable):
   ```bash
   git clone <repository-url>
   cd ai-interviews/backend
   ```

2. **Create and activate virtual environment**:
   ```bash
   python -m venv venv
   
   # Windows
   venv\Scripts\activate
   
   # macOS/Linux
   source venv/bin/activate
   ```

3. **Install Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Install FFmpeg** (required for video processing):
   
   **Windows:**
   - Download from https://ffmpeg.org/download.html
   - Add to system PATH
   
   **macOS:**
   ```bash
   brew install ffmpeg
   ```
   
   **Linux (Ubuntu/Debian):**
   ```bash
   sudo apt update
   sudo apt install ffmpeg
   ```

5. **Configure AWS credentials**:
   
   Copy the example environment file:
   ```bash
   cp .env.example .env
   ```
   
   Edit `.env` with your AWS credentials:
   ```env
   AWS_ACCESS_KEY_ID=your_access_key_here
   AWS_SECRET_ACCESS_KEY=your_secret_key_here
   AWS_REGION=us-east-1
   AWS_S3_BUCKET=your-video-processing-bucket
   ```

6. **Create S3 Bucket** (if it doesn't exist):
   ```bash
   aws s3 mb s3://your-video-processing-bucket --region us-east-1
   ```

## Usage

### Command Line Interface

Test AWS connections:
```bash
python src/main.py --test-connections
```

Process a single video:
```bash
python src/main.py path/to/video.mp4 --output-dir results/
```

Process multiple videos in a directory:
```bash
python src/main.py path/to/videos/ --batch --output-dir results/
```

Use advanced AI features (Bedrock):
```bash
python src/main.py path/to/video.mp4 --use-bedrock --output-dir results/
```

Process with different language:
```bash
python src/main.py path/to/video.mp4 --language es-US --output-dir results/
```

### Programmatic Usage

```python
from src.main import VideoQuestionPipeline

# Initialize the pipeline
pipeline = VideoQuestionPipeline()

# Test AWS connections
connection_results = pipeline.test_aws_connections()
print(connection_results)

# Process a single video
results = pipeline.process_video_file(
    video_path="path/to/video.mp4",
    output_dir="results/",
    use_bedrock=False,
    language_code="en-US"
)

print(f"Questions found: {results['summary']['total_questions_found']}")

# Process multiple videos
batch_results = pipeline.batch_process_videos(
    video_directory="path/to/videos/",
    output_dir="results/"
)

print(f"Processed {batch_results['successful_files']} files successfully")
```

## Supported Video Formats

- MP4 (.mp4)
- AVI (.avi)
- MOV (.mov)
- MKV (.mkv)
- WMV (.wmv)

## Output Format

The application generates detailed JSON results containing:

- Video processing information
- Full transcript with speaker labels
- Extracted questions with confidence scores
- AWS service usage details
- Processing time and costs

Example output structure:
```json
{
  "video_path": "path/to/video.mp4",
  "status": "success",
  "summary": {
    "total_questions_found": 15,
    "video_duration_seconds": 180.5,
    "processing_duration_seconds": 45.2
  },
  "steps": {
    "video_processing": { ... },
    "transcription": { ... },
    "question_extraction": {
      "questions": [
        {
          "text": "What is your experience with Python?",
          "confidence": 0.95,
          "method": "regex",
          "speaker": "spk_0",
          "timestamp": 45.2
        }
      ]
    }
  }
}
```

## Configuration Options

### Environment Variables (.env file)

- `AWS_ACCESS_KEY_ID`: Your AWS access key
- `AWS_SECRET_ACCESS_KEY`: Your AWS secret key
- `AWS_SESSION_TOKEN`: (Optional) For temporary credentials
- `AWS_REGION`: AWS region (default: us-east-1)
- `AWS_S3_BUCKET`: S3 bucket name for file storage

### Application Settings

You can modify settings in `src/config.py`:

- Audio processing settings (sample rate, format)
- Supported file formats
- AWS service configuration
- Transcription job prefixes

## Troubleshooting

### Common Issues

1. **FFmpeg not found**:
   - Install FFmpeg and ensure it's in your system PATH
   - Test with: `ffmpeg -version`

2. **AWS credentials error**:
   - Verify your .env file has correct credentials
   - Test with: `python src/main.py --test-connections`

3. **S3 bucket access denied**:
   - Ensure bucket exists and you have proper permissions
   - Check bucket region matches your AWS_REGION setting

4. **Transcription job fails**:
   - Check audio file format (WAV works best)
   - Verify file is uploaded to S3 successfully

5. **No questions found**:
   - Check if transcript contains actual questions
   - Try different extraction methods (enable Bedrock)

### Logging

The application creates several log files:

- `pipeline_detailed.log`: Detailed application logs
- `pipeline_errors.log`: Error messages only
- `aws_operations.log`: AWS service operations
- `video_processing.log`: General processing log

## Cost Considerations

AWS services incur costs. Approximate pricing (US East region):

- **S3 Storage**: ~$0.023 per GB per month
- **S3 Requests**: ~$0.0004 per 1,000 requests
- **Transcribe**: ~$0.024 per minute of audio
- **Comprehend**: ~$0.0001 per 100 characters
- **Bedrock**: Varies by model (~$0.01-0.10 per 1,000 tokens)

Monitor costs using the built-in usage reporting:
```python
pipeline = VideoQuestionPipeline()
# ... process videos ...
usage_report = pipeline.resource_monitor.generate_usage_report()
print(f"Estimated cost: ${usage_report['total_estimated_cost_usd']}")
```

## Development

### Running Tests

```bash
pip install pytest
pytest tests/
```

### Code Formatting

```bash
pip install black flake8
black src/
flake8 src/
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

[Add your license information here]

## Support

For issues and questions:
1. Check the troubleshooting section above
2. Review the log files for detailed error information
3. Open an issue in the repository (if applicable)