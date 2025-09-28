# AI Interviews - SQS-Triggered Video Processing Pipeline

This application processes interview videos using AWS services to automatically extract questions and answers. The system now supports both SQS-triggered processing for production use and direct video processing for development/testing.

## Architecture Overview

The application implements a 6-step SQS-triggered workflow:

1. **SQS Message Reception**: Receives messages from `app-interviews-sqs-videos` queue with `{"interview_id": "uuid"}`
2. **Interview Retrieval**: Gets interview data from DynamoDB table `app-interviews-interviews`
3. **State Update**: Updates interview state to 'processing'
4. **Video Processing**: Processes video, transcribes audio, and extracts questions using AI
5. **Question Storage**: Saves extracted questions to DynamoDB table `app-interviews-questions`
6. **Completion**: Updates interview state to 'completed'

## Features

- **SQS-Triggered Processing**: Automatic processing of interview videos when messages arrive
- **DynamoDB Integration**: Stores interview metadata and extracted questions
- **AWS AI Services**: Uses AWS Transcribe for speech-to-text and AWS Bedrock for question extraction
- **Robust Error Handling**: Comprehensive error handling with retry logic and state management
- **Monitoring & Logging**: Detailed logging with processing metrics
- **Backwards Compatibility**: Supports legacy direct video processing mode

## AWS Services Used

- **Amazon S3**: Video and audio file storage
- **Amazon SQS**: Message queue for triggering processing
- **Amazon DynamoDB**: Database for interviews and questions
- **Amazon Transcribe**: Audio-to-text transcription
- **Amazon Bedrock**: AI-powered question extraction
- **AWS IAM**: Authentication and authorization

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure AWS credentials and environment variables:
```bash
# Required
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
AWS_S3_BUCKET=your-s3-bucket

# Optional (defaults provided)
AWS_SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/947403101409/app-interviews-sqs-videos
AWS_DYNAMODB_INTERVIEWS_TABLE=app-interviews-interviews
AWS_DYNAMODB_QUESTIONS_TABLE=app-interviews-questions
```

## Usage

### Production Mode (SQS-Triggered)

Start the continuous SQS processing service:

```bash
cd src
python main.py --mode sqs
```

This will:
- Poll the SQS queue for new interview processing requests
- Process each interview through the 6-step workflow
- Handle errors gracefully and update interview states
- Continue running until interrupted

### Development/Testing Modes

Process a single interview by ID:
```bash
python main.py --mode single-interview --interview-id "uuid-here"
```

Process a single video file directly (legacy mode):
```bash
python main.py --mode single-video /path/to/video.mp4 --output-dir ./results
```

## Database Schema

### Interviews Table (`app-interviews-interviews`)
- `id` (String, PK): Interview UUID
- `video_path` (String): S3 path to video file (e.g., `videos/uuid/uuid/filename.mkv`)
- `user_id` (String): User UUID
- `state` (String): Processing state ('processing', 'completed', 'failed')
- `created_at`, `updated_at` (String): ISO timestamps

### Questions Table (`app-interviews-questions`)
- `id` (String, PK): Question UUID
- `interview_id` (String): Reference to interview
- `user_id` (String): User UUID
- `question` (String): The extracted question
- `answer` (String): Professional answer suggestion
- `context` (String): Question context (optional)
- `question_context` (String): Additional context (optional)
- `global` (Boolean): Whether question is globally applicable
- `created_at`, `updated_at` (String): ISO timestamps

## SQS Message Format

Send messages to the queue with this JSON format:
```json
{
  "interview_id": "uuid-of-interview-to-process"
}
```

## Error Handling

The system includes comprehensive error handling:

- **Validation Errors**: Invalid UUIDs, missing fields, malformed data
- **AWS Service Errors**: Network issues, permission problems, service limits
- **Processing Errors**: Video corruption, transcription failures, AI service issues
- **Retry Logic**: Automatic retries with exponential backoff for transient failures
- **State Management**: Interview state is updated to 'failed' on errors

## Logging

Logs are written to both console and `interview_processing.log` file:

- Processing steps and status updates
- Error details and stack traces
- Performance metrics (processing time, questions extracted)
- AWS service interactions

## Monitoring

Key metrics logged for each interview processing:
- Processing duration
- Number of questions extracted
- Success/failure status
- Interview ID for correlation

Example log entry:
```
PROCESSING_METRICS | Interview: abc123 | Status: SUCCESS | Duration: 45.23s | Questions: 8
```

## File Structure

```
src/
├── main.py                 # Main application entry point
├── interview_workflow.py   # SQS-triggered workflow orchestrator
├── sqs_handler.py          # SQS message processing
├── dynamodb_handler.py     # DynamoDB operations
├── error_handling.py       # Error handling utilities
├── config.py              # Configuration management
├── aws_clients.py         # AWS service clients
├── video_processor.py     # Video processing logic
├── audio_transcriber.py   # Audio transcription
└── question_extractor.py  # AI question extraction
```

## Development

For development and testing:

1. Use `--mode single-interview` to test specific interviews
2. Use `--mode single-video` to test video processing without DynamoDB
3. Check logs for detailed processing information
4. Monitor AWS service usage and costs

## Production Deployment

1. Deploy to EC2 instance or container
2. Configure proper IAM roles with required permissions
3. Set up CloudWatch monitoring for logs and metrics
4. Consider using AWS Systems Manager for configuration management
5. Implement health checks and auto-restart mechanisms

## IAM Permissions Required

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:ChangeMessageVisibility",
        "dynamodb:GetItem",
        "dynamodb:PutItem",
        "dynamodb:UpdateItem",
        "dynamodb:BatchWriteItem",
        "transcribe:StartTranscriptionJob",
        "transcribe:GetTranscriptionJob",
        "bedrock:InvokeModel"
      ],
      "Resource": "*"
    }
  ]
}
```

## Migration from Legacy System

If you're migrating from the previous direct video processing system:

1. The old `--video-path` usage is now `--mode single-video`
2. New SQS-triggered mode is the recommended production approach
3. All existing video processing functionality is preserved
4. DynamoDB integration is now the primary data storage method

## Troubleshooting

### Common Issues

1. **SQS Permissions**: Ensure proper SQS access permissions
2. **DynamoDB Access**: Verify table permissions and table existence
3. **S3 Video Access**: Check that video files exist at specified paths
4. **Processing Timeouts**: Large videos may require extended SQS visibility timeouts
5. **State Inconsistency**: Check logs for state update failures

### Development Tips

- Use `single-interview` mode to test specific interviews
- Monitor CloudWatch logs for detailed error information  
- Test with smaller video files first
- Verify AWS service quotas and limits
- Check that all required environment variables are set