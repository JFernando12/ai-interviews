"""
Error Handling and Validation Utilities
Common error handling, validation, and retry logic for the interview processing pipeline
"""
import logging
import time
import functools
from typing import Callable, Any, Optional
from botocore.exceptions import ClientError, BotoCoreError

logger = logging.getLogger(__name__)

class InterviewProcessingError(Exception):
    """Base exception for interview processing errors"""
    pass

class ValidationError(InterviewProcessingError):
    """Raised when validation fails"""
    pass

class AWSServiceError(InterviewProcessingError):
    """Raised when AWS service calls fail"""
    pass

class VideoProcessingError(InterviewProcessingError):
    """Raised when video processing fails"""
    pass

def retry_with_backoff(max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 60.0):
    """
    Decorator for retrying functions with exponential backoff
    
    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (ClientError, BotoCoreError) as e:
                    last_exception = e
                    error_code = getattr(e, 'response', {}).get('Error', {}).get('Code', 'Unknown')
                    
                    # Don't retry on certain error codes
                    if error_code in ['AccessDenied', 'InvalidParameterValue', 'ValidationException']:
                        logger.error(f"Non-retryable AWS error in {func.__name__}: {error_code}")
                        raise AWSServiceError(f"AWS service error: {str(e)}") from e
                    
                    if attempt < max_retries:
                        delay = min(base_delay * (2 ** attempt), max_delay)
                        logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {str(e)}. Retrying in {delay:.1f}s...")
                        time.sleep(delay)
                    else:
                        logger.error(f"All {max_retries} retry attempts failed for {func.__name__}")
                        raise AWSServiceError(f"AWS service error after {max_retries} retries: {str(e)}") from e
                        
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        delay = min(base_delay * (2 ** attempt), max_delay)
                        logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {str(e)}. Retrying in {delay:.1f}s...")
                        time.sleep(delay)
                    else:
                        logger.error(f"All {max_retries} retry attempts failed for {func.__name__}")
                        raise
            
            if last_exception:
                raise last_exception
                
        return wrapper
    return decorator

def handle_errors(error_message: str = "Operation failed"):
    """
    Decorator for consistent error handling and logging
    
    Args:
        error_message: Custom error message prefix
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            try:
                return func(*args, **kwargs)
            except InterviewProcessingError:
                # Re-raise our custom exceptions
                raise
            except (ClientError, BotoCoreError) as e:
                error_code = getattr(e, 'response', {}).get('Error', {}).get('Code', 'Unknown')
                logger.error(f"{error_message} in {func.__name__}: AWS Error [{error_code}] {str(e)}")
                raise AWSServiceError(f"AWS service error: {str(e)}") from e
            except Exception as e:
                logger.error(f"{error_message} in {func.__name__}: {str(e)}")
                raise InterviewProcessingError(f"{error_message}: {str(e)}") from e
                
        return wrapper
    return decorator

def validate_uuid(value: str, field_name: str = "UUID") -> str:
    """
    Validate that a string is a valid UUID
    
    Args:
        value: String to validate
        field_name: Name of the field for error messages
        
    Returns:
        The validated UUID string
        
    Raises:
        ValidationError: If the value is not a valid UUID
    """
    import uuid
    
    if not value or not isinstance(value, str):
        raise ValidationError(f"{field_name} must be a non-empty string")
    
    try:
        uuid.UUID(value)
        return value
    except ValueError as e:
        raise ValidationError(f"{field_name} must be a valid UUID: {value}") from e

def validate_s3_path(path: str, field_name: str = "S3 path") -> str:
    """
    Validate that a string is a valid S3 path
    
    Args:
        path: S3 path to validate
        field_name: Name of the field for error messages
        
    Returns:
        The validated S3 path
        
    Raises:
        ValidationError: If the path is not valid
    """
    if not path or not isinstance(path, str):
        raise ValidationError(f"{field_name} must be a non-empty string")
    
    # Basic S3 path validation - could be enhanced
    if not (path.startswith('s3://') or path.startswith('videos/')):
        raise ValidationError(f"{field_name} must be a valid S3 path or video key: {path}")
    
    return path

def validate_interview_data(interview: dict) -> dict:
    """
    Validate interview data structure
    
    Args:
        interview: Interview dictionary from DynamoDB
        
    Returns:
        Validated interview data
        
    Raises:
        ValidationError: If validation fails
    """
    if not interview or not isinstance(interview, dict):
        raise ValidationError("Interview data must be a non-empty dictionary")
    
    required_fields = ['id', 'video_path', 'user_id']
    missing_fields = [field for field in required_fields if not interview.get(field)]
    
    if missing_fields:
        raise ValidationError(f"Interview missing required fields: {', '.join(missing_fields)}")
    
    # Validate field formats
    validate_uuid(interview['id'], 'Interview ID')
    validate_uuid(interview['user_id'], 'User ID')
    validate_s3_path(interview['video_path'], 'Video path')
    
    return interview

def log_processing_metrics(start_time, end_time, interview_id: str, questions_count: int = 0, success: bool = True):
    """
    Log processing metrics for monitoring and debugging
    
    Args:
        start_time: Processing start timestamp
        end_time: Processing end timestamp
        interview_id: Interview ID being processed
        questions_count: Number of questions extracted
        success: Whether processing was successful
    """
    duration = (end_time - start_time).total_seconds()
    status = "SUCCESS" if success else "FAILED"
    
    logger.info(f"PROCESSING_METRICS | Interview: {interview_id} | Status: {status} | "
               f"Duration: {duration:.2f}s | Questions: {questions_count}")

def safe_get_nested(data: dict, keys: list, default: Any = None) -> Any:
    """
    Safely get nested dictionary values
    
    Args:
        data: Dictionary to search
        keys: List of keys to traverse
        default: Default value if not found
        
    Returns:
        The nested value or default
    """
    current = data
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    return current