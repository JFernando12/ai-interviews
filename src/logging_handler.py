"""
Logging and Error Handling Module
Provides comprehensive logging, monitoring, and error handling for the AWS pipeline
"""
import logging
import traceback
import functools
from datetime import datetime
from typing import Any, Callable, Dict, Optional
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
import json
import os

class PipelineLogger:
    """Enhanced logging class with AWS service monitoring"""
    
    def __init__(self, name: str = "ai_interviews_pipeline", log_level: str = "INFO"):
        self.name = name
        self.logger = logging.getLogger(name)
        
        # Set log level
        numeric_level = getattr(logging, log_level.upper(), logging.INFO)
        self.logger.setLevel(numeric_level)
        
        # Clear existing handlers to avoid duplicates
        self.logger.handlers.clear()
        
        # Create formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
        )
        simple_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        
        # File handler for detailed logs
        file_handler = logging.FileHandler('pipeline_detailed.log')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(detailed_formatter)
        self.logger.addHandler(file_handler)
        
        # Console handler for user-friendly output
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(simple_formatter)
        self.logger.addHandler(console_handler)
        
        # Error file handler for errors only
        error_handler = logging.FileHandler('pipeline_errors.log')
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(detailed_formatter)
        self.logger.addHandler(error_handler)
        
        # AWS operations log
        aws_handler = logging.FileHandler('aws_operations.log')
        aws_handler.setLevel(logging.INFO)
        aws_handler.setFormatter(detailed_formatter)
        
        # Create AWS-specific logger
        self.aws_logger = logging.getLogger(f"{name}.aws")
        self.aws_logger.addHandler(aws_handler)
        self.aws_logger.setLevel(logging.INFO)
    
    def log_aws_operation(self, service: str, operation: str, details: Dict[str, Any]):
        """Log AWS service operations with structured data"""
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'service': service,
            'operation': operation,
            'details': details
        }
        self.aws_logger.info(json.dumps(log_entry))
    
    def log_pipeline_step(self, step_name: str, status: str, details: Optional[Dict] = None):
        """Log pipeline step with structured information"""
        message = f"Pipeline Step: {step_name} - Status: {status}"
        if details:
            message += f" - Details: {json.dumps(details)}"
        
        if status.lower() == 'success':
            self.logger.info(message)
        elif status.lower() == 'error':
            self.logger.error(message)
        else:
            self.logger.info(message)

class AWSErrorHandler:
    """Specialized error handler for AWS service errors"""
    
    @staticmethod
    def handle_aws_error(error: Exception, service: str, operation: str) -> Dict[str, Any]:
        """
        Handle AWS-specific errors and provide user-friendly messages
        
        Args:
            error: The exception that occurred
            service: AWS service name
            operation: Operation being performed
            
        Returns:
            Error details dictionary
        """
        error_details = {
            'service': service,
            'operation': operation,
            'error_type': type(error).__name__,
            'timestamp': datetime.now().isoformat(),
            'suggestions': []
        }
        
        if isinstance(error, NoCredentialsError):
            error_details.update({
                'category': 'authentication',
                'message': 'AWS credentials not found or invalid',
                'user_message': 'Please check your AWS credentials configuration',
                'suggestions': [
                    'Verify AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env file',
                    'Check AWS credentials file (~/.aws/credentials)',
                    'Verify IAM user has necessary permissions'
                ]
            })
        
        elif isinstance(error, ClientError):
            error_code = error.response.get('Error', {}).get('Code', 'Unknown')
            error_message = error.response.get('Error', {}).get('Message', str(error))
            
            error_details.update({
                'category': 'aws_service',
                'aws_error_code': error_code,
                'message': error_message,
                'response': error.response
            })
            
            # Handle specific AWS error codes
            if error_code == 'AccessDenied':
                error_details.update({
                    'user_message': f'Access denied for {service} service',
                    'suggestions': [
                        f'Check IAM permissions for {service} service',
                        'Verify your AWS user has the required policies attached',
                        'Check if the resource exists and you have access to it'
                    ]
                })
            
            elif error_code == 'BucketNotExists' or error_code == 'NoSuchBucket':
                error_details.update({
                    'user_message': 'S3 bucket does not exist or is not accessible',
                    'suggestions': [
                        'Verify the bucket name in your configuration',
                        'Check if the bucket exists in the correct AWS region',
                        'Verify you have access to the bucket'
                    ]
                })
            
            elif error_code == 'InvalidParameterValue':
                error_details.update({
                    'user_message': f'Invalid parameter provided to {service}',
                    'suggestions': [
                        'Check the input parameters for the operation',
                        'Verify file formats and sizes meet AWS service requirements',
                        'Check AWS service documentation for parameter constraints'
                    ]
                })
            
            elif error_code == 'LimitExceeded' or error_code == 'ThrottlingException':
                error_details.update({
                    'user_message': f'{service} service limits exceeded',
                    'suggestions': [
                        'Wait before retrying the operation',
                        'Check your AWS service usage limits',
                        'Consider implementing exponential backoff retry logic'
                    ]
                })
            
            else:
                error_details.update({
                    'user_message': f'{service} service error: {error_message}',
                    'suggestions': [
                        f'Check AWS {service} service documentation',
                        'Verify your AWS region configuration',
                        'Check the AWS service status page'
                    ]
                })
        
        elif isinstance(error, BotoCoreError):
            error_details.update({
                'category': 'boto_core',
                'message': str(error),
                'user_message': 'AWS SDK connection or configuration error',
                'suggestions': [
                    'Check your internet connection',
                    'Verify AWS region configuration',
                    'Check if AWS services are available in your region'
                ]
            })
        
        else:
            error_details.update({
                'category': 'general',
                'message': str(error),
                'user_message': f'Unexpected error in {service} operation',
                'suggestions': [
                    'Check the application logs for more details',
                    'Verify your input data and parameters',
                    'Try the operation again'
                ]
            })
        
        return error_details

def aws_error_handler(service: str, operation: str):
    """
    Decorator for handling AWS service errors consistently
    
    Args:
        service: AWS service name
        operation: Operation being performed
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = PipelineLogger()
            
            try:
                # Log operation start
                logger.log_aws_operation(
                    service=service,
                    operation=operation,
                    details={'status': 'started', 'args_count': len(args), 'kwargs': list(kwargs.keys())}
                )
                
                # Execute the function
                result = func(*args, **kwargs)
                
                # Log operation success
                logger.log_aws_operation(
                    service=service,
                    operation=operation,
                    details={'status': 'success', 'result_type': type(result).__name__}
                )
                
                return result
                
            except Exception as e:
                # Handle the error
                error_details = AWSErrorHandler.handle_aws_error(e, service, operation)
                
                # Log the error
                logger.logger.error(f"AWS {service} {operation} failed: {error_details['message']}")
                logger.log_aws_operation(
                    service=service,
                    operation=operation,
                    details={
                        'status': 'error',
                        'error_details': error_details,
                        'traceback': traceback.format_exc()
                    }
                )
                
                # Re-raise with enhanced error information
                enhanced_error = Exception(f"{error_details['user_message']} | Original: {str(e)}")
                enhanced_error.aws_error_details = error_details
                raise enhanced_error
        
        return wrapper
    return decorator

def pipeline_step_logger(step_name: str):
    """
    Decorator for logging pipeline steps
    
    Args:
        step_name: Name of the pipeline step
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = PipelineLogger()
            start_time = datetime.now()
            
            try:
                logger.log_pipeline_step(step_name, 'started')
                result = func(*args, **kwargs)
                
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                
                logger.log_pipeline_step(
                    step_name, 
                    'success', 
                    {'duration_seconds': duration}
                )
                
                return result
                
            except Exception as e:
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                
                logger.log_pipeline_step(
                    step_name, 
                    'error', 
                    {
                        'duration_seconds': duration,
                        'error': str(e),
                        'traceback': traceback.format_exc()
                    }
                )
                
                raise
        
        return wrapper
    return decorator

class ResourceMonitor:
    """Monitor AWS resource usage and costs"""
    
    def __init__(self):
        self.operations_log = []
    
    def log_operation(self, service: str, operation: str, resource_usage: Dict[str, Any]):
        """Log resource usage for an operation"""
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'service': service,
            'operation': operation,
            'resource_usage': resource_usage
        }
        self.operations_log.append(log_entry)
    
    def estimate_costs(self) -> Dict[str, float]:
        """Estimate costs based on logged operations"""
        # This is a simplified cost estimation
        # In production, you'd integrate with AWS Cost Explorer API
        
        cost_estimates = {
            's3_storage': 0.0,
            's3_requests': 0.0,
            'transcribe_minutes': 0.0,
            'comprehend_requests': 0.0,
            'bedrock_tokens': 0.0
        }
        
        for log_entry in self.operations_log:
            service = log_entry['service']
            usage = log_entry['resource_usage']
            
            if service == 's3':
                # Rough S3 pricing estimates (US East)
                if 'bytes_uploaded' in usage:
                    gb_uploaded = usage['bytes_uploaded'] / (1024**3)
                    cost_estimates['s3_storage'] += gb_uploaded * 0.023  # $0.023 per GB
                
                if 'requests' in usage:
                    cost_estimates['s3_requests'] += usage['requests'] * 0.0004  # $0.4 per 1000 requests
            
            elif service == 'transcribe':
                if 'duration_minutes' in usage:
                    cost_estimates['transcribe_minutes'] += usage['duration_minutes'] * 0.024  # $0.024 per minute
            
            elif service == 'comprehend':
                if 'characters' in usage:
                    cost_estimates['comprehend_requests'] += (usage['characters'] / 100) * 0.0001  # Per 100 chars
        
        return cost_estimates
    
    def generate_usage_report(self) -> Dict[str, Any]:
        """Generate a usage report"""
        total_operations = len(self.operations_log)
        
        if total_operations == 0:
            return {'message': 'No operations logged yet'}
        
        services_used = set(log['service'] for log in self.operations_log)
        cost_estimates = self.estimate_costs()
        total_estimated_cost = sum(cost_estimates.values())
        
        return {
            'total_operations': total_operations,
            'services_used': list(services_used),
            'cost_estimates': cost_estimates,
            'total_estimated_cost_usd': round(total_estimated_cost, 4),
            'operations_by_service': {
                service: sum(1 for log in self.operations_log if log['service'] == service)
                for service in services_used
            }
        }