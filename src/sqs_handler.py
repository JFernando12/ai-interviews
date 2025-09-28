"""
SQS Message Handler Module
Handles receiving and processing messages from the app-interviews-sqs-videos queue
"""
import json
import logging
import time
from typing import Dict, Any, Optional, List
from botocore.exceptions import ClientError
from aws_clients import AWSServiceClients
from config import AWSConfig

logger = logging.getLogger(__name__)

class SQSHandler:
    """Handles SQS message polling and processing"""
    
    def __init__(self):
        self.aws_clients = AWSServiceClients()
        self.config = AWSConfig()
        self.queue_url = self.config.SQS_QUEUE_URL
        
    def poll_messages(self, max_messages: int = 1, wait_time_seconds: int = 20) -> List[Dict[str, Any]]:
        """
        Poll SQS queue for messages
        
        Args:
            max_messages: Maximum number of messages to receive
            wait_time_seconds: Long polling wait time
            
        Returns:
            List of message dictionaries
        """
        try:
            logger.info(f"Polling SQS queue: {self.queue_url}")
            
            response = self.aws_clients.sqs_client.receive_message(
                QueueUrl=self.queue_url,
                AttributeNames=['All'],
                MessageAttributeNames=['All'],
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_time_seconds,
                VisibilityTimeout=300  # 5 minutes to process the message
            )
            
            messages = response.get('Messages', [])
            logger.info(f"Received {len(messages)} message(s) from SQS")
            
            return messages
            
        except ClientError as e:
            logger.error(f"Error polling SQS messages: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error polling SQS messages: {str(e)}")
            raise
    
    def parse_message_body(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse the message body to extract interview_id
        
        Args:
            message: SQS message dictionary
            
        Returns:
            Parsed message body or None if parsing fails
        """
        try:
            body = message.get('Body', '')
            parsed_body = json.loads(body)
            
            # Validate required fields
            if 'interview_id' not in parsed_body:
                logger.error("Message missing required field: interview_id")
                return None
                
            logger.info(f"Parsed message: interview_id={parsed_body['interview_id']}")
            return parsed_body
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message body as JSON: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error parsing message body: {str(e)}")
            return None
    
    def delete_message(self, message: Dict[str, Any]) -> bool:
        """
        Delete a processed message from the queue
        
        Args:
            message: SQS message dictionary
            
        Returns:
            True if successful, False otherwise
        """
        try:
            receipt_handle = message.get('ReceiptHandle')
            if not receipt_handle:
                logger.error("Message missing ReceiptHandle")
                return False
                
            self.aws_clients.sqs_client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle
            )
            
            logger.info("Message deleted successfully from SQS queue")
            return True
            
        except ClientError as e:
            logger.error(f"Error deleting SQS message: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error deleting SQS message: {str(e)}")
            return False
    
    def change_message_visibility(self, message: Dict[str, Any], visibility_timeout: int) -> bool:
        """
        Change the visibility timeout of a message (useful for extending processing time)
        
        Args:
            message: SQS message dictionary
            visibility_timeout: New visibility timeout in seconds
            
        Returns:
            True if successful, False otherwise
        """
        try:
            receipt_handle = message.get('ReceiptHandle')
            if not receipt_handle:
                logger.error("Message missing ReceiptHandle")
                return False
                
            self.aws_clients.sqs_client.change_message_visibility(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle,
                VisibilityTimeout=visibility_timeout
            )
            
            logger.info(f"Message visibility timeout changed to {visibility_timeout} seconds")
            return True
            
        except ClientError as e:
            logger.error(f"Error changing message visibility: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error changing message visibility: {str(e)}")
            return False
    
    def run_message_processor(self, processor_callback):
        """
        Run continuous message processing loop
        
        Args:
            processor_callback: Function to call for each message. 
                               Should accept (interview_id, message) and return True if successful
        """
        logger.info("Starting SQS message processor loop")
        
        while True:
            try:
                # Poll for messages
                messages = self.poll_messages(max_messages=1, wait_time_seconds=20)
                
                if not messages:
                    logger.debug("No messages received, continuing to poll...")
                    continue
                
                for message in messages:
                    # Parse message body
                    parsed_body = self.parse_message_body(message)
                    if not parsed_body:
                        # Failed to parse, delete the message to avoid reprocessing
                        logger.warning("Deleting unparseable message")
                        self.delete_message(message)
                        continue
                    
                    interview_id = parsed_body['interview_id']
                    
                    # Extend visibility timeout for long processing
                    self.change_message_visibility(message, 600)  # 10 minutes
                    
                    try:
                        # Process the message using callback
                        success = processor_callback(interview_id, message)
                        
                        if success:
                            # Delete message if processing was successful
                            self.delete_message(message)
                            logger.info(f"Successfully processed interview_id: {interview_id}")
                        else:
                            logger.error(f"Processing failed for interview_id: {interview_id}")
                            # Message will become visible again after timeout
                            
                    except Exception as e:
                        logger.error(f"Error processing message for interview_id {interview_id}: {str(e)}")
                        # Message will become visible again after timeout
                        
            except KeyboardInterrupt:
                logger.info("Received interrupt signal, stopping message processor")
                break
            except Exception as e:
                logger.error(f"Error in message processor loop: {str(e)}")
                # Wait before retrying
                time.sleep(5)