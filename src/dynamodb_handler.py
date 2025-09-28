"""
DynamoDB Handler Module
Handles database operations for interviews and questions tables
"""
import json
import logging
import uuid
from datetime import datetime
from typing import Dict, Any, Optional, List
from botocore.exceptions import ClientError
from aws_clients import AWSServiceClients
from config import AWSConfig

logger = logging.getLogger(__name__)

class DynamoDBHandler:
    """Handles DynamoDB operations for interviews and questions"""
    
    def __init__(self):
        self.aws_clients = AWSServiceClients()
        self.config = AWSConfig()
        self.interviews_table = self.config.INTERVIEWS_TABLE_NAME
        self.questions_table = self.config.QUESTIONS_TABLE_NAME
        
    def get_interview_by_id(self, interview_id: str) -> Optional[Dict[str, Any]]:
        """
        Get interview record by ID from DynamoDB
        
        Args:
            interview_id: UUID string of the interview
            
        Returns:
            Interview record or None if not found
        """
        try:
            logger.info(f"Getting interview with ID: {interview_id}")
            
            response = self.aws_clients.dynamodb_client.get_item(
                TableName=self.interviews_table,
                Key={
                    'id': {'S': interview_id}
                }
            )
            
            if 'Item' not in response:
                logger.warning(f"Interview not found: {interview_id}")
                return None
                
            # Convert DynamoDB format to regular dict
            item = self._dynamodb_to_dict(response['Item'])
            logger.info(f"Retrieved interview: {interview_id}")
            return item
            
        except ClientError as e:
            logger.error(f"Error getting interview {interview_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error getting interview {interview_id}: {str(e)}")
            raise
    
    def update_interview_state(self, interview_id: str, state: str) -> bool:
        """
        Update the state of an interview
        
        Args:
            interview_id: UUID string of the interview
            state: New state ('processing', 'completed', 'failed', etc.)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Updating interview {interview_id} state to: {state}")
            
            current_time = datetime.utcnow().isoformat()
            
            response = self.aws_clients.dynamodb_client.update_item(
                TableName=self.interviews_table,
                Key={
                    'id': {'S': interview_id}
                },
                UpdateExpression='SET #state = :state, updated_at = :updated_at',
                ExpressionAttributeNames={
                    '#state': 'state'
                },
                ExpressionAttributeValues={
                    ':state': {'S': state},
                    ':updated_at': {'S': current_time}
                },
                ReturnValues='UPDATED_NEW'
            )
            
            logger.info(f"Successfully updated interview {interview_id} state to {state}")
            return True
            
        except ClientError as e:
            logger.error(f"Error updating interview {interview_id} state: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error updating interview {interview_id} state: {str(e)}")
            return False
    
    def save_questions_batch(self, interview_id: str, user_id: str, questions: List[Dict[str, str]]) -> bool:
        """
        Save multiple questions to DynamoDB in batch
        
        Args:
            interview_id: UUID string of the interview
            user_id: UUID string of the user
            questions: List of question dictionaries with 'context', 'question', 'answer'
            
        Returns:
            True if all questions saved successfully, False otherwise
        """
        try:
            logger.info(f"Saving {len(questions)} questions for interview {interview_id}")
            
            if not questions:
                logger.warning("No questions to save")
                return True
            
            current_time = datetime.utcnow().isoformat()
            
            # Prepare batch write items
            put_requests = []
            
            for question_data in questions:
                question_id = str(uuid.uuid4())
                
                # Validate required fields
                if not all(key in question_data for key in ['question']):
                    logger.error(f"Question missing required fields: {question_data}")
                    continue
                
                item = {
                    'id': {'S': question_id},
                    'interview_id': {'S': interview_id},
                    'user_id': {'S': user_id},
                    'question': {'S': question_data['question']},
                    'global': {'BOOL': False},
                    'created_at': {'S': current_time},
                    'updated_at': {'S': current_time}
                }
                
                # Add optional fields
                if 'context' in question_data and question_data['context']:
                    item['context'] = {'S': question_data['context']}
                    
                if 'answer' in question_data and question_data['answer']:
                    item['answer'] = {'S': question_data['answer']}
                
                if 'question_context' in question_data and question_data['question_context']:
                    item['question_context'] = {'S': question_data['question_context']}
                
                put_requests.append({
                    'PutRequest': {
                        'Item': item
                    }
                })
            
            # DynamoDB batch_write_item can handle max 25 items at a time
            batch_size = 25
            total_saved = 0
            
            for i in range(0, len(put_requests), batch_size):
                batch = put_requests[i:i + batch_size]
                
                response = self.aws_clients.dynamodb_client.batch_write_item(
                    RequestItems={
                        self.questions_table: batch
                    }
                )
                
                # Handle unprocessed items
                unprocessed = response.get('UnprocessedItems', {})
                if unprocessed:
                    logger.warning(f"Some items were not processed in batch: {len(unprocessed)}")
                    # In production, you might want to retry unprocessed items
                
                total_saved += len(batch)
                logger.info(f"Saved batch of {len(batch)} questions ({total_saved}/{len(put_requests)} total)")
            
            logger.info(f"Successfully saved {total_saved} questions for interview {interview_id}")
            return True
            
        except ClientError as e:
            logger.error(f"Error saving questions batch for interview {interview_id}: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error saving questions batch for interview {interview_id}: {str(e)}")
            return False
    
    def save_single_question(self, interview_id: str, user_id: str, question_data: Dict[str, str]) -> Optional[str]:
        """
        Save a single question to DynamoDB
        
        Args:
            interview_id: UUID string of the interview
            user_id: UUID string of the user
            question_data: Dictionary with 'context', 'question', 'answer'
            
        Returns:
            Question ID if successful, None otherwise
        """
        try:
            question_id = str(uuid.uuid4())
            current_time = datetime.utcnow().isoformat()
            
            # Validate required fields
            if 'question' not in question_data:
                logger.error("Question data missing required 'question' field")
                return None
            
            item = {
                'id': {'S': question_id},
                'interview_id': {'S': interview_id},
                'user_id': {'S': user_id},
                'question': {'S': question_data['question']},
                'global': {'BOOL': False},
                'created_at': {'S': current_time},
                'updated_at': {'S': current_time}
            }
            
            # Add optional fields
            if 'context' in question_data and question_data['context']:
                item['context'] = {'S': question_data['context']}
                
            if 'answer' in question_data and question_data['answer']:
                item['answer'] = {'S': question_data['answer']}
            
            if 'question_context' in question_data and question_data['question_context']:
                item['question_context'] = {'S': question_data['question_context']}
            
            self.aws_clients.dynamodb_client.put_item(
                TableName=self.questions_table,
                Item=item
            )
            
            logger.info(f"Successfully saved question {question_id} for interview {interview_id}")
            return question_id
            
        except ClientError as e:
            logger.error(f"Error saving single question for interview {interview_id}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error saving single question for interview {interview_id}: {str(e)}")
            return None
    
    def _dynamodb_to_dict(self, dynamodb_item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert DynamoDB item format to regular Python dictionary
        
        Args:
            dynamodb_item: DynamoDB item with type descriptors
            
        Returns:
            Regular Python dictionary
        """
        result = {}
        
        for key, value in dynamodb_item.items():
            if 'S' in value:  # String
                result[key] = value['S']
            elif 'N' in value:  # Number
                result[key] = float(value['N']) if '.' in value['N'] else int(value['N'])
            elif 'BOOL' in value:  # Boolean
                result[key] = value['BOOL']
            elif 'NULL' in value:  # Null
                result[key] = None
            elif 'SS' in value:  # String Set
                result[key] = value['SS']
            elif 'NS' in value:  # Number Set
                result[key] = [float(n) if '.' in n else int(n) for n in value['NS']]
            elif 'L' in value:  # List
                result[key] = [self._dynamodb_to_dict({'item': item})['item'] for item in value['L']]
            elif 'M' in value:  # Map
                result[key] = self._dynamodb_to_dict(value['M'])
            else:
                # Fallback for unknown types
                result[key] = value
                
        return result