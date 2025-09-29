"""
Question Extraction Module using AWS AI Services
Extracts questions from transcribed text using AWS Comprehend and Amazon Bedrock
"""
import re
import json
import logging
from typing import List, Dict, Any, Optional
from botocore.exceptions import ClientError
from aws_clients import AWSServiceClients
from config import AWSConfig

logger = logging.getLogger(__name__)

class QuestionExtractor:
    """Extracts questions from text using various AWS AI services"""
    
    def __init__(self):
        self.aws_clients = AWSServiceClients()
        self.config = AWSConfig()
    
    def extract_questions_with_bedrock(self, text: str, model_id: str = "us.anthropic.claude-sonnet-4-20250514-v1:0") -> List[Dict[str, str]]:
        """
        Extract questions using Amazon Bedrock AI models
        
        Args:
            text: Input text to analyze
            model_id: Bedrock model ID to use
            
        Returns:
            List of dictionaries with 'question' and 'professional_answer' keys
        """
        try:
            if not self.aws_clients.bedrock_client:
                logger.warning("Bedrock client not available")
                return []
            
            prompt = f"""
            I am going to provide you with an interview transcript.
            This is a human resource interview for a Backend Developer with Python, Node.js, and AWS experience.
            Please analyze the following interview transcript and extract the questions asked by the interviewer.

            After extracting the questions, I will need you to answer in a professional manner, you will help me to prepare for my interview.
            
            Instructions:
            - Extract complete questions asked by the interviewer
            - Do not include answers or responses from the interviewee
            - If the question is ambiguous like "Do you have any questions about any of that?" please provide short context to clarify the question, example: "The interviewer talked about the company culture".
            - Do not include confidential information of the interviewee, such as names, locations, or specific project details instead, generalize them (e.g., "a previous project", "a team member", "my current company").
            - Do not include confidential information of the interviewer, such as names, salaries or company names, instead, generalize them (e.g., "the interviewer", "the company", "salary range from x to y").
            - Return ONLY a simple JSON array of objects with the attributes "question", "professional_answer" and optionally "question_context".
            - Format: [{{"question": "question 1", "professional_answer": "answer 1", "question_context": "Optional question context"}}, {{"question": "question 2", "professional_answer": "answer 2"}}]
            - Do not repeat questions
            - Do not include any other text or explanations
            
            Interview transcript:
            {text}
            
            JSON array of interviewer questions and professional answers:
            """
            
            body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 10000,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "temperature": 0.1,
                "top_p": 0.9
            }
            
            response = self.aws_clients.bedrock_client.invoke_model(
                modelId=model_id,
                body=json.dumps(body),
                contentType="application/json",
                accept="application/json"
            )
            
            response_body = json.loads(response['body'].read())
            content = response_body.get('content', [])
            
            if content and len(content) > 0:
                completion = content[0].get('text', '')
            else:
                logger.warning("No content found in Bedrock response")
                return []
            
            # Try to parse JSON from the response
            try:
                # Look for JSON array in the response
                json_start = completion.find('[')
                json_end = completion.rfind(']') + 1
                if json_start >= 0 and json_end > json_start:
                    questions_data = json.loads(completion[json_start:json_end])
                    
                    # Extract questions and answers from the structured format
                    valid_questions = []
                    for item in questions_data:
                        if isinstance(item, dict) and 'question' in item and 'professional_answer' in item:
                            if len(item['question'].strip()) > 5:
                                # Create a clean item with required fields and optional question_context
                                clean_item = {
                                    'question': item['question'],
                                    'professional_answer': item['professional_answer']
                                }
                                # Add question_context if it exists and is not empty
                                if 'question_context' in item and item['question_context'] and item['question_context'].strip():
                                    clean_item['question_context'] = item['question_context']
                                
                                valid_questions.append(clean_item)
                    
                    logger.info(f"Extracted {len(valid_questions)} questions using Bedrock AI")
                    return valid_questions
                else:
                    logger.warning("No valid JSON array found in Bedrock response")
                    return []
                    
            except json.JSONDecodeError as e:
                logger.warning(f"Could not parse Bedrock response as JSON: {str(e)}")
                logger.debug(f"Response content: {completion}")
                return []
            
        except ClientError as e:
            logger.error(f"Bedrock question extraction failed: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"Bedrock question extraction failed: {str(e)}")
            return []
    
    def extract_questions(self, text: str) -> Dict[str, Any]:
        """
        Complete question extraction pipeline - simplified to use only Bedrock
        
        Args:
            text: Input text to analyze            
        Returns:
            Dict with format: {"interviewer_questions": [{"question": "...", "professional_answer": "..."}, ...]}
        """
        try:
            logger.info("Starting simplified question extraction using Bedrock AI")
            
            try:
                questions = self.extract_questions_with_bedrock(text)
                
                result = {
                    "interviewer_questions": questions,
                    "total_questions": len(questions),
                    "status": "success"
                }
                
                logger.info(f"Question extraction completed: {len(questions)} interviewer questions found")
                return result
                
            except Exception as e:
                logger.error(f"Bedrock extraction failed: {str(e)}")
                return {
                    "interviewer_questions": [],
                    "total_questions": 0,
                    "status": "error",
                    "error_message": str(e)
                }
        
        except Exception as e:
            logger.error(f"Question extraction failed: {str(e)}")
            return {
                "interviewer_questions": [],
                "total_questions": 0,
                "status": "error",
                "error_message": str(e)
            }