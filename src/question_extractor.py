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
    
    def extract_questions_only_with_bedrock(self, text: str, model_id: str = "us.anthropic.claude-sonnet-4-20250514-v1:0") -> List[Dict[str, str]]:
        """
        Extract ONLY questions using Amazon Bedrock AI models (first call)
        
        Args:
            text: Input text to analyze
            model_id: Bedrock model ID to use
            
        Returns:
            List of dictionaries with 'question' and optionally 'question_context' keys
        """
        try:
            if not self.aws_clients.bedrock_client:
                logger.warning("Bedrock client not available")
                return []
            
            prompt = f"""
            I am going to provide you with an interview transcript.
            This is a human resource interview for a Backend Developer with Python, Node.js, and AWS experience.
            Please analyze the following interview transcript and extract ONLY the questions asked by the interviewer.

            Instructions:
            - Extract complete questions asked by the interviewer
            - Do not include answers or responses from the interviewee
            - If the question is ambiguous like "Do you have any questions about any of that?" please provide short context to clarify the question, example: "The interviewer talked about the company culture".
            - Do not include confidential information of the interviewee, such as names, locations, or specific project details instead, generalize them (e.g., "a previous project", "a team member", "my current company").
            - Do not include confidential information of the interviewer, such as names, salaries or company names, instead, generalize them (e.g., "the interviewer", "the company", "salary range from x to y").
            - Return ONLY a simple JSON array of objects with the attributes "question" and optionally "question_context".
            - Format: [{{"question": "question 1", "question_context": "Optional question context"}}, {{"question": "question 2"}}]
            - Do not repeat questions
            - Do not include any other text or explanations
            - Do NOT generate answers, only extract questions
            
            Interview transcript:
            {text}
            
            JSON array of interviewer questions:
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
                    
                    # Extract questions from the structured format
                    valid_questions = []
                    for item in questions_data:
                        if isinstance(item, dict) and 'question' in item:
                            if len(item['question'].strip()) > 5:
                                # Create a clean item with required fields and optional question_context
                                clean_item = {
                                    'question': item['question']
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
    
    def generate_professional_answer_with_bedrock(self, question: str, question_context: Optional[str] = None, model_id: str = "us.anthropic.claude-sonnet-4-20250514-v1:0") -> str:
        """
        Generate a professional answer for a single question using Amazon Bedrock AI models
        
        Args:
            question: The interview question to answer
            question_context: Optional context for the question
            model_id: Bedrock model ID to use
            
        Returns:
            Professional answer as string
        """
        try:
            if not self.aws_clients.bedrock_client:
                logger.warning("Bedrock client not available")
                return ""
            
            context_part = f"\nQuestion context: {question_context}" if question_context else ""
            
            prompt = f"""
            I need you to provide a professional answer for an interview question.
            This is a human resource interview for a Backend Developer with Python, Node.js, and AWS experience.
            You will help me to prepare for my interview by providing a professional, well-structured answer.

            Question: {question}{context_part}
            
            Instructions:
            - Provide a professional, comprehensive answer
            - Focus on backend development skills, Python, Node.js, and AWS experience
            - Keep the answer concise but informative
            - Use technical terms appropriately
            - Structure the answer clearly
            - Do not include personal information
            - Return ONLY the answer text, no additional formatting or explanations
            
            Professional answer:
            """
            
            body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 2000,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "temperature": 0.2,
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
                answer = content[0].get('text', '').strip()
                logger.debug(f"Generated answer for question: {question[:50]}...")
                return answer
            else:
                logger.warning("No content found in Bedrock response for answer generation")
                return ""
                
        except ClientError as e:
            logger.error(f"Bedrock answer generation failed: {str(e)}")
            return ""
        except Exception as e:
            logger.error(f"Bedrock answer generation failed: {str(e)}")
            return ""
    
    def extract_questions(self, text: str) -> Dict[str, Any]:
        """
        Complete question extraction pipeline - NEW FLOW: 
        1. First call to extract questions only
        2. Individual calls to generate answers for each question
        
        Args:
            text: Input text to analyze            
        Returns:
            Dict with format: {"interviewer_questions": [{"question": "...", "professional_answer": "..."}, ...]}
        """
        try:
            logger.info("Starting NEW question extraction flow: separate extraction and answer generation")
            
            # Step 1: Extract questions only (first AI call)
            logger.info("Step 1: Extracting questions only from transcript...")
            try:
                raw_questions = self.extract_questions_only_with_bedrock(text)
                
                if not raw_questions:
                    logger.warning("No questions extracted from transcript")
                    return {
                        "interviewer_questions": [],
                        "total_questions": 0,
                        "status": "success"
                    }
                
                logger.info(f"Step 1 completed: Extracted {len(raw_questions)} questions")
                
            except Exception as e:
                logger.error(f"Question extraction failed: {str(e)}")
                return {
                    "interviewer_questions": [],
                    "total_questions": 0,
                    "status": "error",
                    "error_message": f"Question extraction failed: {str(e)}"
                }
            
            # Step 2: Generate professional answers for each question (individual AI calls)
            logger.info(f"Step 2: Generating professional answers for {len(raw_questions)} questions...")
            complete_questions = []
            
            for i, question_data in enumerate(raw_questions, 1):
                try:
                    question = question_data.get('question', '')
                    question_context = question_data.get('question_context', '')
                    
                    logger.info(f"  Generating answer for question {i}/{len(raw_questions)}: {question[:50]}...")
                    
                    # Individual AI call for this question
                    professional_answer = self.generate_professional_answer_with_bedrock(
                        question, 
                        question_context if question_context else None
                    )
                    
                    if professional_answer:
                        complete_question = {
                            'question': question,
                            'professional_answer': professional_answer
                        }
                        # Add question_context if it exists
                        if question_context:
                            complete_question['question_context'] = question_context
                        
                        complete_questions.append(complete_question)
                        logger.debug(f"  ✓ Answer generated for question {i}")
                    else:
                        logger.warning(f"  ✗ Failed to generate answer for question {i}")
                        # Still add the question but with empty answer
                        complete_question = {
                            'question': question,
                            'professional_answer': 'Answer generation failed'
                        }
                        if question_context:
                            complete_question['question_context'] = question_context
                        complete_questions.append(complete_question)
                
                except Exception as e:
                    logger.error(f"Failed to generate answer for question {i}: {str(e)}")
                    # Still add the question but with error message
                    complete_question = {
                        'question': question_data.get('question', 'Unknown question'),
                        'professional_answer': f'Answer generation failed: {str(e)}'
                    }
                    if question_data.get('question_context'):
                        complete_question['question_context'] = question_data['question_context']
                    complete_questions.append(complete_question)
            
            logger.info(f"Step 2 completed: Generated answers for {len(complete_questions)} questions")
            logger.info(f"NEW FLOW completed: Total AI calls made = {1 + len(raw_questions)} (1 for extraction + {len(raw_questions)} for answers)")
            
            result = {
                "interviewer_questions": complete_questions,
                "total_questions": len(complete_questions),
                "status": "success",
                "ai_calls_made": 1 + len(raw_questions),  # Track number of AI calls
                "extraction_method": "separate_calls_flow"  # Track which method was used
            }
            
            logger.info(f"Question extraction completed: {len(complete_questions)} interviewer questions found")
            return result
                
        except Exception as e:
            logger.error(f"Question extraction failed: {str(e)}")
            return {
                "interviewer_questions": [],
                "total_questions": 0,
                "status": "error",
                "error_message": str(e),
                "ai_calls_made": 0,
                "extraction_method": "separate_calls_flow"
            }