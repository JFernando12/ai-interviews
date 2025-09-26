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
        
        # Common question patterns
        self.question_patterns = [
            r'\b[Ww]hat\s+(?:is|are|was|were|do|does|did|will|would|should|could|can)\b[^?]*\?',
            r'\b[Ww]here\s+(?:is|are|was|were|do|does|did|will|would|should|could|can)\b[^?]*\?',
            r'\b[Ww]hen\s+(?:is|are|was|were|do|does|did|will|would|should|could|can)\b[^?]*\?',
            r'\b[Ww]hy\s+(?:is|are|was|were|do|does|did|will|would|should|could|can)\b[^?]*\?',
            r'\b[Hh]ow\s+(?:is|are|was|were|do|does|did|will|would|should|could|can)\b[^?]*\?',
            r'\b[Ww]ho\s+(?:is|are|was|were|do|does|did|will|would|should|could|can)\b[^?]*\?',
            r'\b[Ww]hich\s+(?:is|are|was|were|do|does|did|will|would|should|could|can)\b[^?]*\?',
            r'\b[Dd]o\s+you\b[^?]*\?',
            r'\b[Dd]oes\s+(?:he|she|it|this|that)\b[^?]*\?',
            r'\b[Dd]id\s+you\b[^?]*\?',
            r'\b[Ww]ill\s+you\b[^?]*\?',
            r'\b[Cc]an\s+you\b[^?]*\?',
            r'\b[Cc]ould\s+you\b[^?]*\?',
            r'\b[Ww]ould\s+you\b[^?]*\?',
            r'\b[Ss]hould\s+(?:I|we)\b[^?]*\?',
            r'\b[Ii]s\s+(?:it|this|that|there)\b[^?]*\?',
            r'\b[Aa]re\s+(?:you|they|there)\b[^?]*\?',
            r'[A-Z][^.!?]*\?'  # Any sentence ending with question mark
        ]
    
    def extract_questions_regex(self, text: str) -> List[Dict[str, Any]]:
        """
        Extract questions using regex patterns
        
        Args:
            text: Input text to analyze
            
        Returns:
            List of extracted questions with metadata
        """
        questions = []
        seen_questions = set()  # To avoid duplicates
        
        for pattern in self.question_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE | re.DOTALL)
            
            for match in matches:
                question_text = match.group().strip()
                
                # Clean up the question
                question_text = re.sub(r'\s+', ' ', question_text)  # Normalize whitespace
                question_text = question_text.strip()
                
                # Avoid duplicates
                question_lower = question_text.lower()
                if question_lower not in seen_questions and len(question_text) > 5:
                    seen_questions.add(question_lower)
                    
                    questions.append({
                        'text': question_text,
                        'start_position': match.start(),
                        'end_position': match.end(),
                        'method': 'regex',
                        'confidence': 0.8,  # Static confidence for regex
                        'pattern_matched': pattern
                    })
        
        # Sort by position in text
        questions.sort(key=lambda x: x['start_position'])
        
        logger.info(f"Extracted {len(questions)} questions using regex patterns")
        return questions
    
    def analyze_text_with_comprehend(self, text: str) -> Dict[str, Any]:
        """
        Analyze text using AWS Comprehend to understand structure
        
        Args:
            text: Input text to analyze
            
        Returns:
            Comprehend analysis results
        """
        try:
            # Split text into chunks if it's too long (Comprehend has limits)
            max_chars = 5000
            text_chunks = [text[i:i+max_chars] for i in range(0, len(text), max_chars)]
            
            analysis_results = {
                'entities': [],
                'key_phrases': [],
                'sentiment': None,
                'dominant_language': None
            }
            
            for chunk in text_chunks[:3]:  # Limit to first 3 chunks to avoid costs
                # Detect dominant language
                if analysis_results['dominant_language'] is None:
                    lang_response = self.aws_clients.comprehend_client.detect_dominant_language(
                        Text=chunk
                    )
                    analysis_results['dominant_language'] = lang_response['Languages'][0]['LanguageCode']
                
                language_code = analysis_results['dominant_language']
                
                # Extract entities
                entities_response = self.aws_clients.comprehend_client.detect_entities(
                    Text=chunk,
                    LanguageCode=language_code
                )
                analysis_results['entities'].extend(entities_response['Entities'])
                
                # Extract key phrases
                phrases_response = self.aws_clients.comprehend_client.detect_key_phrases(
                    Text=chunk,
                    LanguageCode=language_code
                )
                analysis_results['key_phrases'].extend(phrases_response['KeyPhrases'])
                
                # Analyze sentiment (only for first chunk)
                if analysis_results['sentiment'] is None:
                    sentiment_response = self.aws_clients.comprehend_client.detect_sentiment(
                        Text=chunk,
                        LanguageCode=language_code
                    )
                    analysis_results['sentiment'] = sentiment_response
            
            logger.info("Text analysis completed using AWS Comprehend")
            return analysis_results
            
        except ClientError as e:
            logger.error(f"AWS Comprehend analysis failed: {str(e)}")
            return {'error': str(e)}
        except Exception as e:
            logger.error(f"Text analysis failed: {str(e)}")
            return {'error': str(e)}
    
    def extract_questions_with_bedrock(self, text: str, model_id: str = "us.anthropic.claude-sonnet-4-20250514-v1:0") -> List[str]:
        """
        Extract questions using Amazon Bedrock AI models
        
        Args:
            text: Input text to analyze
            model_id: Bedrock model ID to use
            
        Returns:
            List of extracted question strings
        """
        try:
            if not self.aws_clients.bedrock_client:
                logger.warning("Bedrock client not available")
                return []
            
            prompt = f"""
            Please analyze the following interview transcript and extract ONLY the questions asked by the interviewer.
            
            Instructions:
            - Extract complete questions asked by the interviewer
            - Do not include answers or responses from the interviewee
            - Return ONLY a simple JSON array of question strings
            - Format: ["question 1", "question 2", "question 3"]
            - Do not include any other text or explanations
            
            Interview transcript:
            {text}
            
            JSON array of interviewer questions:
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
                    questions_list = json.loads(completion[json_start:json_end])
                    
                    # Filter out empty questions
                    valid_questions = [q for q in questions_list if isinstance(q, str) and len(q.strip()) > 5]
                    
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
    
    def enhance_questions_with_context(self, questions: List[Dict], transcript_data: Dict) -> List[Dict]:
        """
        Enhance questions with context from transcript data
        
        Args:
            questions: List of extracted questions
            transcript_data: Full transcript data with speaker information
            
        Returns:
            Enhanced questions with additional context
        """
        try:
            detailed_transcript = transcript_data.get('detailed_transcript', [])
            full_text = transcript_data.get('full_transcript', '')
            
            for question in questions:
                question_text = question['text'].lower()
                
                # Find the speaker and timestamp context
                for segment in detailed_transcript:
                    segment_text = segment.get('text', '').lower()
                    if question_text in segment_text or any(word in segment_text for word in question_text.split()[:3]):
                        question['speaker'] = segment.get('speaker')
                        question['timestamp'] = segment.get('start_time')
                        break
                
                # Add surrounding context
                if 'start_position' in question:
                    start_pos = max(0, question['start_position'] - 200)
                    end_pos = min(len(full_text), question['end_position'] + 200)
                    question['surrounding_context'] = full_text[start_pos:end_pos]
            
            logger.info(f"Enhanced {len(questions)} questions with context")
            return questions
            
        except Exception as e:
            logger.error(f"Failed to enhance questions with context: {str(e)}")
            return questions
    
    def extract_questions(self, 
                         text: str, 
                         transcript_data: Optional[Dict] = None,
                         use_bedrock: bool = True,
                         use_comprehend: bool = False) -> Dict[str, Any]:
        """
        Complete question extraction pipeline - simplified to use only Bedrock
        
        Args:
            text: Input text to analyze
            transcript_data: Optional transcript data (not used in simplified version)
            use_bedrock: Whether to use Bedrock AI for extraction (default: True)
            use_comprehend: Whether to use Comprehend (disabled in simplified version)
            
        Returns:
            Simple format: {"interviewer_questions": ["question1", "question2", ...]}
        """
        try:
            logger.info("Starting simplified question extraction using Bedrock AI")
            
            # Use only Bedrock AI extraction
            if use_bedrock:
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
            else:
                logger.warning("Bedrock is disabled - no questions extracted")
                return {
                    "interviewer_questions": [],
                    "total_questions": 0,
                    "status": "error",
                    "error_message": "Bedrock AI is required but disabled"
                }
            
        except Exception as e:
            logger.error(f"Question extraction failed: {str(e)}")
            return {
                "interviewer_questions": [],
                "total_questions": 0,
                "status": "error",
                "error_message": str(e)
            }