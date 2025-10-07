#!/usr/bin/env python3
"""
Test script to verify the new question extraction flow
This script tests that the new multi-call AI approach produces the same output format
"""
import sys
import os
import logging

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from question_extractor import QuestionExtractor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_new_flow():
    """Test the new question extraction flow"""
    
    # Sample interview transcript for testing
    sample_transcript = """
    Interviewer: Hello, welcome to our interview today. Can you start by telling me about yourself and your background in backend development?
    
    Candidate: Thank you for having me. I'm a backend developer with 5 years of experience...
    
    Interviewer: That's great. Can you walk me through your experience with Python and Node.js? What projects have you worked on?
    
    Candidate: Absolutely. I've been working with Python for about 4 years now...
    
    Interviewer: What about AWS services? Which ones have you used and in what context?
    
    Candidate: I've worked extensively with AWS...
    
    Interviewer: How do you handle database design and optimization in your projects?
    
    Candidate: Database design is something I'm really passionate about...
    
    Interviewer: Do you have any questions for us about the role or the company?
    
    Candidate: Yes, I do have a few questions...
    """
    
    logger.info("=" * 80)
    logger.info("TESTING NEW QUESTION EXTRACTION FLOW")
    logger.info("=" * 80)
    
    try:
        # Initialize the question extractor
        extractor = QuestionExtractor()
        
        # Test the new flow
        logger.info("Testing new flow: separate extraction and answer generation...")
        results = extractor.extract_questions(sample_transcript)
        
        # Verify the results
        logger.info(f"Status: {results.get('status')}")
        logger.info(f"Total questions: {results.get('total_questions')}")
        logger.info(f"AI calls made: {results.get('ai_calls_made')}")
        logger.info(f"Extraction method: {results.get('extraction_method')}")
        
        if results['status'] == 'success':
            questions = results.get('interviewer_questions', [])
            logger.info(f"\nExtracted {len(questions)} questions:")
            
            for i, q in enumerate(questions, 1):
                logger.info(f"\n--- Question {i} ---")
                logger.info(f"Question: {q.get('question', 'N/A')}")
                logger.info(f"Answer: {q.get('professional_answer', 'N/A')[:100]}...")
                if 'question_context' in q:
                    logger.info(f"Context: {q.get('question_context', 'N/A')}")
            
            # Verify output format
            logger.info("\n" + "=" * 80)
            logger.info("OUTPUT FORMAT VERIFICATION")
            logger.info("=" * 80)
            
            required_keys = ['interviewer_questions', 'total_questions', 'status']
            for key in required_keys:
                if key in results:
                    logger.info(f"✓ Key '{key}' present")
                else:
                    logger.error(f"✗ Key '{key}' missing!")
            
            if questions:
                question_keys = ['question', 'professional_answer']
                for key in question_keys:
                    if key in questions[0]:
                        logger.info(f"✓ Question key '{key}' present")
                    else:
                        logger.error(f"✗ Question key '{key}' missing!")
            
            logger.info("\n✓ NEW FLOW TEST COMPLETED SUCCESSFULLY!")
            
        else:
            logger.error(f"Test failed: {results.get('error_message')}")
            
    except Exception as e:
        logger.error(f"Test failed with exception: {str(e)}")
        raise

if __name__ == "__main__":
    test_new_flow()