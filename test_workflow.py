"""
Test script for the updated interview processing workflow
"""
import sys
import os

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from interview_workflow import InterviewProcessingWorkflow
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

def test_interview_processing():
    """Test the interview processing workflow"""
    try:
        # Test interview ID from your log
        interview_id = "a9ad6877-9767-4642-9171-034d101bd676"
        
        print(f"Testing interview processing for ID: {interview_id}")
        
        # Create workflow instance
        workflow = InterviewProcessingWorkflow()
        
        # Process single interview
        success = workflow.process_single_interview(interview_id)
        
        if success:
            print(f"✓ Successfully processed interview: {interview_id}")
        else:
            print(f"✗ Failed to process interview: {interview_id}")
            
    except Exception as e:
        print(f"Error during test: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_interview_processing()