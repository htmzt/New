# app/tasks.py
"""
Background task processing for file uploads
"""
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from app.database import SessionLocal
from app.services.file_service import FileService

logger = logging.getLogger(__name__)

# Global task queue and workers list
task_queue = asyncio.Queue()
task_workers = []
thread_pool = ThreadPoolExecutor(max_workers=4)


async def task_worker():
    """Background worker that processes tasks from the queue"""
    logger.info("Starting background task worker")
    while True:
        try:
            task_type, task_data = await task_queue.get()
            
            if task_type == "shutdown":
                logger.info("Worker shutting down")
                break
                
            if task_type == "po_process":
                # Now handles 3 parameters: file_path, user_id, filename
                file_path, user_id, filename = task_data
                await process_po_file_async(file_path, user_id, filename)
            elif task_type == "acceptance_process":
                # Now handles 3 parameters: file_path, user_id, filename
                file_path, user_id, filename = task_data
                await process_acceptance_file_async(file_path, user_id, filename)
                
            task_queue.task_done()
        except Exception as e:
            logger.error(f"Error in task worker: {str(e)}")


async def process_po_file_async(file_path: str, user_id: str, filename: str):
    """Process PO file in background"""
    try:
        logger.info(f"Starting PO file processing for user {user_id}: {filename}")
        
        db = SessionLocal()
        file_service = FileService(db)
        
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            thread_pool, 
            file_service.process_po_file,
            file_path, 
            user_id,
            filename
        )
        
        if result.get('success'):
            logger.info(f"✅ PO file processing completed successfully for user {user_id}")
            logger.info(f"   Upload ID: {result.get('upload_id')}")
            logger.info(f"   Stats: {result.get('stats')}")
        else:
            logger.error(f"❌ PO file processing failed for user {user_id}: {result.get('message')}")
            
        db.close()
    except Exception as e:
        logger.error(f"💥 Exception in PO file processing for user {user_id}: {str(e)}")


async def process_acceptance_file_async(file_path: str, user_id: str, filename: str):
    """Process Acceptance file in background"""
    try:
        logger.info(f"Starting Acceptance file processing for user {user_id}: {filename}")
        
        db = SessionLocal()
        file_service = FileService(db)
        
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            thread_pool, 
            file_service.process_acceptance_file,
            file_path, 
            user_id,
            filename
        )
        
        if result.get('success'):
            logger.info(f"✅ Acceptance file processing completed successfully for user {user_id}")
            logger.info(f"   Upload ID: {result.get('upload_id')}")
            logger.info(f"   Stats: {result.get('stats')}")
        else:
            logger.error(f"❌ Acceptance file processing failed for user {user_id}: {result.get('message')}")
            
        db.close()
    except Exception as e:
        logger.error(f"💥 Exception in Acceptance file processing for user {user_id}: {str(e)}")