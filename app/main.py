# app/main.py
"""
Main FastAPI Application
PO Management System
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy.orm import Session

# Database and models
from app.database import engine, SessionLocal
from app import models

# Routers
from app.routers import (
    auth,
    files,
    dashboard,
    po,
    acceptance,
    accounts,
    merged_data,
    summary,
    gap_analysis,
    overview_charts,
    upload_history  # ⭐ ADDED
)

# Background task utilities
from app.tasks import task_worker, task_queue, task_workers

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create database tables
models.Base.metadata.create_all(bind=engine)

# Initialize FastAPI app
app = FastAPI(
    title="PO Management API",
    version="1.0.0",
    description="Purchase Order and Acceptance Management System with Upload History Tracking"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Thread pool for background processing
thread_pool = ThreadPoolExecutor(max_workers=4)


# Root endpoint
@app.get("/")
async def root():
    """API root endpoint with available endpoints"""
    return {
        "message": "SIB API is running",
        "version": "1.0.0",
        "status": "operational",
        "endpoints": {
            "auth": "/login",
            "uploads": "/api/upload",
            "upload_acceptance": "/api/upload-acceptance",
            "upload_history": "/api/upload-history", 
            "dashboard": "/api/dashboard-analytics",
            "po_data": "/api/po-data",
            "acceptance": "/api/acceptance-data",
            "merged_data": "/api/merged-data",
            "summary": "/api/summary",
            "gap_analysis": "/api/gap-analysis",
            "overview_charts": "/api/overview-charts",
            "accounts": "/api/accounts"
        },
        "documentation": {
            "swagger": "/docs",
            "redoc": "/redoc"
        }
    }


# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "version": "1.0.0",
        "database": "connected",
        "workers": len(task_workers)
    }


# Include all routers
app.include_router(auth.router)
app.include_router(files.router)
app.include_router(dashboard.router)
app.include_router(po.router)
app.include_router(acceptance.router)
app.include_router(accounts.router)
app.include_router(merged_data.router)
app.include_router(summary.router)
app.include_router(gap_analysis.router)
app.include_router(overview_charts.router)
app.include_router(upload_history.router)  # ⭐ ADDED

logger.info("✅ All routers registered successfully")


@app.on_event("startup")
async def startup_event():
    """Initialize startup tasks"""
    logger.info("🚀 Starting PO Management API...")
    
    # Create database tables
    db = SessionLocal()
    try:
        models.Base.metadata.create_all(bind=engine)
        logger.info("✅ Database tables created/verified")
    except Exception as e:
        logger.error(f"❌ Error during database initialization: {str(e)}")
    finally:
        db.close()
    
    # Start background workers
    logger.info("🔧 Starting background task workers...")
    for i in range(2):
        worker = asyncio.create_task(task_worker())
        task_workers.append(worker)
        logger.info(f"✅ Worker {i+1} started")
    
    logger.info("🎉 PO Management API started successfully with 2 background workers")
    logger.info("📚 API Documentation available at /docs")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    logger.info("🛑 Shutting down PO Management API...")
    
    # Signal workers to shutdown
    logger.info("⏳ Signaling workers to shutdown...")
    for _ in range(len(task_workers)):
        await task_queue.put(("shutdown", None))
    
    # Wait for all tasks to complete
    logger.info("⏳ Waiting for pending tasks to complete...")
    await task_queue.join()
    
    # Cancel worker tasks
    logger.info("⏳ Canceling worker tasks...")
    for worker in task_workers:
        worker.cancel()
    
    # Wait for workers to finish
    await asyncio.gather(*task_workers, return_exceptions=True)
    
    # Shutdown thread pool
    logger.info("⏳ Shutting down thread pool...")
    thread_pool.shutdown(wait=True)
    
    logger.info("✅ PO Management API shutdown complete")


# Exception handlers
@app.exception_handler(404)
async def not_found_handler(request, exc):
    """Custom 404 handler"""
    return {
        "error": "Not Found",
        "message": f"The endpoint {request.url.path} does not exist",
        "status_code": 404
    }


@app.exception_handler(500)
async def internal_error_handler(request, exc):
    """Custom 500 handler"""
    logger.error(f"Internal server error: {str(exc)}")
    return {
        "error": "Internal Server Error",
        "message": "An unexpected error occurred. Please try again later.",
        "status_code": 500
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )