"""
Persistent Webhook Processor Service
Keeps PySpark session alive for fast Delta Lake writes
"""

import os
import json
import time
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PersistentWebhookProcessor:
    """
    Long-running service that maintains a PySpark session for fast webhook processing
    """
    
    def __init__(self):
        self.spark = None
        self.delta_manager = None
        self.setup_spark_session()
        
    def setup_spark_session(self):
        """Initialize persistent Spark session"""
        try:
            # Import here to avoid startup delays
            import sys
            sys.path.append('/opt/airflow/dags')
            from utils.true_delta_manager import TrueDeltaLakeManager
            
            logger.info("🚀 Starting persistent PySpark session...")
            self.delta_manager = TrueDeltaLakeManager()
            self.spark = self.delta_manager.spark
            logger.info("✅ PySpark session ready for webhook processing")
            
        except Exception as e:
            logger.error(f"Failed to initialize PySpark session: {e}")
            raise
    
    def process_webhook_files(self) -> Dict[str, int]:
        """
        Process all pending webhook files to Delta Lake
        Returns processing statistics
        """
        try:
            pending_dir = Path("/opt/airflow/data/pending_webhooks")
            
            if not pending_dir.exists():
                return {"status": "success", "files_processed": 0, "message": "No pending directory"}
            
            # Get all webhook files
            webhook_files = list(pending_dir.glob("*.json"))
            
            if not webhook_files:
                return {"status": "success", "files_processed": 0, "message": "No files to process"}
            
            # Limit batch size for memory safety
            batch_files = webhook_files[:500]
            logger.info(f"Processing {len(batch_files)} webhook files")
            
            # Read webhook data
            webhook_data = []
            processed_files = []
            
            for webhook_file in batch_files:
                try:
                    with open(webhook_file, 'r') as f:
                        webhook_record = json.load(f)
                        webhook_data.append(webhook_record)
                        processed_files.append(webhook_file)
                except Exception as e:
                    logger.warning(f"Failed to read {webhook_file}: {e}")
                    continue
            
            if not webhook_data:
                return {"status": "success", "files_processed": 0, "message": "No valid webhook data"}
            
            # Convert to DataFrame using existing Spark session (FAST!)
            from pyspark.sql.functions import current_timestamp, lit, date_format
            
            bronze_df = self.spark.createDataFrame(webhook_data)
            
            # Add processing metadata
            bronze_df = bronze_df.withColumn("ingested_at", current_timestamp()) \
                               .withColumn("processing_date", date_format(current_timestamp(), "yyyy-MM-dd")) \
                               .withColumn("processed_to_silver", lit(False))
            
            # Write to Delta Lake using existing session (FAST!)
            webhook_table_path = "s3a://webhook-notifications/bronze/webhooks"
            
            if self.delta_manager.table_exists(webhook_table_path):
                merge_condition = "target.webhook_id = source.webhook_id"
                version = self.delta_manager.upsert_data(
                    table_path=webhook_table_path,
                    source_df=bronze_df,
                    merge_condition=merge_condition
                )
                logger.info(f"MERGE operation completed - version: {version}")
            else:
                version = self.delta_manager.create_table(
                    bronze_df,
                    webhook_table_path,
                    partition_cols=["processing_date"]
                )
                logger.info(f"Created new Delta table - version: {version}")
            
            # Clean up processed files
            deleted_count = 0
            for processed_file in processed_files:
                try:
                    processed_file.unlink()
                    deleted_count += 1
                except Exception as e:
                    logger.error(f"Failed to delete {processed_file}: {e}")
            
            logger.info(f"Processed {len(webhook_data)} webhooks, cleaned {deleted_count} files")
            
            return {
                "status": "success",
                "files_processed": len(webhook_data),
                "files_cleaned": deleted_count,
                "table_version": version
            }
            
        except Exception as e:
            logger.error(f"Error processing webhooks: {e}")
            return {"status": "error", "error": str(e)}
    
    def cleanup_old_files(self):
        """Clean up old webhook files"""
        try:
            pending_dir = Path("/opt/airflow/data/pending_webhooks")
            if not pending_dir.exists():
                return
            
            current_time = time.time()
            old_files = []
            
            for webhook_file in pending_dir.glob("*.json"):
                try:
                    file_age = current_time - webhook_file.stat().st_mtime
                    if file_age > 300:  # 5 minutes
                        old_files.append(webhook_file)
                except:
                    continue
            
            # Delete old files
            for old_file in old_files:
                try:
                    old_file.unlink()
                except:
                    pass
            
            if old_files:
                logger.warning(f"Cleaned up {len(old_files)} old webhook files")
                
        except Exception as e:
            logger.error(f"Error in file cleanup: {e}")
    
    def run_processing_loop(self):
        """
        Main processing loop - runs continuously
        """
        logger.info("🔄 Starting webhook processing loop...")
        
        while True:
            try:
                # Clean up old files first
                self.cleanup_old_files()
                
                # Process pending webhooks
                result = self.process_webhook_files()
                
                if result["files_processed"] > 0:
                    logger.info(f"✅ Processed {result['files_processed']} webhooks")
                
                # Sleep for 10 seconds before next check
                time.sleep(10)
                
            except KeyboardInterrupt:
                logger.info("Shutting down webhook processor...")
                break
            except Exception as e:
                logger.error(f"Error in processing loop: {e}")
                time.sleep(30)  # Wait longer after errors
    
    def shutdown(self):
        """Clean shutdown"""
        if self.delta_manager:
            self.delta_manager.stop()
        logger.info("Webhook processor shutdown complete")

if __name__ == "__main__":
    processor = PersistentWebhookProcessor()
    try:
        processor.run_processing_loop()
    finally:
        processor.shutdown()