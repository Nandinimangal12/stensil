import os
import shutil
import time
from datetime import datetime
import re
import logging
import hashlib
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(r"C:\spi\backup", "pcb_counter.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
SOURCE_LOG_PATH = r"C:\spi\spi_log.his"  # Path to the .his log file
BACKUP_DIR = r"C:\spi\backup"            # Backup directory
BACKUP_LOG_PATH = os.path.join(BACKUP_DIR, "spi_log_backup.his")
COUNT_FILE_PATH = os.path.join(BACKUP_DIR, "pcb_daily_count.txt")
PCB_LINES_PATH = os.path.join(BACKUP_DIR, "pcb_lines.txt")  # Debug file for PCB lines
TOTAL_PCB_FILE = os.path.join(BACKUP_DIR, "total_pcb_ids.txt")  # Persistent total PCB IDs
PCB_ID_PATTERN = r"PCB.*(\d+)"           # Primary regex
FALLBACK_PATTERN = r"[Pp][Cc][Bb].*?(\d+)"  # Fallback regex
DELAY_SECONDS = 0.5                      # Delay to ensure file write completion

class LogFileHandler(FileSystemEventHandler):
    def __init__(self):
        self.total_pcb_ids = self.load_total_pcb_ids()
        self.current_date = datetime.now().date()
        
    def load_total_pcb_ids(self):
        """Load total PCB IDs from persistent file."""
        try:
            if os.path.exists(TOTAL_PCB_FILE):
                with open(TOTAL_PCB_FILE, 'r', encoding='utf-8') as f:
                    return set(line.strip() for line in f if line.strip().isdigit())
            return set()
        except Exception as e:
            logger.error(f"Error loading total PCB IDs: {e}")
            return set()

    def save_total_pcb_ids(self):
        """Save total PCB IDs to persistent file."""
        try:
            with open(TOTAL_PCB_FILE, 'w', encoding='utf-8') as f:
                for pcb_id in sorted(self.total_pcb_ids, key=int):
                    f.write(f"{pcb_id}\n")
            logger.info(f"Saved {len(self.total_pcb_ids)} total PCB IDs to {TOTAL_PCB_FILE}")
        except Exception as e:
            logger.error(f"Error saving total PCB IDs: {e}")

    def on_modified(self, event):
        if event.src_path == SOURCE_LOG_PATH and not event.is_directory:
            time.sleep(DELAY_SECONDS)
            self.process_log_file()

    def process_log_file(self):
        # Check if date has changed (for logging and count file)
        current_date = datetime.now().date()
        if current_date != self.current_date:
            logger.info("New day detected.")
            self.current_date = current_date
            self.update_count_file()

        # Copy source file to backup
        try:
            if not os.path.exists(SOURCE_LOG_PATH):
                logger.warning(f"Source file {SOURCE_LOG_PATH} does not exist.")
                self.update_count_file()
                return
            shutil.copy2(SOURCE_LOG_PATH, BACKUP_LOG_PATH)
            logger.info(f"Backed up {SOURCE_LOG_PATH} to {BACKUP_LOG_PATH}")
        except Exception as e:
            logger.error(f"Error copying source file to backup: {e}")
            self.update_count_file()
            return

        # Read and process backup file
        try:
            file_size = os.path.getsize(BACKUP_LOG_PATH)
            if file_size == 0:
                logger.warning("Backup file is empty. Skipping processing.")
                self.update_count_file()
                return
                
            # Try reading as text with different encodings
            content = None
            content_hash = None
            for encoding in ['utf-8', 'latin1', 'ascii', 'utf-16', 'utf-16-le', 'utf-16-be']:
                try:
                    with open(BACKUP_LOG_PATH, 'r', encoding=encoding, errors='ignore') as backup_file:
                        content = backup_file.read()
                        content_hash = hashlib.md5(content.encode('utf-8', errors='ignore')).hexdigest()
                        logger.info(f"Read {len(content)} characters from backup file using {encoding} encoding. MD5: {content_hash}")
                        break
                except Exception as e:
                    logger.warning(f"Failed to read with {encoding} encoding: {e}")
            
            # Fallback: Read as binary
            if content is None:
                try:
                    with open(BACKUP_LOG_PATH, 'rb') as backup_file:
                        raw_content = backup_file.read()
                        content = raw_content.decode('utf-8', errors='ignore')
                        content_hash = hashlib.md5(content.encode('utf-8', errors='ignore')).hexdigest()
                        logger.info(f"Read {len(content)} characters from backup file as binary (decoded utf-8). MD5: {content_hash}")
                except Exception as e:
                    logger.error(f"Failed to read backup file as binary: {e}")
                    self.update_count_file()
                    return

            # Log first 500 lines or full content
            lines = content.splitlines()
            log_lines = lines[:500] if len(lines) > 500 else lines
            logger.debug(f"First {len(log_lines)} lines of backup file:\n{chr(10).join(log_lines)}")

            # Extract PCB IDs
            pcb_ids = re.findall(PCB_ID_PATTERN, content, re.IGNORECASE)
            if not pcb_ids:
                logger.warning("No PCB IDs found with primary pattern. Trying fallback.")
                pcb_ids = re.findall(FALLBACK_PATTERN, content, re.IGNORECASE)
            
            pcb_lines = [line.strip() for line in lines if "PCB" in line.lower() or "pcb" in line]
            
            # Log PCB-related lines to separate file
            try:
                with open(PCB_LINES_PATH, 'w', encoding='utf-8') as pcb_file:
                    pcb_file.write(f"Date: {datetime.now()}\n")
                    pcb_file.write(f"PCB-related lines ({len(pcb_lines)}):\n")
                    for line in pcb_lines:
                        pcb_file.write(f"{line}\n")
                logger.info(f"Wrote {len(pcb_lines)} PCB-related lines to {PCB_LINES_PATH}")
            except Exception as e:
                logger.error(f"Error writing to PCB lines file: {e}")

            if not pcb_ids:
                logger.warning("No PCB IDs found. Check PCB lines in pcb_lines.txt.")
                if pcb_lines:
                    logger.debug(f"Found {len(pcb_lines)} lines containing 'PCB':\n{chr(10).join(pcb_lines)}")
                
            logger.info(f"Found {len(pcb_ids)} PCB IDs: {pcb_ids}")
            prev_total_count = len(self.total_pcb_ids)
            self.total_pcb_ids.update(pcb_ids)
            
            # Save total PCB IDs
            if len(self.total_pcb_ids) > prev_total_count:
                self.save_total_pcb_ids()
            
            # Always update count file
            self.update_count_file()
            if len(self.total_pcb_ids) > prev_total_count:
                logger.info(f"Added {len(self.total_pcb_ids) - prev_total_count} new PCB IDs.")
            else:
                logger.debug("No new PCB IDs added.")
                    
        except Exception as e:
            logger.error(f"Error processing backup file: {e}")
            self.update_count_file()

    def update_count_file(self):
        try:
            with open(COUNT_FILE_PATH, 'w', encoding='utf-8') as count_file:
                count_file.write(f"{len(self.total_pcb_ids)}\n")
            logger.info(f"Updated count file with {len(self.total_pcb_ids)} total PCBs")
        except PermissionError as e:
            logger.error(f"Permission error writing to count file: {e}")
        except Exception as e:
            logger.error(f"Error updating count file: {e}")

def ensure_backup_directory():
    try:
        if not os.path.exists(BACKUP_DIR):
            os.makedirs(BACKUP_DIR)
            logger.info(f"Created backup directory: {BACKUP_DIR}")
    except Exception as e:
        logger.error(f"Error creating backup directory: {e}")

def main():
    ensure_backup_directory()
    event_handler = LogFileHandler()
    
    # Initial count file creation
    logger.info("Creating initial count file to test write permissions.")
    event_handler.update_count_file()
    
    # Initial processing
    if os.path.exists(SOURCE_LOG_PATH):
        logger.info("Performing initial processing.")
        event_handler.process_log_file()
    
    # Start live monitoring
    observer = Observer()
    observer.schedule(event_handler, path=os.path.dirname(SOURCE_LOG_PATH), recursive=False)
    observer.start()
    
    logger.info(f"Live monitoring started for: {SOURCE_LOG_PATH}")
    logger.info(f"Backup file: {BACKUP_LOG_PATH}")
    logger.info(f"Count file: {COUNT_FILE_PATH}")
    logger.info(f"PCB lines debug file: {PCB_LINES_PATH}")
    logger.info(f"Total PCB IDs file: {TOTAL_PCB_FILE}")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping live monitoring")
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()
