import subprocess
import sys
import os
from pathlib import Path
import time

def run_script(script_path, args=None, capture_output=True):
    """Run a Python script with arguments."""
    cmd = [sys.executable, str(script_path)]
    if args:
        cmd.extend(args)
    
    result = subprocess.run(
        cmd,
        capture_output=capture_output,
        text=True
    )
    
    return {
        'returncode': result.returncode,
        'stdout': result.stdout if capture_output else None,
        'stderr': result.stderr if capture_output else None
    }

def run_gmail_parser(script_path, max_emails=10, dry_run=False, config=None):
    """Run the Gmail parser script."""
    args = []
    
    if max_emails:
        args.extend(["--max-emails", str(max_emails)])
    if dry_run:
        args.append("--dry-run")
    if config:
        args.extend(["--config", config])
    
    return run_script(script_path, args)

def run_compass_enrichment(script_path, output=None, limit=None, headless=False, update_db=False, address=None):
    """Run the Compass enrichment script."""
    args = []
    
    if output:
        args.extend(["--output", output])
    if limit:
        args.extend(["--limit", str(limit)])
    if headless:
        args.append("--headless")
    if update_db:
        args.append("--update-db")
    if address:
        args.extend(["--address", address])
    
    return run_script(script_path, args)

def run_walkscore_enrichment(script_path):
    """Run the WalkScore enrichment script."""
    return run_script(script_path)

def run_init_db(script_path):
    """Run the database initialization script."""
    return run_script(script_path)

def get_script_progress(result_stdout):
    """Parse script output to get progress information."""
    if not result_stdout:
        return None
    
    lines = result_stdout.strip().split('\n')
    progress_info = {
        'total': 0,
        'processed': 0,
        'success': 0,
        'failed': 0,
        'last_message': '',
    }
    
    for line in lines:
        line = line.strip()
        
        # Try to extract total count
        if "Found" in line and "listings" in line:
            try:
                progress_info['total'] = int(line.split("Found")[1].split("listings")[0].strip())
            except (ValueError, IndexError):
                pass
        
        # Try to extract processed count
        if "Processing" in line and "/" in line:
            try:
                parts = line.split("Processing")[1].split("/")[0].strip()
                if "[" in parts:
                    parts = parts.split("[")[1].strip()
                progress_info['processed'] = int(parts)
            except (ValueError, IndexError):
                pass
        
        # Check for success messages
        if "✅" in line or "Successfully" in line:
            progress_info['success'] += 1
        
        # Check for error messages
        if "❌" in line or "Error" in line or "Failed" in line:
            progress_info['failed'] += 1
        
        # Store the last line as a message
        if line:
            progress_info['last_message'] = line
    
    return progress_info
