import os
import http.server
import socketserver
import json
import threading
import logging
import time
from urllib.parse import urlparse, parse_qs
from http import HTTPStatus
from datetime import datetime
from get_large_transactions import fetch_and_process_data

# Global variables for tracking progress
current_process = None
process_status = "idle"
process_progress = {"current": 0, "total": 0, "message": "Ready"}
process_start_time = None

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_with_progress_tracking(config_params=None):
    """Wrapper function to track progress of the fetch process."""
    global process_status, process_progress, process_start_time

    try:
        process_status = "running"
        process_start_time = time.time()
        process_progress = {"current": 0, "total": 0, "message": "Starting transaction fetch..."}
        logger.info("Starting transaction fetch process")

        # Call the actual fetch function
        fetch_and_process_data()

        process_status = "completed"
        process_progress["message"] = "Transaction fetch completed successfully"
        logger.info("Transaction fetch completed successfully")

    except Exception as e:
        process_status = "error"
        process_progress["message"] = f"Error: {str(e)}"
        logger.error(f"Transaction fetch failed: {str(e)}")
    finally:
        if process_status != "error":
            process_status = "completed"

class Handler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        global current_process, process_status, process_progress, process_start_time

        parsed_path = urlparse(self.path)
        path = parsed_path.path
        query_params = parse_qs(parsed_path.query)

        self.send_response(HTTPStatus.OK)

        # Set content type based on endpoint
        if path == '/progress':
            self.send_header('Content-type', 'application/json')
        else:
            self.send_header('Content-type', 'text/html')
        self.end_headers()

        if path == '/':
            msg = '''
            <html>
            <head>
                <title>LiFi Transaction Fetcher</title>
                <style>
                    body { font-family: Arial, sans-serif; margin: 40px; }
                    .endpoint { margin: 10px 0; padding: 10px; border: 1px solid #ddd; border-radius: 5px; }
                    .endpoint a { text-decoration: none; font-weight: bold; color: #0066cc; }
                    .endpoint a:hover { text-decoration: underline; }
                    .warning { color: #ff6600; font-weight: bold; }
                    .success { color: #00aa00; font-weight: bold; }
                </style>
            </head>
            <body>
                <h1>🚀 LiFi Large Transactions Fetcher</h1>
                <p>Welcome to the enhanced LiFi transaction analysis tool!</p>

                <h2>📊 Main Operations:</h2>
                <div class="endpoint">
                    <a href="/rebuild">/rebuild</a> - 🔄 Rebuild Excel file with latest BTC transactions
                </div>
                <div class="endpoint">
                    <a href="/fetch">/fetch</a> - 📥 Start fetching large BTC transactions (legacy)
                </div>

                <h2>📈 Monitoring:</h2>
                <div class="endpoint">
                    <a href="/status">/status</a> - 📊 Check detailed process status and file information
                </div>
                <div class="endpoint">
                    <a href="/progress">/progress</a> - ⏱️ Real-time progress tracking (JSON)
                </div>

                <h2>🛠️ Management:</h2>
                <div class="endpoint">
                    <a href="/clear">/clear</a> - 🗑️ Delete existing Excel file and start fresh
                </div>
                <div class="endpoint">
                    <a href="/download">/download</a> - 💾 Download the latest results (if available)
                </div>

                <h2>ℹ️ System Info:</h2>
                <p><strong>Date Range:</strong> 2023-01-01 to 2025-09-30</p>
                <p><strong>Token Filter:</strong> BTC transactions only</p>
                <p><strong>Output:</strong> txns_2023_to_2025.xlsx</p>

                <div style="margin-top: 30px; padding: 15px; background-color: #f0f8ff; border-radius: 5px;">
                    <h3>💡 Quick Start:</h3>
                    <ol>
                        <li>Click <a href="/rebuild">/rebuild</a> to start fetching transactions</li>
                        <li>Monitor progress with <a href="/status">/status</a></li>
                        <li>Check <a href="/download">/download</a> when complete</li>
                    </ol>
                </div>
            </body>
            </html>
            '''
        elif path == '/fetch' or path == '/rebuild':
            # Start the fetch process in a background thread
            if process_status == "running":
                msg = '''
                <html>
                <body>
                    <h2>⚠️ Process Already Running</h2>
                    <p>A transaction fetching process is already running.</p>
                    <p><a href="/status">Check Status</a> | <a href="/progress">View Progress</a> | <a href="/">Back to Home</a></p>
                </body>
                </html>
                '''
            else:
                try:
                    # Parse configuration parameters
                    config_params = {
                        'token_filter': query_params.get('token', ['BTC'])[0],
                        'start_date': query_params.get('start_date', ['2023-01-01'])[0],
                        'end_date': query_params.get('end_date', ['2025-09-30'])[0]
                    }

                    current_process = threading.Thread(target=fetch_with_progress_tracking, args=(config_params,))
                    current_process.daemon = True
                    current_process.start()

                    endpoint_name = "Rebuild" if path == '/rebuild' else "Fetch"
                    logger.info(f"{endpoint_name} process started with params: {config_params}")

                    msg = f'''
                    <html>
                    <body>
                        <h2>✅ {endpoint_name} Started!</h2>
                        <p>The transaction fetching process has been started in the background.</p>
                        <p><strong>Configuration:</strong></p>
                        <ul>
                            <li>Token Filter: {config_params['token_filter']}</li>
                            <li>Start Date: {config_params['start_date']}</li>
                            <li>End Date: {config_params['end_date']}</li>
                        </ul>
                        <p>This may take several minutes to complete depending on the date range.</p>
                        <p>
                            <a href="/status">📊 Check Status</a> |
                            <a href="/progress">⏱️ View Progress</a> |
                            <a href="/">🏠 Back to Home</a>
                        </p>
                        <script>
                            // Auto-refresh every 5 seconds
                            setTimeout(function(){{
                                window.location.href = '/status';
                            }}, 5000);
                        </script>
                    </body>
                    </html>
                    '''
                except Exception as e:
                    logger.error(f"Failed to start fetch process: {str(e)}")
                    msg = f'''
                    <html>
                    <body>
                        <h2>❌ Error!</h2>
                        <p>Failed to start fetching: {str(e)}</p>
                        <p><a href="/">🏠 Back to Home</a></p>
                    </body>
                    </html>
                    '''
        elif path == '/progress':
            # Return JSON progress data
            progress_data = {
                "status": process_status,
                "progress": process_progress.copy(),
                "start_time": process_start_time,
                "elapsed_time": time.time() - process_start_time if process_start_time else 0,
                "timestamp": time.time()
            }

            msg = json.dumps(progress_data, indent=2)

        elif path == '/clear':
            # Delete existing Excel file
            output_file = "txns_2023_to_2025.xlsx"
            resume_file = "resume_cursor.txt"

            try:
                files_deleted = []
                if os.path.exists(output_file):
                    os.remove(output_file)
                    files_deleted.append(output_file)
                if os.path.exists(resume_file):
                    os.remove(resume_file)
                    files_deleted.append(resume_file)

                if files_deleted:
                    logger.info(f"Cleared files: {', '.join(files_deleted)}")
                    msg = f'''
                    <html>
                    <body>
                        <h2>🗑️ Files Cleared</h2>
                        <p>Successfully deleted the following files:</p>
                        <ul>
                            {''.join(f'<li>{file}</li>' for file in files_deleted)}
                        </ul>
                        <p>You can now start a fresh rebuild.</p>
                        <p>
                            <a href="/rebuild">🔄 Start Rebuild</a> |
                            <a href="/">🏠 Back to Home</a>
                        </p>
                    </body>
                    </html>
                    '''
                else:
                    msg = '''
                    <html>
                    <body>
                        <h2>ℹ️ No Files to Clear</h2>
                        <p>No Excel or resume files found to delete.</p>
                        <p><a href="/">🏠 Back to Home</a></p>
                    </body>
                    </html>
                    '''
            except Exception as e:
                logger.error(f"Error clearing files: {str(e)}")
                msg = f'''
                <html>
                <body>
                    <h2>❌ Error Clearing Files</h2>
                    <p>Error: {str(e)}</p>
                    <p><a href="/">🏠 Back to Home</a></p>
                </body>
                </html>
                '''

        elif path == '/status':
            # Enhanced status with detailed information
            output_file = "txns_2023_to_2025.xlsx"
            resume_file = "resume_cursor.txt"

            # File information
            file_info = {}
            if os.path.exists(output_file):
                file_stat = os.stat(output_file)
                file_info['exists'] = True
                file_info['size'] = file_stat.st_size
                file_info['size_mb'] = round(file_stat.st_size / (1024*1024), 2)
                file_info['modified'] = datetime.fromtimestamp(file_stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
            else:
                file_info['exists'] = False

            # Process information
            elapsed_time = time.time() - process_start_time if process_start_time else 0
            elapsed_str = f"{int(elapsed_time//60)}m {int(elapsed_time%60)}s" if elapsed_time > 0 else "N/A"

            # Status indicators
            status_color = {
                'idle': '#6c757d',
                'running': '#007bff',
                'completed': '#28a745',
                'error': '#dc3545'
            }.get(process_status, '#6c757d')

            status_icon = {
                'idle': '⚪',
                'running': '🔵',
                'completed': '🟢',
                'error': '🔴'
            }.get(process_status, '⚪')

            msg = f'''
            <html>
            <head>
                <title>Status - LiFi Transaction Fetcher</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 40px; }}
                    .status-card {{
                        border: 1px solid #ddd;
                        border-radius: 8px;
                        padding: 20px;
                        margin: 15px 0;
                        background-color: #f8f9fa;
                    }}
                    .status-indicator {{
                        display: inline-block;
                        padding: 5px 15px;
                        border-radius: 20px;
                        color: white;
                        font-weight: bold;
                        background-color: {status_color};
                    }}
                    .file-info {{ background-color: {'#d4edda' if file_info['exists'] else '#f8d7da'}; }}
                    .process-info {{ background-color: #e2e3e5; }}
                    .actions {{ background-color: #cce5ff; }}
                    table {{ width: 100%; border-collapse: collapse; }}
                    th, td {{ padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }}
                    th {{ background-color: #f2f2f2; }}
                    .refresh-note {{ font-style: italic; color: #666; margin-top: 20px; }}
                </style>
                <meta http-equiv="refresh" content="10">
            </head>
            <body>
                <h1>📊 System Status</h1>

                <div class="status-card process-info">
                    <h3>🔄 Process Status</h3>
                    <p><span class="status-indicator">{status_icon} {process_status.upper()}</span></p>
                    <table>
                        <tr><th>Current Status</th><td>{process_status}</td></tr>
                        <tr><th>Current Message</th><td>{process_progress.get('message', 'N/A')}</td></tr>
                        <tr><th>Elapsed Time</th><td>{elapsed_str}</td></tr>
                        <tr><th>Process Thread</th><td>{'Active' if current_process and current_process.is_alive() else 'Inactive'}</td></tr>
                    </table>
                </div>

                <div class="status-card file-info">
                    <h3>📁 File Information</h3>
                    <table>
                        <tr><th>Output File</th><td>{output_file}</td></tr>
                        <tr><th>File Exists</th><td>{'✅ Yes' if file_info['exists'] else '❌ No'}</td></tr>
                        {f'<tr><th>File Size</th><td>{file_info["size"]:,} bytes ({file_info["size_mb"]} MB)</td></tr>' if file_info['exists'] else ''}
                        {f'<tr><th>Last Modified</th><td>{file_info["modified"]}</td></tr>' if file_info['exists'] else ''}
                        <tr><th>Resume File</th><td>{'✅ Exists' if os.path.exists(resume_file) else '❌ Not found'}</td></tr>
                    </table>
                </div>

                <div class="status-card actions">
                    <h3>🛠️ Available Actions</h3>
                    <p>
                        {f'<a href="/download">💾 Download Excel File</a> | ' if file_info['exists'] else ''}
                        <a href="/progress">⏱️ View Progress (JSON)</a> |
                        {f'<a href="/rebuild">🔄 Start New Rebuild</a> | ' if process_status != 'running' else ''}
                        <a href="/clear">🗑️ Clear Files</a> |
                        <a href="/">🏠 Home</a>
                    </p>
                </div>

                <div class="refresh-note">
                    <p>🔄 This page automatically refreshes every 10 seconds</p>
                    <p>Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>
            </body>
            </html>
            '''
        elif path == '/download':
            output_file = "txns_2023_to_2025.xlsx"
            if os.path.exists(output_file):
                file_stat = os.stat(output_file)
                file_size_mb = round(file_stat.st_size / (1024*1024), 2)
                modified_time = datetime.fromtimestamp(file_stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')

                msg = f'''
                <html>
                <head>
                    <title>Download - LiFi Transaction Fetcher</title>
                    <style>
                        body {{ font-family: Arial, sans-serif; margin: 40px; }}
                        .download-info {{
                            border: 1px solid #28a745;
                            border-radius: 8px;
                            padding: 20px;
                            background-color: #d4edda;
                            margin: 15px 0;
                        }}
                        .file-details {{
                            background-color: #f8f9fa;
                            padding: 15px;
                            border-radius: 5px;
                            margin: 10px 0;
                        }}
                        table {{ width: 100%; border-collapse: collapse; }}
                        th, td {{ padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }}
                        th {{ background-color: #f2f2f2; }}
                    </style>
                </head>
                <body>
                    <h1>💾 Download Excel File</h1>

                    <div class="download-info">
                        <h2>✅ File Ready for Download</h2>
                        <p>Your LiFi transaction data has been successfully processed and is ready for download.</p>
                    </div>

                    <div class="file-details">
                        <h3>📋 File Details</h3>
                        <table>
                            <tr><th>File Name</th><td>{output_file}</td></tr>
                            <tr><th>File Size</th><td>{file_stat.st_size:,} bytes ({file_size_mb} MB)</td></tr>
                            <tr><th>Last Modified</th><td>{modified_time}</td></tr>
                            <tr><th>Content</th><td>BTC cross-chain transactions (2023-2025)</td></tr>
                            <tr><th>Format</th><td>Microsoft Excel (.xlsx)</td></tr>
                        </table>
                    </div>

                    <div style="background-color: #fff3cd; padding: 15px; border-radius: 5px; margin: 15px 0;">
                        <h3>📝 Note about Downloads</h3>
                        <p><strong>File Location:</strong> The Excel file is saved locally in the server's application directory.</p>
                        <p><strong>Server Access:</strong> To download the file, you'll need to access the server filesystem directly or implement file serving capabilities.</p>
                        <p><strong>File Path:</strong> <code>./{output_file}</code></p>
                    </div>

                    <div style="margin-top: 30px;">
                        <h3>🛠️ Next Steps</h3>
                        <p>
                            <a href="/status">📊 View Status</a> |
                            <a href="/rebuild">🔄 Rebuild Data</a> |
                            <a href="/">🏠 Back to Home</a>
                        </p>
                    </div>
                </body>
                </html>
                '''
            else:
                msg = '''
                <html>
                <head>
                    <title>Download - LiFi Transaction Fetcher</title>
                    <style>
                        body {{ font-family: Arial, sans-serif; margin: 40px; }}
                        .no-file {{
                            border: 1px solid #dc3545;
                            border-radius: 8px;
                            padding: 20px;
                            background-color: #f8d7da;
                            margin: 15px 0;
                        }}
                    </style>
                </head>
                <body>
                    <h1>💾 Download Excel File</h1>

                    <div class="no-file">
                        <h2>❌ No File Available</h2>
                        <p>No Excel file found for download. You need to build the transaction data first.</p>
                    </div>

                    <div style="margin-top: 30px;">
                        <h3>🛠️ Available Actions</h3>
                        <p>
                            <a href="/rebuild">🔄 Start Building Excel File</a> |
                            <a href="/status">📊 Check Status</a> |
                            <a href="/">🏠 Back to Home</a>
                        </p>
                    </div>
                </body>
                </html>
                '''
        else:
            logger.warning(f"404 - Page not found: {path}")
            msg = f'''
            <html>
            <head>
                <title>404 - Page Not Found</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 40px; }}
                    .error-404 {{
                        border: 1px solid #ffc107;
                        border-radius: 8px;
                        padding: 20px;
                        background-color: #fff3cd;
                        margin: 15px 0;
                    }}
                </style>
            </head>
            <body>
                <h1>🚫 404 - Page Not Found</h1>

                <div class="error-404">
                    <h2>⚠️ Invalid Endpoint</h2>
                    <p><strong>Requested URL:</strong> <code>{path}</code></p>
                    <p>The page you requested does not exist on this server.</p>
                </div>

                <h3>📋 Available Endpoints:</h3>
                <ul>
                    <li><strong>/</strong> - Home page</li>
                    <li><strong>/rebuild</strong> - Start Excel rebuild process</li>
                    <li><strong>/fetch</strong> - Legacy fetch endpoint</li>
                    <li><strong>/status</strong> - Detailed system status</li>
                    <li><strong>/progress</strong> - JSON progress data</li>
                    <li><strong>/clear</strong> - Clear existing files</li>
                    <li><strong>/download</strong> - Download information</li>
                </ul>

                <p style="margin-top: 30px;">
                    <a href="/">🏠 Return to Home Page</a>
                </p>
            </body>
            </html>
            '''

        try:
            self.wfile.write(msg.encode())
        except Exception as e:
            logger.error(f"Error writing response: {str(e)}")

    def log_message(self, format, *args):
        """Override to use our logger instead of default stderr logging."""
        logger.info(f"{self.address_string()} - {format % args}")


port = int(os.getenv('PORT', 8080))
print('LiFi Transaction Fetcher listening on port %s' % (port))
print('Visit http://localhost:%s to access the web interface' % (port))
httpd = socketserver.TCPServer(('', port), Handler)
httpd.serve_forever()
