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
from get_large_transactions_db import fetch_and_process_data_db
from database import LiFiDatabase

# Global variables for tracking progress
current_process = None
process_status = "idle"
process_progress = {"current": 0, "total": 0, "message": "Ready"}
process_start_time = None

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database instance
db = LiFiDatabase()

def fetch_with_progress_tracking(config_params=None, use_database=True):
    """Wrapper function to track progress of the fetch process."""
    global process_status, process_progress, process_start_time

    def progress_callback(message, current=0, total=0):
        global process_progress
        process_progress = {"current": current, "total": total, "message": message}

    try:
        process_status = "running"
        process_start_time = time.time()
        process_progress = {"current": 0, "total": 0, "message": "Starting transaction fetch..."}
        logger.info("Starting transaction fetch process")

        if use_database:
            # Use the new database-enabled fetch function
            count = fetch_and_process_data_db(progress_callback)
            process_progress["message"] = f"Database fetch completed! Saved {count} transactions."
        else:
            # Use the legacy Excel-only function
            fetch_and_process_data()
            process_progress["message"] = "Excel-only fetch completed successfully"

        process_status = "completed"
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

                <h2>📊 Data Collection:</h2>
                <div class="endpoint">
                    <a href="/rebuild">/rebuild</a> - 🔄 Fetch latest transactions to database + Excel
                </div>
                <div class="endpoint">
                    <a href="/fetch">/fetch</a> - 📥 Legacy Excel-only fetch (old method)
                </div>

                <h2>🗄️ Database Operations:</h2>
                <div class="endpoint">
                    <a href="/data/stats">/data/stats</a> - 📈 Database statistics and analytics
                </div>
                <div class="endpoint">
                    <a href="/data/view">/data/view</a> - 👁️ View transactions with filtering
                </div>
                <div class="endpoint">
                    <a href="/data/export">/data/export</a> - 💾 Export data (Excel, JSON, CSV)
                </div>

                <h2>📈 Monitoring:</h2>
                <div class="endpoint">
                    <a href="/status">/status</a> - 📊 System status (files + database)
                </div>
                <div class="endpoint">
                    <a href="/progress">/progress</a> - ⏱️ Real-time progress tracking (JSON)
                </div>

                <h2>🛠️ Management:</h2>
                <div class="endpoint">
                    <a href="/clear">/clear</a> - 🗑️ Clear files and database
                </div>
                <div class="endpoint">
                    <a href="/download">/download</a> - 📁 File download information
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
            # Delete existing Excel file and clear database
            output_file = "txns_2023_to_2025.xlsx"
            resume_file = "resume_cursor.txt"
            clear_db = query_params.get('database', ['yes'])[0].lower() == 'yes'

            try:
                files_deleted = []
                actions_performed = []

                # Clear files
                if os.path.exists(output_file):
                    os.remove(output_file)
                    files_deleted.append(output_file)
                if os.path.exists(resume_file):
                    os.remove(resume_file)
                    files_deleted.append(resume_file)

                # Clear database if requested
                if clear_db:
                    stats_before = db.get_statistics()
                    db.clear_database()
                    actions_performed.append(f"Database cleared ({stats_before['total_transactions']} transactions removed)")

                if files_deleted or actions_performed:
                    logger.info(f"Cleared files: {', '.join(files_deleted)}, Actions: {', '.join(actions_performed)}")
                    msg = f'''
                    <html>
                    <body>
                        <h2>🗑️ Cleanup Complete</h2>
                        <h3>Files Deleted:</h3>
                        <ul>
                            {''.join(f'<li>{file}</li>' for file in files_deleted) if files_deleted else '<li>No files to delete</li>'}
                        </ul>
                        <h3>Database Actions:</h3>
                        <ul>
                            {''.join(f'<li>{action}</li>' for action in actions_performed) if actions_performed else '<li>Database not cleared</li>'}
                        </ul>
                        <p>You can now start a fresh rebuild.</p>
                        <p>
                            <a href="/rebuild">🔄 Start Rebuild</a> |
                            <a href="/data/stats">📊 Check Database</a> |
                            <a href="/">🏠 Back to Home</a>
                        </p>
                    </body>
                    </html>
                    '''
                else:
                    msg = '''
                    <html>
                    <body>
                        <h2>ℹ️ Nothing to Clear</h2>
                        <p>No files or data found to clear.</p>
                        <p>
                            <a href="/clear?database=yes">🗑️ Force Clear Database</a> |
                            <a href="/">🏠 Back to Home</a>
                        </p>
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

        elif path.startswith('/data/'):
            # Handle database-related endpoints
            if path == '/data/stats':
                # Database statistics endpoint
                try:
                    stats = db.get_statistics()
                    db_info = db.get_database_info()

                    msg = f'''
                    <html>
                    <head>
                        <title>Database Statistics - LiFi Fetcher</title>
                        <style>
                            body {{ font-family: Arial, sans-serif; margin: 40px; }}
                            .stats-card {{
                                border: 1px solid #ddd;
                                border-radius: 8px;
                                padding: 20px;
                                margin: 15px 0;
                                background-color: #f8f9fa;
                            }}
                            .metric {{ display: flex; justify-content: space-between; margin: 10px 0; }}
                            .metric-label {{ font-weight: bold; }}
                            .metric-value {{ color: #007bff; }}
                            table {{ width: 100%; border-collapse: collapse; margin: 10px 0; }}
                            th, td {{ padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }}
                            th {{ background-color: #f2f2f2; }}
                        </style>
                    </head>
                    <body>
                        <h1>📊 Database Statistics</h1>

                        <div class="stats-card">
                            <h3>📋 Overview</h3>
                            <div class="metric">
                                <span class="metric-label">Total Transactions:</span>
                                <span class="metric-value">{stats['total_transactions']:,}</span>
                            </div>
                            <div class="metric">
                                <span class="metric-label">Total Volume (USD):</span>
                                <span class="metric-value">${stats['total_volume_usd']:,.2f}</span>
                            </div>
                            <div class="metric">
                                <span class="metric-label">Recent Transactions (24h):</span>
                                <span class="metric-value">{stats['recent_transactions_24h']:,}</span>
                            </div>
                            <div class="metric">
                                <span class="metric-label">Date Range:</span>
                                <span class="metric-value">{stats['date_range']['earliest']} to {stats['date_range']['latest']}</span>
                            </div>
                        </div>

                        <div class="stats-card">
                            <h3>🪙 Top Tokens</h3>
                            <table>
                                <tr><th>Token</th><th>Transactions</th><th>Volume (USD)</th></tr>
                                {''.join(f'<tr><td>{token["token_symbol"]}</td><td>{token["count"]:,}</td><td>${token["total_volume"] or 0:,.2f}</td></tr>' for token in stats['top_tokens'])}
                            </table>
                        </div>

                        <div class="stats-card">
                            <h3>⛓️ Top Chains</h3>
                            <table>
                                <tr><th>Chain</th><th>Transactions</th><th>Volume (USD)</th></tr>
                                {''.join(f'<tr><td>{chain["chain_name"]}</td><td>{chain["count"]:,}</td><td>${chain["total_volume"] or 0:,.2f}</td></tr>' for chain in stats['top_chains'])}
                            </table>
                        </div>

                        <div class="stats-card">
                            <h3>💾 Database File Info</h3>
                            <div class="metric">
                                <span class="metric-label">File Size:</span>
                                <span class="metric-value">{db_info['file_size_mb']} MB</span>
                            </div>
                            <div class="metric">
                                <span class="metric-label">Last Modified:</span>
                                <span class="metric-value">{db_info['last_modified']}</span>
                            </div>
                            <div class="metric">
                                <span class="metric-label">File Path:</span>
                                <span class="metric-value">{db_info['file_path']}</span>
                            </div>
                        </div>

                        <p style="margin-top: 30px;">
                            <a href="/data/view">👁️ View Data</a> |
                            <a href="/data/export">💾 Export Data</a> |
                            <a href="/">🏠 Home</a>
                        </p>
                    </body>
                    </html>
                    '''
                except Exception as e:
                    logger.error(f"Error getting database statistics: {e}")
                    msg = f'''
                    <html>
                    <body>
                        <h2>❌ Database Error</h2>
                        <p>Error retrieving statistics: {str(e)}</p>
                        <p><a href="/">🏠 Back to Home</a></p>
                    </body>
                    </html>
                    '''

            elif path == '/data/view':
                # View transactions with filtering
                try:
                    # Parse query parameters for filtering
                    token_symbol = query_params.get('token', [None])[0]
                    min_usd = query_params.get('min_usd', [None])[0]
                    max_usd = query_params.get('max_usd', [None])[0]
                    limit = int(query_params.get('limit', ['100'])[0])

                    # Build filters
                    filters = {'limit': limit}
                    if token_symbol:
                        filters['token_symbol'] = token_symbol
                    if min_usd:
                        filters['min_usd'] = float(min_usd)
                    if max_usd:
                        filters['max_usd'] = float(max_usd)

                    transactions = db.get_transactions(**filters)

                    # Create filter form
                    filter_form = f'''
                    <form method="GET" style="background: #f8f9fa; padding: 15px; border-radius: 5px; margin: 15px 0;">
                        <h3>🔍 Filter Transactions</h3>
                        <table>
                            <tr>
                                <td>Token Symbol:</td>
                                <td><input type="text" name="token" value="{token_symbol or ''}" placeholder="e.g., BTC, ETH"></td>
                            </tr>
                            <tr>
                                <td>Min USD Amount:</td>
                                <td><input type="number" name="min_usd" value="{min_usd or ''}" step="0.01"></td>
                            </tr>
                            <tr>
                                <td>Max USD Amount:</td>
                                <td><input type="number" name="max_usd" value="{max_usd or ''}" step="0.01"></td>
                            </tr>
                            <tr>
                                <td>Limit:</td>
                                <td><input type="number" name="limit" value="{limit}" min="1" max="1000"></td>
                            </tr>
                        </table>
                        <button type="submit">Apply Filters</button>
                        <a href="/data/view">Clear</a>
                    </form>
                    '''

                    # Create transaction table
                    if transactions:
                        table_rows = ''
                        for tx in transactions:
                            table_rows += f'''
                            <tr>
                                <td>{tx['transaction_id'][:10]}...</td>
                                <td>{tx['sending_token'] or 'N/A'}</td>
                                <td>${tx['sending_amount_usd'] or 0:.2f}</td>
                                <td>{tx['receiving_token'] or 'N/A'}</td>
                                <td>${tx['receiving_amount_usd'] or 0:.2f}</td>
                                <td>{tx['tool']}</td>
                                <td>{tx['sending_timestamp'][:19] if tx['sending_timestamp'] else 'N/A'}</td>
                            </tr>
                            '''

                        transaction_table = f'''
                        <table style="width: 100%; border-collapse: collapse;">
                            <thead>
                                <tr style="background: #f2f2f2;">
                                    <th>TX ID</th>
                                    <th>Sending Token</th>
                                    <th>Sending USD</th>
                                    <th>Receiving Token</th>
                                    <th>Receiving USD</th>
                                    <th>Tool</th>
                                    <th>Timestamp</th>
                                </tr>
                            </thead>
                            <tbody>
                                {table_rows}
                            </tbody>
                        </table>
                        '''
                    else:
                        transaction_table = '<p>No transactions found with the current filters.</p>'

                    msg = f'''
                    <html>
                    <head>
                        <title>View Transactions - LiFi Fetcher</title>
                        <style>
                            body {{ font-family: Arial, sans-serif; margin: 40px; }}
                            table {{ width: 100%; border-collapse: collapse; }}
                            th, td {{ padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }}
                            th {{ background-color: #f2f2f2; }}
                            input {{ padding: 5px; margin: 2px; }}
                            button {{ padding: 8px 15px; background: #007bff; color: white; border: none; border-radius: 3px; }}
                        </style>
                    </head>
                    <body>
                        <h1>👁️ View Transactions</h1>

                        {filter_form}

                        <h3>📊 Results ({len(transactions)} transactions)</h3>
                        {transaction_table}

                        <p style="margin-top: 30px;">
                            <a href="/data/stats">📈 Statistics</a> |
                            <a href="/data/export">💾 Export</a> |
                            <a href="/">🏠 Home</a>
                        </p>
                    </body>
                    </html>
                    '''

                except Exception as e:
                    logger.error(f"Error viewing transactions: {e}")
                    msg = f'''
                    <html>
                    <body>
                        <h2>❌ Error</h2>
                        <p>Error viewing transactions: {str(e)}</p>
                        <p><a href="/">🏠 Back to Home</a></p>
                    </body>
                    </html>
                    '''

            elif path == '/data/export':
                # Export data in various formats
                try:
                    export_format = query_params.get('format', [None])[0]
                    token_symbol = query_params.get('token', [None])[0]

                    if export_format:
                        # Perform export
                        filters = {}
                        if token_symbol:
                            filters['token_symbol'] = token_symbol

                        if export_format == 'excel':
                            filename = db.export_to_excel(filters=filters)
                            msg = f'''
                            <html>
                            <body>
                                <h2>✅ Excel Export Complete</h2>
                                <p>File created: {filename}</p>
                                <p><a href="/data/export">Back to Export</a> | <a href="/">🏠 Home</a></p>
                            </body>
                            </html>
                            '''
                        elif export_format == 'json':
                            filename = db.export_to_json(filters=filters)
                            msg = f'''
                            <html>
                            <body>
                                <h2>✅ JSON Export Complete</h2>
                                <p>File created: {filename}</p>
                                <p><a href="/data/export">Back to Export</a> | <a href="/">🏠 Home</a></p>
                            </body>
                            </html>
                            '''
                        elif export_format == 'csv':
                            filename = db.export_to_csv(filters=filters)
                            msg = f'''
                            <html>
                            <body>
                                <h2>✅ CSV Export Complete</h2>
                                <p>File created: {filename}</p>
                                <p><a href="/data/export">Back to Export</a> | <a href="/">🏠 Home</a></p>
                            </body>
                            </html>
                            '''
                        else:
                            raise ValueError(f"Unsupported format: {export_format}")

                    else:
                        # Show export options
                        msg = '''
                        <html>
                        <head>
                            <title>Export Data - LiFi Fetcher</title>
                            <style>
                                body { font-family: Arial, sans-serif; margin: 40px; }
                                .export-option {
                                    border: 1px solid #ddd;
                                    border-radius: 8px;
                                    padding: 20px;
                                    margin: 15px 0;
                                    background-color: #f8f9fa;
                                }
                                .export-button {
                                    background: #007bff;
                                    color: white;
                                    padding: 10px 20px;
                                    text-decoration: none;
                                    border-radius: 5px;
                                    margin: 5px;
                                    display: inline-block;
                                }
                            </style>
                        </head>
                        <body>
                            <h1>💾 Export Transaction Data</h1>

                            <div class="export-option">
                                <h3>📊 Excel Export</h3>
                                <p>Export to Excel spreadsheet (.xlsx) - compatible with Excel, LibreOffice, Google Sheets</p>
                                <a href="/data/export?format=excel" class="export-button">Export All to Excel</a>
                                <a href="/data/export?format=excel&token=BTC" class="export-button">Export BTC Only</a>
                            </div>

                            <div class="export-option">
                                <h3>📄 JSON Export</h3>
                                <p>Export to JSON format - perfect for API integration and data analysis</p>
                                <a href="/data/export?format=json" class="export-button">Export All to JSON</a>
                                <a href="/data/export?format=json&token=ETH" class="export-button">Export ETH Only</a>
                            </div>

                            <div class="export-option">
                                <h3>📋 CSV Export</h3>
                                <p>Export to CSV format - universal format for data analysis tools</p>
                                <a href="/data/export?format=csv" class="export-button">Export All to CSV</a>
                                <a href="/data/export?format=csv&token=USDC" class="export-button">Export USDC Only</a>
                            </div>

                            <p style="margin-top: 30px;">
                                <a href="/data/stats">📈 Statistics</a> |
                                <a href="/data/view">👁️ View Data</a> |
                                <a href="/">🏠 Home</a>
                            </p>
                        </body>
                        </html>
                        '''

                except Exception as e:
                    logger.error(f"Error exporting data: {e}")
                    msg = f'''
                    <html>
                    <body>
                        <h2>❌ Export Error</h2>
                        <p>Error: {str(e)}</p>
                        <p><a href="/data/export">Try Again</a> | <a href="/">🏠 Home</a></p>
                    </body>
                    </html>
                    '''

            else:
                # Unknown /data/ endpoint
                msg = '''
                <html>
                <body>
                    <h2>❌ Unknown Data Endpoint</h2>
                    <p>Available endpoints:</p>
                    <ul>
                        <li><a href="/data/stats">📈 Statistics</a></li>
                        <li><a href="/data/view">👁️ View Transactions</a></li>
                        <li><a href="/data/export">💾 Export Data</a></li>
                    </ul>
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
