import os
import http.server
import socketserver
import json
import threading
from urllib.parse import urlparse, parse_qs
from http import HTTPStatus
from get_large_transactions import fetch_and_process_data


class Handler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        query_params = parse_qs(parsed_path.query)

        self.send_response(HTTPStatus.OK)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

        if path == '/':
            msg = '''
            <html>
            <head><title>LiFi Transaction Fetcher</title></head>
            <body>
                <h1>LiFi Large Transactions Fetcher</h1>
                <p>Welcome to the LiFi transaction analysis tool!</p>
                <h2>Available Endpoints:</h2>
                <ul>
                    <li><a href="/fetch">/fetch</a> - Start fetching large BTC transactions</li>
                    <li><a href="/status">/status</a> - Check if fetching is in progress</li>
                    <li><a href="/download">/download</a> - Download the latest results (if available)</li>
                </ul>
                <p>The system fetches BTC transactions from 2023 to 2025 and saves them to an Excel file.</p>
            </body>
            </html>
            '''
        elif path == '/fetch':
            # Start the fetch process in a background thread
            try:
                thread = threading.Thread(target=fetch_and_process_data)
                thread.daemon = True
                thread.start()
                msg = '''
                <html>
                <body>
                    <h2>Fetching Started!</h2>
                    <p>The transaction fetching process has been started in the background.</p>
                    <p>This may take several minutes to complete.</p>
                    <p><a href="/status">Check Status</a> | <a href="/">Back to Home</a></p>
                </body>
                </html>
                '''
            except Exception as e:
                msg = f'''
                <html>
                <body>
                    <h2>Error!</h2>
                    <p>Failed to start fetching: {str(e)}</p>
                    <p><a href="/">Back to Home</a></p>
                </body>
                </html>
                '''
        elif path == '/status':
            # Check if output file exists and its size
            output_file = "txns_2023_to_2025.xlsx"
            if os.path.exists(output_file):
                file_size = os.path.getsize(output_file)
                msg = f'''
                <html>
                <body>
                    <h2>Status</h2>
                    <p>Output file exists: {output_file}</p>
                    <p>File size: {file_size} bytes</p>
                    <p><a href="/download">Download Results</a> | <a href="/">Back to Home</a></p>
                </body>
                </html>
                '''
            else:
                msg = '''
                <html>
                <body>
                    <h2>Status</h2>
                    <p>No output file found. Either fetching hasn't started or is still in progress.</p>
                    <p><a href="/fetch">Start Fetching</a> | <a href="/">Back to Home</a></p>
                </body>
                </html>
                '''
        elif path == '/download':
            output_file = "txns_2023_to_2025.xlsx"
            if os.path.exists(output_file):
                msg = f'''
                <html>
                <body>
                    <h2>Download</h2>
                    <p>File is available for download: {output_file}</p>
                    <p>File size: {os.path.getsize(output_file)} bytes</p>
                    <p>Note: Direct file download through this web interface is not implemented.</p>
                    <p>The file is saved locally in the application directory.</p>
                    <p><a href="/">Back to Home</a></p>
                </body>
                </html>
                '''
            else:
                msg = '''
                <html>
                <body>
                    <h2>Download</h2>
                    <p>No file available for download.</p>
                    <p><a href="/fetch">Start Fetching</a> | <a href="/">Back to Home</a></p>
                </body>
                </html>
                '''
        else:
            msg = f'''
            <html>
            <body>
                <h2>Page Not Found</h2>
                <p>You requested: {path}</p>
                <p><a href="/">Back to Home</a></p>
            </body>
            </html>
            '''

        self.wfile.write(msg.encode())


port = int(os.getenv('PORT', 8080))
print('LiFi Transaction Fetcher listening on port %s' % (port))
print('Visit http://localhost:%s to access the web interface' % (port))
httpd = socketserver.TCPServer(('', port), Handler)
httpd.serve_forever()
