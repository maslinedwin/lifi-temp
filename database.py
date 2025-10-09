import sqlite3
import json
import csv
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional, Any
import logging

logger = logging.getLogger(__name__)

class LiFiDatabase:
    def __init__(self, db_path: str = "lifi_transactions.db"):
        self.db_path = db_path
        self.init_database()

    def get_connection(self):
        """Get database connection with proper settings."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Enable column access by name
        return conn

    def init_database(self):
        """Initialize database with required tables."""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Main transactions table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id TEXT PRIMARY KEY,
                    from_address TEXT,
                    to_address TEXT,
                    tool TEXT,
                    status TEXT,
                    substatus TEXT,
                    substatus_message TEXT,
                    lifi_explorer_link TEXT,
                    integrator TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Sending transactions details
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sending_transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    transaction_id TEXT,
                    tx_hash TEXT,
                    tx_link TEXT,
                    token_address TEXT,
                    token_symbol TEXT,
                    token_name TEXT,
                    token_decimals INTEGER,
                    token_price_usd DECIMAL,
                    chain_id INTEGER,
                    chain_name TEXT,
                    amount TEXT,
                    amount_usd DECIMAL,
                    gas_price TEXT,
                    gas_used TEXT,
                    gas_amount TEXT,
                    gas_amount_usd DECIMAL,
                    timestamp TIMESTAMP,
                    FOREIGN KEY (transaction_id) REFERENCES transactions (transaction_id)
                )
            ''')

            # Receiving transactions details
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS receiving_transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    transaction_id TEXT,
                    tx_hash TEXT,
                    tx_link TEXT,
                    token_address TEXT,
                    token_symbol TEXT,
                    token_name TEXT,
                    token_decimals INTEGER,
                    token_price_usd DECIMAL,
                    chain_id INTEGER,
                    chain_name TEXT,
                    amount TEXT,
                    amount_usd DECIMAL,
                    gas_price TEXT,
                    gas_used TEXT,
                    gas_amount TEXT,
                    gas_amount_usd DECIMAL,
                    timestamp TIMESTAMP,
                    FOREIGN KEY (transaction_id) REFERENCES transactions (transaction_id)
                )
            ''')

            # Create indexes for better performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_tool ON transactions(tool)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sending_token_symbol ON sending_transactions(token_symbol)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sending_amount_usd ON sending_transactions(amount_usd)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sending_timestamp ON sending_transactions(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_receiving_token_symbol ON receiving_transactions(token_symbol)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_receiving_timestamp ON receiving_transactions(timestamp)')

            conn.commit()
            logger.info("Database initialized successfully")

    def get_chain_name(self, chain_id: int) -> str:
        """Convert chain ID to human-readable name."""
        chain_map = {
            1: "Ethereum", 10: "Optimism", 56: "BNB Smart Chain", 100: "Gnosis Chain",
            137: "Polygon", 250: "Fantom", 42161: "Arbitrum", 43114: "Avalanche",
            20000000000001: "Bitcoin", 747474: "Katana", 8453: "Base",
            324: "zkSync Era", 59144: "Linea", 534352: "Scroll"
        }
        return chain_map.get(chain_id, f"Chain {chain_id}")

    def insert_transaction(self, tx_data: Dict[str, Any]) -> bool:
        """Insert a single transaction into the database."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Check if transaction already exists
                cursor.execute(
                    "SELECT transaction_id FROM transactions WHERE transaction_id = ?",
                    (tx_data.get('transactionId'),)
                )
                if cursor.fetchone():
                    return False  # Transaction already exists

                # Extract metadata
                metadata = tx_data.get('metadata', {})

                # Insert main transaction
                cursor.execute('''
                    INSERT INTO transactions (
                        transaction_id, from_address, to_address, tool, status,
                        substatus, substatus_message, lifi_explorer_link, integrator
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    tx_data.get('transactionId'),
                    tx_data.get('fromAddress'),
                    tx_data.get('toAddress'),
                    tx_data.get('tool'),
                    tx_data.get('status'),
                    tx_data.get('substatus'),
                    tx_data.get('substatusMessage'),
                    tx_data.get('lifiExplorerLink'),
                    metadata.get('integrator')
                ))

                # Insert sending transaction details
                sending = tx_data.get('sending', {})
                if sending:
                    sending_token = sending.get('token', {})
                    cursor.execute('''
                        INSERT INTO sending_transactions (
                            transaction_id, tx_hash, tx_link, token_address, token_symbol,
                            token_name, token_decimals, token_price_usd, chain_id, chain_name,
                            amount, amount_usd, gas_price, gas_used, gas_amount, gas_amount_usd, timestamp
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        tx_data.get('transactionId'),
                        sending.get('txHash'),
                        sending.get('txLink'),
                        sending_token.get('address'),
                        sending_token.get('symbol'),
                        sending_token.get('name'),
                        sending_token.get('decimals'),
                        float(sending_token.get('priceUSD', 0)) if sending_token.get('priceUSD') else None,
                        sending.get('chainId'),
                        self.get_chain_name(sending.get('chainId', 0)),
                        sending.get('amount'),
                        float(sending.get('amountUSD', 0)) if sending.get('amountUSD') else None,
                        sending.get('gasPrice'),
                        sending.get('gasUsed'),
                        sending.get('gasAmount'),
                        float(sending.get('gasAmountUSD', 0)) if sending.get('gasAmountUSD') else None,
                        datetime.fromtimestamp(sending.get('timestamp', 0)) if sending.get('timestamp') else None
                    ))

                # Insert receiving transaction details
                receiving = tx_data.get('receiving', {})
                if receiving:
                    receiving_token = receiving.get('token', {})
                    cursor.execute('''
                        INSERT INTO receiving_transactions (
                            transaction_id, tx_hash, tx_link, token_address, token_symbol,
                            token_name, token_decimals, token_price_usd, chain_id, chain_name,
                            amount, amount_usd, gas_price, gas_used, gas_amount, gas_amount_usd, timestamp
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        tx_data.get('transactionId'),
                        receiving.get('txHash'),
                        receiving.get('txLink'),
                        receiving_token.get('address'),
                        receiving_token.get('symbol'),
                        receiving_token.get('name'),
                        receiving_token.get('decimals'),
                        float(receiving_token.get('priceUSD', 0)) if receiving_token.get('priceUSD') else None,
                        receiving.get('chainId'),
                        self.get_chain_name(receiving.get('chainId', 0)),
                        receiving.get('amount'),
                        float(receiving.get('amountUSD', 0)) if receiving.get('amountUSD') else None,
                        receiving.get('gasPrice'),
                        receiving.get('gasUsed'),
                        receiving.get('gasAmount'),
                        float(receiving.get('gasAmountUSD', 0)) if receiving.get('gasAmountUSD') else None,
                        datetime.fromtimestamp(receiving.get('timestamp', 0)) if receiving.get('timestamp') else None
                    ))

                conn.commit()
                return True

        except Exception as e:
            logger.error(f"Error inserting transaction: {e}")
            return False

    def bulk_insert_transactions(self, transactions: List[Dict[str, Any]]) -> int:
        """Insert multiple transactions efficiently."""
        inserted_count = 0
        for tx in transactions:
            if self.insert_transaction(tx):
                inserted_count += 1
        return inserted_count

    def get_transactions(self,
                        token_symbol: Optional[str] = None,
                        min_usd: Optional[float] = None,
                        max_usd: Optional[float] = None,
                        start_date: Optional[str] = None,
                        end_date: Optional[str] = None,
                        chain_id: Optional[int] = None,
                        limit: int = 1000,
                        offset: int = 0) -> List[Dict[str, Any]]:
        """Query transactions with filters."""

        query = '''
            SELECT
                t.transaction_id,
                t.from_address,
                t.to_address,
                t.tool,
                t.status,
                s.token_symbol as sending_token,
                s.amount_usd as sending_amount_usd,
                s.chain_name as sending_chain,
                s.timestamp as sending_timestamp,
                r.token_symbol as receiving_token,
                r.amount_usd as receiving_amount_usd,
                r.chain_name as receiving_chain,
                t.lifi_explorer_link
            FROM transactions t
            LEFT JOIN sending_transactions s ON t.transaction_id = s.transaction_id
            LEFT JOIN receiving_transactions r ON t.transaction_id = r.transaction_id
            WHERE 1=1
        '''

        params = []

        if token_symbol:
            query += " AND (s.token_symbol = ? OR r.token_symbol = ?)"
            params.extend([token_symbol, token_symbol])

        if min_usd is not None:
            query += " AND (s.amount_usd >= ? OR r.amount_usd >= ?)"
            params.extend([min_usd, min_usd])

        if max_usd is not None:
            query += " AND (s.amount_usd <= ? OR r.amount_usd <= ?)"
            params.extend([max_usd, max_usd])

        if start_date:
            query += " AND s.timestamp >= ?"
            params.append(start_date)

        if end_date:
            query += " AND s.timestamp <= ?"
            params.append(end_date)

        if chain_id:
            query += " AND (s.chain_id = ? OR r.chain_id = ?)"
            params.extend([chain_id, chain_id])

        query += " ORDER BY s.timestamp DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()

            return [dict(row) for row in rows]

    def get_statistics(self) -> Dict[str, Any]:
        """Get database statistics."""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Total transactions
            cursor.execute("SELECT COUNT(*) FROM transactions")
            total_transactions = cursor.fetchone()[0]

            # Total USD volume
            cursor.execute("SELECT SUM(amount_usd) FROM sending_transactions WHERE amount_usd IS NOT NULL")
            total_volume = cursor.fetchone()[0] or 0

            # Top tokens by transaction count
            cursor.execute('''
                SELECT token_symbol, COUNT(*) as count, SUM(amount_usd) as total_volume
                FROM sending_transactions
                WHERE token_symbol IS NOT NULL AND token_symbol != ''
                GROUP BY token_symbol
                ORDER BY count DESC
                LIMIT 10
            ''')
            top_tokens = [dict(row) for row in cursor.fetchall()]

            # Top chains by transaction count
            cursor.execute('''
                SELECT chain_name, COUNT(*) as count, SUM(amount_usd) as total_volume
                FROM sending_transactions
                WHERE chain_name IS NOT NULL
                GROUP BY chain_name
                ORDER BY count DESC
                LIMIT 10
            ''')
            top_chains = [dict(row) for row in cursor.fetchall()]

            # Recent activity (last 24 hours)
            cursor.execute('''
                SELECT COUNT(*)
                FROM sending_transactions
                WHERE timestamp > datetime('now', '-1 day')
            ''')
            recent_transactions = cursor.fetchone()[0]

            # Date range
            cursor.execute('''
                SELECT MIN(timestamp) as earliest, MAX(timestamp) as latest
                FROM sending_transactions
                WHERE timestamp IS NOT NULL
            ''')
            date_range = dict(cursor.fetchone())

            return {
                'total_transactions': total_transactions,
                'total_volume_usd': float(total_volume),
                'recent_transactions_24h': recent_transactions,
                'date_range': date_range,
                'top_tokens': top_tokens,
                'top_chains': top_chains
            }

    def export_to_excel(self, filename: str = "lifi_transactions.xlsx",
                       filters: Optional[Dict[str, Any]] = None) -> str:
        """Export transactions to Excel file."""
        transactions = self.get_transactions(**(filters or {}))

        if not transactions:
            raise ValueError("No transactions found to export")

        df = pd.DataFrame(transactions)

        # Rename columns for better readability
        column_mapping = {
            'transaction_id': 'Transaction ID',
            'from_address': 'From Address',
            'to_address': 'To Address',
            'tool': 'Bridge/Tool',
            'status': 'Status',
            'sending_token': 'Source Token',
            'sending_amount_usd': 'Source Amount (USD)',
            'sending_chain': 'Source Chain',
            'receiving_token': 'Destination Token',
            'receiving_amount_usd': 'Destination Amount (USD)',
            'receiving_chain': 'Destination Chain',
            'sending_timestamp': 'Timestamp',
            'lifi_explorer_link': 'Explorer Link'
        }

        df = df.rename(columns=column_mapping)
        df.to_excel(filename, index=False)
        return filename

    def export_to_json(self, filename: str = "lifi_transactions.json",
                      filters: Optional[Dict[str, Any]] = None) -> str:
        """Export transactions to JSON file."""
        transactions = self.get_transactions(**(filters or {}))

        with open(filename, 'w') as f:
            json.dump(transactions, f, indent=2, default=str)

        return filename

    def export_to_csv(self, filename: str = "lifi_transactions.csv",
                     filters: Optional[Dict[str, Any]] = None) -> str:
        """Export transactions to CSV file."""
        transactions = self.get_transactions(**(filters or {}))

        if not transactions:
            raise ValueError("No transactions found to export")

        df = pd.DataFrame(transactions)
        df.to_csv(filename, index=False)
        return filename

    def clear_database(self):
        """Clear all transaction data."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM receiving_transactions")
            cursor.execute("DELETE FROM sending_transactions")
            cursor.execute("DELETE FROM transactions")
            conn.commit()
            logger.info("Database cleared successfully")

    def get_database_info(self) -> Dict[str, Any]:
        """Get database file information."""
        import os

        if os.path.exists(self.db_path):
            stat = os.stat(self.db_path)
            return {
                'file_exists': True,
                'file_size': stat.st_size,
                'file_size_mb': round(stat.st_size / (1024*1024), 2),
                'last_modified': datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
                'file_path': os.path.abspath(self.db_path)
            }
        else:
            return {
                'file_exists': False,
                'file_path': os.path.abspath(self.db_path)
            }