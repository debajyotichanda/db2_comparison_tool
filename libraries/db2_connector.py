# libraries/db2_connector.py
import ibm_db
import ibm_db_dbi
from robot.api import logger
from typing import Dict, Any, List, Optional


class DB2Connector:
    def __init__(self):
        self.connections = {}

    def connect_to_db2(self, connection_name: str, host: str, port: int,
                       database: str, username: str, password: str) -> None:
        """Connect to DB2 database"""
        conn_str = f"DATABASE={database};HOSTNAME={host};PORT={port};PROTOCOL=TCPIP;UID={username};PWD={password};"

        try:
            conn = ibm_db.connect(conn_str, "", "")
            self.connections[connection_name] = conn
            logger.info(f"Successfully connected to {connection_name}")
        except Exception as e:
            raise Exception(f"Failed to connect to {connection_name}: {str(e)}")

    def execute_query(self, connection_name: str, query: str) -> List[Dict]:
        """Execute SQL query and return results"""
        if connection_name not in self.connections:
            raise Exception(f"Connection {connection_name} not found")

        try:
            conn = self.connections[connection_name]
            stmt = ibm_db.exec_immediate(conn, query)
            results = []

            while True:
                row = ibm_db.fetch_assoc(stmt)
                if not row:
                    break
                results.append(dict(row))

            return results
        except Exception as e:
            raise Exception(f"Query execution failed: {str(e)}")

    def disconnect_all(self) -> None:
        """Close all database connections"""
        for name, conn in self.connections.items():
            ibm_db.close(conn)
            logger.info(f"Disconnected from {name}")
        self.connections.clear()

    # Add this method to get a connection by name
    def get_connection(self, connection_name: str):
        """Get a connection by name"""
        return self.connections.get(connection_name)