#!/usr/bin/env python3
"""
001.init_db.py - Initialize SurrealDB with schema for Plan 03: Liveness Record

This script:
1. Removes the existing database (if exists)
2. Creates a fresh database
3. Applies the schema.sql with tolerance_time_ms replaced

Usage:
    python 001.init_db.py [--tolerance 300]
    
Options:
    --tolerance: Tolerance time in seconds (default: 300)
"""

import argparse
import logging
import os
import sys
from pathlib import Path

import requests
import urllib3
import yaml

# Disable SSL warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Script directory
SCRIPT_DIR = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / "config.yaml"
SCHEMA_FILE = SCRIPT_DIR / "schema.sql"


def load_config() -> dict:
    """Load configuration from config.yaml"""
    with open(CONFIG_FILE, 'r') as f:
        return yaml.safe_load(f)


def load_schema(tolerance_time_ms: int) -> str:
    """Load schema.sql and replace placeholders"""
    with open(SCHEMA_FILE, 'r') as f:
        schema_sql = f.read()
    
    # Replace tolerance_time_ms placeholder
    schema_sql = schema_sql.replace('{tolerance_time_ms}', str(tolerance_time_ms))
    logger.info(f"Loaded schema from {SCHEMA_FILE} with tolerance_time_ms={tolerance_time_ms}")
    
    return schema_sql


class SurrealDBClient:
    """Simple SurrealDB HTTP client"""
    
    def __init__(self, config: dict):
        self.url = config['url'].rstrip('/')
        self.username = config['username']
        self.password = config['password']
        self.namespace = config['namespace']
        self.database = config['database']
        self.session = requests.Session()
        self.session.auth = (self.username, self.password)
        self.session.verify = False  # For self-signed certs
    
    def execute(self, query: str, use_db: bool = True) -> dict:
        """Execute a SurrealQL query"""
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'text/plain',
            'surreal-ns': self.namespace,
        }
        if use_db:
            headers['surreal-db'] = self.database
        
        response = self.session.post(
            f"{self.url}/sql",
            headers=headers,
            data=query.encode('utf-8')
        )
        response.raise_for_status()
        return response.json()
    
    def remove_database(self):
        """Remove the database if it exists"""
        logger.info(f"Removing database '{self.database}' if exists...")
        query = f"REMOVE DATABASE IF EXISTS {self.database};"
        try:
            result = self.execute(query, use_db=False)
            logger.info(f"Remove database result: {result}")
        except Exception as e:
            logger.warning(f"Failed to remove database (may not exist): {e}")
    
    def create_database(self):
        """Create the database"""
        logger.info(f"Creating database '{self.database}'...")
        query = f"DEFINE DATABASE {self.database};"
        result = self.execute(query, use_db=False)
        logger.info(f"Create database result: {result}")
    
    def apply_schema(self, schema_sql: str):
        """Apply schema to the database"""
        logger.info("Applying schema...")
        
        # Split schema into statements and execute
        # SurrealDB can handle multiple statements in one request
        result = self.execute(schema_sql, use_db=True)
        
        # Check for errors
        error_count = 0
        success_count = 0
        for item in result:
            if item.get('status') == 'ERR':
                error_count += 1
                logger.error(f"Schema error: {item.get('result')}")
            else:
                success_count += 1
        
        logger.info(f"Schema applied: {success_count} success, {error_count} errors")
        
        if error_count > 0:
            raise RuntimeError(f"Schema application had {error_count} errors")
    
    def verify_schema(self):
        """Verify the schema was applied correctly"""
        logger.info("Verifying schema...")
        
        result = self.execute("INFO FOR DB;", use_db=True)
        
        if result and result[0].get('status') == 'OK':
            db_info = result[0].get('result', {})
            tables = db_info.get('tables', {})
            functions = db_info.get('functions', {})
            
            logger.info(f"Database has {len(tables)} tables and {len(functions)} functions")
            
            # Check critical tables
            critical_tables = [
                'pod', 'node', 'container', 'deployment', 'replicaset', 'service', 'system',
                'pod_to_pod', 'pod_to_system',
                'node_with_pod', 'node_with_system', 'container_with_pod',
                'pod_with_service', 'deployment_with_replicaset', 'pod_with_replicaset'
            ]
            
            missing_tables = [t for t in critical_tables if t not in tables]
            if missing_tables:
                logger.error(f"Missing tables: {missing_tables}")
                return False
            
            # Verify relation tables have source_id/target_id fields by querying table info
            relation_tables = ['pod_to_pod', 'pod_to_system', 'node_with_pod', 'node_with_system',
                               'container_with_pod', 'pod_with_service', 'deployment_with_replicaset',
                               'pod_with_replicaset']
            
            for table_name in relation_tables:
                # Query table fields
                fields_result = self.execute(f"INFO FOR TABLE {table_name};", use_db=True)
                if fields_result and fields_result[0].get('status') == 'OK':
                    table_info = fields_result[0].get('result', {})
                    fields = table_info.get('fields', {})
                    
                    if 'source_id' not in fields or 'target_id' not in fields:
                        logger.error(f"Table '{table_name}' missing source_id/target_id fields: {list(fields.keys())}")
                        return False
                    logger.info(f"✓ {table_name}: source_id/target_id verified")
                else:
                    logger.error(f"Failed to get info for table '{table_name}': {fields_result}")
                    return False
            
            logger.info("Schema verification passed!")
            return True
        else:
            logger.error(f"Failed to get database info: {result}")
            return False


def main():
    parser = argparse.ArgumentParser(description='Initialize SurrealDB with Plan 03 schema')
    parser.add_argument('--tolerance', type=int, default=300,
                        help='Tolerance time in seconds (default: 300)')
    parser.add_argument('--skip-remove', action='store_true',
                        help='Skip removing existing database')
    args = parser.parse_args()
    
    # Load config
    config = load_config()
    surreal_config = config['surreal_db']
    
    logger.info(f"Initializing SurrealDB at {surreal_config['url']}")
    logger.info(f"Namespace: {surreal_config['namespace']}, Database: {surreal_config['database']}")
    logger.info(f"Tolerance time: {args.tolerance} seconds")
    
    # Load schema
    schema_sql = load_schema(args.tolerance)
    
    # Initialize client
    client = SurrealDBClient(surreal_config)
    
    try:
        # Step 1: Remove existing database
        if not args.skip_remove:
            client.remove_database()
        
        # Step 2: Create database
        client.create_database()
        
        # Step 3: Apply schema
        client.apply_schema(schema_sql)
        
        # Step 4: Verify schema
        if client.verify_schema():
            logger.info("=" * 60)
            logger.info("Database initialization completed successfully!")
            logger.info("=" * 60)
            return 0
        else:
            logger.error("Schema verification failed!")
            return 1
            
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
