import sqlite3
import yaml
from datetime import datetime
import sys
import os
import pandas as pd
from sqlalchemy import create_engine, text
from snowflake.sqlalchemy import URL

def inspect_sqlite_database(db_path):
    """
    Inspect a SQLite database and generate a schema configuration.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    schema_config = {
        'business_context': {
            'description': 'Auto-generated schema configuration',
            'key_concepts': [
                'Generated from database inspection',
                'Please update with business-specific concepts'
            ]
        },
        'database_config': {
            'type': 'sqlite',
            'date_functions': {
                'year': "strftime('%Y', {column})",
                'month': "strftime('%Y-%m', {column})",
                'date': "strftime('%Y-%m-%d', {column})"
            }
        },
        'tables': {}
    }
    
    # Inspect each table
    for table_name in tables:
        table_name = table_name[0]
        
        # Get column info
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        
        # Get foreign key info
        cursor.execute(f"PRAGMA foreign_key_list({table_name});")
        foreign_keys = cursor.fetchall()
        
        table_info = {
            'description': f'Table containing {table_name} data',
            'fields': {}
        }
        
        # Process columns
        for col in columns:
            col_id, col_name, col_type, not_null, default_value, is_pk = col
            
            field_info = {
                'type': col_type.upper(),
                'description': f'{col_name} field',
            }
            
            if is_pk:
                field_info['is_key'] = True
            
            # Check if this column is a foreign key
            for fk in foreign_keys:
                if fk[3] == col_name:  # fk[3] is the column name
                    field_info['foreign_key'] = f"{fk[2]}.{fk[4]}"  # fk[2] is referenced table, fk[4] is referenced column
            
            table_info['fields'][col_name] = field_info
        
        # Add relationships based on foreign keys
        if foreign_keys:
            table_info['relationships'] = []
            for fk in foreign_keys:
                relationship = {
                    'table': fk[2],  # Referenced table
                    'type': 'many_to_one',  # Assume many-to-one by default
                    'join_fields': [fk[3], fk[4]]  # Local and referenced columns
                }
                table_info['relationships'].append(relationship)
        
        schema_config['tables'][table_name] = table_info
    
    conn.close()
    return schema_config

def inspect_snowflake_database(engine):
    """
    Inspect a Snowflake database and generate a schema configuration.
    """
    schema_config = {
        'business_context': {
            'description': 'Auto-generated schema configuration for Snowflake TPC-H sample data',
            'key_concepts': [
                'TPC-H is a decision support benchmark',
                'Consists of a suite of business-oriented ad-hoc queries',
                'Models a wholesale supplier managing sales orders'
            ]
        },
        'database_config': {
            'type': 'snowflake',
            'date_functions': {
                'year': "DATE_PART('YEAR', {column})",
                'month': "DATE_TRUNC('MONTH', {column})",
                'date': "DATE_TRUNC('DAY', {column})"
            }
        },
        'tables': {}
    }
    
    with engine.connect() as conn:
        # Get tables using SQLAlchemy text()
        tables_query = text("""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = CURRENT_SCHEMA()
        AND TABLE_TYPE = 'BASE TABLE'
        """)
        result = conn.execute(tables_query)
        tables = [row[0] for row in result]
        
        # Known TPC-H relationships
        tpch_relationships = {
            'ORDERS': [
                {'table': 'CUSTOMER', 'key': 'O_CUSTKEY', 'ref_key': 'C_CUSTKEY'},
            ],
            'LINEITEM': [
                {'table': 'ORDERS', 'key': 'L_ORDERKEY', 'ref_key': 'O_ORDERKEY'},
                {'table': 'PART', 'key': 'L_PARTKEY', 'ref_key': 'P_PARTKEY'},
                {'table': 'SUPPLIER', 'key': 'L_SUPPKEY', 'ref_key': 'S_SUPPKEY'},
            ],
            'PARTSUPP': [
                {'table': 'PART', 'key': 'PS_PARTKEY', 'ref_key': 'P_PARTKEY'},
                {'table': 'SUPPLIER', 'key': 'PS_SUPPKEY', 'ref_key': 'S_SUPPKEY'},
            ],
            'SUPPLIER': [
                {'table': 'NATION', 'key': 'S_NATIONKEY', 'ref_key': 'N_NATIONKEY'},
            ],
            'CUSTOMER': [
                {'table': 'NATION', 'key': 'C_NATIONKEY', 'ref_key': 'N_NATIONKEY'},
            ],
            'NATION': [
                {'table': 'REGION', 'key': 'N_REGIONKEY', 'ref_key': 'R_REGIONKEY'},
            ]
        }
        
        # Known TPC-H primary keys
        tpch_primary_keys = {
            'CUSTOMER': ['C_CUSTKEY'],
            'LINEITEM': ['L_ORDERKEY', 'L_LINENUMBER'],
            'NATION': ['N_NATIONKEY'],
            'ORDERS': ['O_ORDERKEY'],
            'PART': ['P_PARTKEY'],
            'PARTSUPP': ['PS_PARTKEY', 'PS_SUPPKEY'],
            'REGION': ['R_REGIONKEY'],
            'SUPPLIER': ['S_SUPPKEY']
        }
        
        # Inspect each table
        for table_name in tables:
            # Get column info
            columns_query = text(f"""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
            """)
            columns = conn.execute(columns_query)
            
            table_info = {
                'description': get_tpch_table_description(table_name),
                'fields': {}
            }
            
            # Process columns
            for col in columns:
                # Access columns by index since we're using SQLAlchemy's Result object
                field_info = {
                    'type': col[1].upper(),  # DATA_TYPE is at index 1
                    'description': get_tpch_column_description(table_name, col[0]),  # COLUMN_NAME is at index 0
                    'nullable': col[2] == 'YES'  # IS_NULLABLE is at index 2
                }
                
                # Add primary key info from TPC-H schema
                if table_name in tpch_primary_keys and col[0] in tpch_primary_keys[table_name]:
                    field_info['is_key'] = True
                
                table_info['fields'][col[0]] = field_info
            
            # Add relationships based on TPC-H schema
            if table_name in tpch_relationships:
                table_info['relationships'] = []
                for rel in tpch_relationships[table_name]:
                    relationship = {
                        'table': rel['table'],
                        'type': 'many_to_one',
                        'join_fields': [rel['key'], rel['ref_key']]
                    }
                    # Add foreign key info to the field
                    if rel['key'] in table_info['fields']:
                        table_info['fields'][rel['key']]['foreign_key'] = f"{rel['table']}.{rel['ref_key']}"
                    table_info['relationships'].append(relationship)
            
            schema_config['tables'][table_name] = table_info
    
    return schema_config

def get_tpch_table_description(table_name):
    """Get description for TPC-H tables."""
    descriptions = {
        'CUSTOMER': 'Contains customer information including demographics and market segments',
        'LINEITEM': 'Contains the line items of all orders, representing the sales details of each transaction',
        'NATION': 'Contains information about nations/countries',
        'ORDERS': 'Contains all orders made by customers',
        'PART': 'Contains information about parts/products available for sale',
        'PARTSUPP': 'Contains supplier information for parts (price and availability)',
        'REGION': 'Contains information about geographical regions',
        'SUPPLIER': 'Contains supplier information including contact details and location'
    }
    return descriptions.get(table_name, f'Table containing {table_name} data')

def get_tpch_column_description(table_name, column_name):
    """Get description for TPC-H columns."""
    descriptions = {
        'CUSTOMER': {
            'C_CUSTKEY': 'Unique identifier for the customer',
            'C_NAME': 'Customer name',
            'C_ADDRESS': 'Customer address',
            'C_NATIONKEY': 'Reference to the nation where the customer is located',
            'C_PHONE': 'Customer phone number',
            'C_ACCTBAL': 'Customer account balance',
            'C_MKTSEGMENT': 'Market segment to which the customer belongs',
            'C_COMMENT': 'Additional comments about the customer'
        },
        'ORDERS': {
            'O_ORDERKEY': 'Unique identifier for the order',
            'O_CUSTKEY': 'Reference to the customer who placed the order',
            'O_ORDERSTATUS': 'Current status of the order',
            'O_TOTALPRICE': 'Total price of the order',
            'O_ORDERDATE': 'Date when the order was placed',
            'O_ORDERPRIORITY': 'Priority level of the order',
            'O_CLERK': 'Clerk who processed the order',
            'O_SHIPPRIORITY': 'Shipping priority of the order',
            'O_COMMENT': 'Additional comments about the order'
        }
        # Add more table/column descriptions as needed
    }
    return descriptions.get(table_name, {}).get(column_name, f'{column_name} field')

def save_schema_config(config, output_path):
    """Save the schema configuration to a YAML file."""
    with open(output_path, 'w') as f:
        yaml.dump(config, f, sort_keys=False, default_flow_style=False)

def inspect_database(db_type="sqlite", **connection_params):
    """
    Inspect a database and generate a schema configuration.
    Supports both SQLite and Snowflake databases.
    """
    if db_type == "sqlite":
        if 'db_path' not in connection_params:
            raise ValueError("db_path is required for SQLite database inspection")
        return inspect_sqlite_database(connection_params['db_path'])
    
    elif db_type == "snowflake":
        required_params = ['account', 'user', 'password', 'database', 'warehouse', 'schema']
        missing_params = [param for param in required_params if param not in connection_params]
        if missing_params:
            raise ValueError(f"Missing required Snowflake connection parameters: {', '.join(missing_params)}")
        
        engine = create_engine(URL(
            account=connection_params['account'],
            user=connection_params['user'],
            password=connection_params['password'],
            database=connection_params['database'],
            warehouse=connection_params['warehouse'],
            schema=connection_params['schema']
        ))
        
        return inspect_snowflake_database(engine)
    
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python schema_inspector.py <database_type> [output_path] [connection_params]")
        print("Example for SQLite: python schema_inspector.py sqlite sample.db schema_config.yaml")
        print("Example for Snowflake: python schema_inspector.py snowflake snowflake_schema_config.yaml account=xyz user=abc ...")
        sys.exit(1)
    
    db_type = sys.argv[1].lower()
    output_path = sys.argv[2] if len(sys.argv) > 2 else f'{db_type}_schema_config.yaml'
    
    try:
        if db_type == "sqlite":
            if len(sys.argv) < 3:
                print("Error: SQLite database path is required")
                sys.exit(1)
            db_path = sys.argv[2]
            output_path = sys.argv[3] if len(sys.argv) > 3 else 'schema_config.yaml'
            
            if not os.path.exists(db_path):
                print(f"Error: Database file '{db_path}' not found.")
                sys.exit(1)
            
            config = inspect_database(db_type="sqlite", db_path=db_path)
        
        elif db_type == "snowflake":
            # Parse connection parameters
            connection_params = {}
            for param in sys.argv[3:]:
                key, value = param.split('=')
                connection_params[key] = value
            
            config = inspect_database(db_type="snowflake", **connection_params)
        
        else:
            print(f"Error: Unsupported database type '{db_type}'")
            sys.exit(1)
        
        save_schema_config(config, output_path)
        print(f"Schema configuration has been saved to {output_path}")
        print("\nNote: Please review and update the following in the generated config:")
        print("1. Business context description and key concepts")
        print("2. Table and field descriptions")
        print("3. Relationship types (currently defaults to many_to_one)")
        print("4. Query guidelines and tips specific to your data")
    
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
