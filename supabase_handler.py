#!/usr/bin/env python3
"""
Supabase Database Handler for NPA Depot Manager Records
Replaces PostgreSQL logic with Supabase REST API calls
"""

import os
import requests
import pandas as pd
import datetime
import hashlib
import json
from typing import Dict, Tuple, List, Optional
from utils import setup_logging
import traceback

logger = setup_logging('supabase_handler.log')

class SupabaseHandler:
    """Handles all database operations using Supabase REST API"""
    
    def __init__(self):
        # Load Supabase credentials from environment variables
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_ANON_KEY')  # or SERVICE_ROLE_KEY for admin operations
        
        if not self.supabase_url or not self.supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_ANON_KEY must be set in environment variables")
        
        self.headers = {
            'apikey': self.supabase_key,
            'Authorization': f'Bearer {self.supabase_key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=minimal'
        }
        
        self.columns = [
            'ORDER_DATE', 'ORDER_NUMBER', 'PRODUCTS', 'VOLUME',
            'EX_REF_PRICE', 'BRV_NUMBER', 'BDC'
        ]
        
        self.table_names = [
            'approved', 'bdc_cancel_order', 'bdc_decline', 'brv_checked',
            'depot_manager', 'good_standing', 'loaded', 'order_released',
            'ordered', 'ppmc_cancel_order', 'depot_manager_decline', 'marked'
        ]
        
        # Initialize tables on startup
        self.initialize_tables()
    
    def make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, params: Optional[Dict] = None) -> Tuple[Optional[Dict], Optional[str]]:
        """Make HTTP request to Supabase API with error handling"""
        try:
            url = f"{self.supabase_url}/rest/v1/{endpoint}"
            
            if method.upper() == 'GET':
                response = requests.get(url, headers=self.headers, params=params, timeout=30)
            elif method.upper() == 'POST':
                response = requests.post(url, headers=self.headers, json=data, timeout=30)
            elif method.upper() == 'DELETE':
                response = requests.delete(url, headers=self.headers, params=params, timeout=30)
            elif method.upper() == 'PATCH':
                response = requests.patch(url, headers=self.headers, json=data, params=params, timeout=30)
            else:
                return None, f"Unsupported HTTP method: {method}"
                
            response.raise_for_status()
            
            # Handle empty responses
            if response.status_code == 204 or not response.content:
                return {}, None
                
            return response.json(), None
            
        except requests.exceptions.Timeout:
            error_msg = f"Timeout during {method} request to {endpoint}"
            logger.error(error_msg)
            return None, error_msg
        except requests.exceptions.RequestException as e:
            error_msg = f"Request failed for {method} {endpoint}: {str(e)}"
            logger.error(error_msg)
            return None, error_msg
        except json.JSONDecodeError as e:
            error_msg = f"Failed to parse JSON response: {str(e)}"
            logger.error(error_msg)
            return None, error_msg
        except Exception as e:
            error_msg = f"Unexpected error during {method} request: {str(e)}"
            logger.error(error_msg)
            return None, error_msg
    
    def initialize_tables(self) -> bool:
        """Initialize all required tables using Supabase SQL functions"""
        try:
            # Create tables using Supabase RPC (stored procedure)
            # You would need to create this function in Supabase SQL editor first
            for table_name in self.table_names:
                # Check if table exists
                result, error = self.make_request('GET', f'{table_name}', params={'limit': '1'})
                if error and '42P01' in str(error):  # Table doesn't exist
                    logger.info(f"Table {table_name} needs to be created manually in Supabase")
                    # For now, we'll assume tables are created manually in Supabase
                    # You can create them using the SQL editor in Supabase dashboard
                
            logger.info("Table initialization check completed")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize tables: {str(e)}")
            return False
    
    def generate_record_hash(self, row: pd.Series) -> str:
        """Generate a hash for a record"""
        record_str = ''.join(str(row.get(col, '')) for col in self.columns)
        return hashlib.md5(record_str.encode()).hexdigest()
    
    def prepare_record_data(self, row: pd.Series, include_hash: bool = True) -> dict:
        """Prepare a record for database insertion"""
        record = {}
        
        # Handle datetime conversion
        order_date = row.get('ORDER_DATE')
        if pd.notnull(order_date):
            if isinstance(order_date, str):
                try:
                    record['order_date'] = pd.to_datetime(order_date).isoformat()
                except:
                    record['order_date'] = None
            else:
                record['order_date'] = order_date.isoformat() if hasattr(order_date, 'isoformat') else None
        else:
            record['order_date'] = None
            
        record['order_number'] = str(row.get('ORDER_NUMBER', '')).strip() or None
        record['products'] = str(row.get('PRODUCTS', '')).strip() or None
        
        # Handle numeric fields
        volume = row.get('VOLUME')
        record['volume'] = int(volume) if pd.notnull(volume) and str(volume).strip() != '' else None
        
        price = row.get('EX_REF_PRICE')
        record['ex_ref_price'] = float(price) if pd.notnull(price) and str(price).strip() != '' else None
        
        record['brv_number'] = str(row.get('BRV_NUMBER', '')).strip() or None
        record['bdc'] = str(row.get('BDC', '')).strip() or None
        
        if include_hash:
            record['record_hash'] = row.get('record_hash', self.generate_record_hash(row))
            
        # Add timestamps
        record['created_at'] = datetime.datetime.now().isoformat()
        record['updated_at'] = datetime.datetime.now().isoformat()
        
        return record
    
    def get_existing_hashes(self, table_name: str) -> set:
        """Get existing record hashes from history table"""
        try:
            history_table = f"{table_name}_history"
            result, error = self.make_request('GET', history_table, params={'select': 'record_hash'})
            
            if error:
                logger.warning(f"Could not fetch existing hashes for {table_name}: {error}")
                return set()
                
            if result:
                return {record['record_hash'] for record in result if record.get('record_hash')}
            return set()
            
        except Exception as e:
            logger.error(f"Failed to get existing hashes for {table_name}: {str(e)}")
            return set()
    
    def identify_new_records(self, table_name: str, df: pd.DataFrame) -> pd.DataFrame:
        """Identify new records by comparing hashes"""
        try:
            existing_hashes = self.get_existing_hashes(table_name)
            df_with_hash = df.copy()
            df_with_hash['record_hash'] = df_with_hash.apply(self.generate_record_hash, axis=1)
            
            new_records = df_with_hash[~df_with_hash['record_hash'].isin(existing_hashes)]
            logger.info(f"Found {len(new_records)} new records for {table_name}")
            return new_records
            
        except Exception as e:
            logger.error(f"Failed to identify new records for {table_name}: {str(e)}")
            return pd.DataFrame()
    
    def backup_current_data(self, table_name: str) -> bool:
        """Backup current data to history table"""
        try:
            # Get current data
            current_data, error = self.make_request('GET', table_name)
            if error:
                logger.error(f"Failed to fetch current data for backup: {error}")
                return False
                
            if not current_data:
                logger.info(f"No data to backup for {table_name}")
                return True
            
            # Prepare data for history table
            history_data = []
            for record in current_data:
                history_record = {
                    'order_date': record.get('order_date'),
                    'order_number': record.get('order_number'),
                    'products': record.get('products'),
                    'volume': record.get('volume'),
                    'ex_ref_price': record.get('ex_ref_price'),
                    'brv_number': record.get('brv_number'),
                    'bdc': record.get('bdc'),
                    'record_hash': record.get('record_hash'),
                    'archived_at': datetime.datetime.now().isoformat()
                }
                history_data.append(history_record)
            
            # Insert into history table
            history_table = f"{table_name}_history"
            result, error = self.make_request('POST', history_table, data=history_data)
            
            if error:
                logger.error(f"Failed to backup data to {history_table}: {error}")
                return False
                
            logger.info(f"Successfully backed up {len(history_data)} records to {history_table}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to backup data for {table_name}: {str(e)}")
            return False
    
    def clear_existing_data(self, table_name: str) -> bool:
        """Clear existing data from main table"""
        try:
            # Delete all records (Supabase requires a condition, so we use a always-true condition)
            result, error = self.make_request('DELETE', table_name, params={'id': 'gte.0'})
            
            if error:
                logger.error(f"Failed to clear data from {table_name}: {error}")
                return False
                
            logger.info(f"Successfully cleared data from {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to clear data from {table_name}: {str(e)}")
            return False
    
    def insert_data_to_table(self, table_name: str, df: pd.DataFrame) -> bool:
        """Insert data into main table"""
        try:
            if df.empty:
                logger.info(f"No data to insert for {table_name}")
                return True
                
            # Prepare records for insertion
            records = []
            for _, row in df.iterrows():
                record = self.prepare_record_data(row)
                records.append(record)
            
            # Insert in batches to avoid payload size limits
            batch_size = 100
            total_inserted = 0
            
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                result, error = self.make_request('POST', table_name, data=batch)
                
                if error:
                    logger.error(f"Failed to insert batch {i//batch_size + 1} for {table_name}: {error}")
                    return False
                    
                total_inserted += len(batch)
                logger.info(f"Inserted batch {i//batch_size + 1} ({len(batch)} records) for {table_name}")
            
            logger.info(f"Successfully inserted {total_inserted} records to {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert data to {table_name}: {str(e)}")
            return False
    
    def save_new_records(self, table_name: str, new_records_df: pd.DataFrame) -> bool:
        """Save new records to the new_records table"""
        try:
            if new_records_df.empty:
                logger.info(f"No new records to save for {table_name}")
                return True
            
            new_records_table = f"{table_name}_new_records"
            
            # Clear existing new records
            self.make_request('DELETE', new_records_table, params={'id': 'gte.0'})
            
            # Prepare new records for insertion
            records = []
            for _, row in new_records_df.iterrows():
                record = self.prepare_record_data(row)
                record['detected_at'] = datetime.datetime.now().isoformat()
                records.append(record)
            
            # Insert new records
            result, error = self.make_request('POST', new_records_table, data=records)
            
            if error:
                logger.error(f"Failed to save new records for {table_name}: {error}")
                return False
                
            logger.info(f"Successfully saved {len(records)} new records for {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save new records for {table_name}: {str(e)}")
            return False
    
    def get_new_depot_manager_records(self) -> pd.DataFrame:
        """Retrieve new Depot Manager records"""
        try:
            result, error = self.make_request('GET', 'depot_manager_new_records', 
                                             params={'order': 'detected_at.desc'})
            
            if error:
                logger.error(f"Failed to retrieve new Depot Manager records: {error}")
                return pd.DataFrame()
            
            if not result:
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(result)
            
            # Ensure required columns exist
            required_columns = ['order_date', 'order_number', 'products', 'volume', 
                              'ex_ref_price', 'brv_number', 'bdc', 'detected_at']
            
            for col in required_columns:
                if col not in df.columns:
                    df[col] = None
            
            return df[required_columns]
            
        except Exception as e:
            logger.error(f"Failed to retrieve new Depot Manager records: {str(e)}")
            return pd.DataFrame()
    
    def search_brv_number(self, brv_number: str) -> List[Dict]:
        """Search for a specific BRV number across all tables"""
        try:
            found_records = []
            
            for table_name in self.table_names:
                try:
                    # Search in main table
                    result, error = self.make_request('GET', table_name, 
                                                     params={'brv_number': f'eq.{brv_number}'})
                    
                    if not error and result:
                        for record in result:
                            found_records.append({
                                'table': table_name,
                                'data': record
                            })
                            
                except Exception as e:
                    logger.error(f"Error searching in table {table_name}: {e}")
                    continue
            
            logger.info(f"Found {len(found_records)} records for BRV number {brv_number}")
            return found_records
            
        except Exception as e:
            logger.error(f"Failed to search for BRV number {brv_number}: {str(e)}")
            return []
    
    def get_table_stats(self) -> Dict[str, int]:
        """Get statistics for all tables"""
        try:
            stats = {}
            
            for table_name in self.table_names:
                try:
                    result, error = self.make_request('GET', table_name, 
                                                     params={'select': 'id', 'limit': '0'})
                    
                    if not error:
                        # Supabase returns count in response headers, but we'll do a simple count
                        count_result, count_error = self.make_request('GET', table_name, 
                                                                     params={'select': 'count'})
                        stats[table_name] = len(count_result) if count_result else 0
                    else:
                        stats[table_name] = 0
                        
                except Exception as e:
                    logger.error(f"Error getting stats for {table_name}: {e}")
                    stats[table_name] = 0
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get table stats: {str(e)}")
            return {}
    
    def test_connection(self) -> bool:
        """Test Supabase connection"""
        try:
            # Try to access any table to test connection
            result, error = self.make_request('GET', 'depot_manager', params={'limit': '1'})
            
            if error:
                logger.error(f"Supabase connection test failed: {error}")
                return False
                
            logger.info("Supabase connection test successful")
            return True
            
        except Exception as e:
            logger.error(f"Supabase connection test failed: {str(e)}")
            return False

class SupabaseTableGenerator:
    """Replaces the original TableGenerator with Supabase functionality"""
    
    def __init__(self):
        self.supabase_handler = SupabaseHandler()
        self.columns = self.supabase_handler.columns
        self.table_names = self.supabase_handler.table_names
    
    def connect_to_database(self) -> bool:
        """Test database connection"""
        return self.supabase_handler.test_connection()
    
    def find_section_boundaries(self, df: pd.DataFrame) -> Dict[str, Tuple[int, int]]:
        """Find section boundaries in DataFrame"""
        boundaries = {}
        section_starts = {}
        first_col = df.columns[0]
        
        # Map display names to table names
        table_name_map = {
            'APPROVED': 'approved',
            'BDC CANCEL ORDER': 'bdc_cancel_order',
            'BDC DECLINE': 'bdc_decline',
            'BRV CHECKED': 'brv_checked',
            'DEPOT MANAGER': 'depot_manager',
            'GOOD STANDING': 'good_standing',
            'LOADED': 'loaded',
            'ORDER RELEASED': 'order_released',
            'ORDERED': 'ordered',
            'PPMC CANCEL ORDER': 'ppmc_cancel_order',
            'DEPOT MANAGER DECLINE': 'depot_manager_decline',
            'MARKED': 'marked'
        }
        
        for idx, row in df.iterrows():
            first_col_value = str(row[first_col]).strip().upper()
            if (row != '').sum() == 1 and first_col_value != '':
                for display_name, table_name in table_name_map.items():
                    if display_name in first_col_value:
                        section_starts[table_name] = idx
                        break
        
        # Calculate boundaries
        sorted_sections = sorted(section_starts.items(), key=lambda x: x[1])
        for i, (section_name, start_idx) in enumerate(sorted_sections):
            end_idx = sorted_sections[i + 1][1] - 1 if i < len(sorted_sections) - 1 else df.index[-1]
            boundaries[section_name] = (start_idx + 1, end_idx)
        
        return boundaries
    
    def split_dataframe_by_sections(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Split DataFrame into sections"""
        boundaries = self.find_section_boundaries(df)
        section_dataframes = {}
        
        for section_name, (start_idx, end_idx) in boundaries.items():
            try:
                section_df = df.loc[start_idx:end_idx].copy()
                
                # Remove empty rows
                section_df = section_df[~section_df.apply(
                    lambda row: all(val.strip() == '' for val in row), axis=1
                )]
                
                # Remove rows with only one non-empty cell
                section_df = section_df[section_df.apply(
                    lambda row: (row != '').sum() > 1, axis=1
                )]
                
                if not section_df.empty:
                    section_dataframes[section_name] = section_df
                    logger.info(f"Section {section_name}: {len(section_df)} records")
                    
            except Exception as e:
                logger.error(f"Error processing section {section_name}: {str(e)}")
        
        return section_dataframes
    
    def populate_tables(self, section_dataframes: Dict[str, pd.DataFrame]) -> Dict[str, bool]:
        """Populate tables with processed data using Supabase"""
        results = {}
        
        if not self.connect_to_database():
            logger.error("Failed to connect to Supabase")
            return {table: False for table in self.table_names}
        
        for table_name in self.table_names:
            try:
                if table_name in section_dataframes:
                    df = section_dataframes[table_name]
                    logger.info(f"Processing table {table_name} with {len(df)} records")
                    
                    # For depot_manager table, identify and save new records
                    if table_name == 'depot_manager':
                        new_records = self.supabase_handler.identify_new_records(table_name, df)
                        if not new_records.empty:
                            self.supabase_handler.save_new_records(table_name, new_records)
                            logger.info(f"Saved {len(new_records)} new records for {table_name}")
                    
                    # Backup current data
                    if not self.supabase_handler.backup_current_data(table_name):
                        logger.warning(f"Failed to backup data for {table_name}")
                    
                    # Clear existing data
                    if not self.supabase_handler.clear_existing_data(table_name):
                        logger.error(f"Failed to clear data for {table_name}")
                        results[table_name] = False
                        continue
                    
                    # Insert new data
                    results[table_name] = self.supabase_handler.insert_data_to_table(table_name, df)
                    
                    if results[table_name]:
                        logger.info(f"Successfully populated {table_name}")
                    else:
                        logger.error(f"Failed to populate {table_name}")
                else:
                    logger.info(f"No data found for table {table_name}")
                    results[table_name] = True
                    
            except Exception as e:
                logger.error(f"Failed to populate table {table_name}: {str(e)}")
                logger.error(f"Traceback: {traceback.format_exc()}")
                results[table_name] = False
        
        return results
    
    def get_new_depot_manager_records(self) -> pd.DataFrame:
        """Get new depot manager records"""
        return self.supabase_handler.get_new_depot_manager_records()
    
    def search_brv_number(self, brv_number: str) -> List[Dict]:
        """Search for BRV number across tables"""
        return self.supabase_handler.search_brv_number(brv_number)
    
    def get_table_stats(self) -> Dict[str, int]:
        """Get table statistics"""
        return self.supabase_handler.get_table_stats()
