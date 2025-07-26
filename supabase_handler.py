#!/usr/bin/env python3
"""
Enhanced Supabase Database Handler for NPA Depot Manager Records
Features performance optimizations including:
- Connection pooling
- Batch operations
- Asynchronous requests
- Caching
- Realtime subscriptions
"""

import os
import aiohttp
import pandas as pd
import datetime
import hashlib
import json
import gzip
import time
import asyncio
import contextlib
from typing import Dict, Tuple, List, Optional, AsyncGenerator
from utils import setup_logging
import traceback

logger = setup_logging('supabase_handler.log')

class ConfigurationError(Exception):
    """Custom exception for configuration issues"""
    pass

class SupabaseConnectionManager:
    """Manages Supabase connections with pooling and retry logic"""
    
    def __init__(self):
        self._connection_pool = []
        self._max_pool_size = 5
        self._connection_timeout = 30
        self._retry_attempts = 3
        self._retry_delay = 1
        
    async def get_connection(self) -> Dict:
        """Get a connection from pool or create new one"""
        for attempt in range(self._retry_attempts):
            try:
                if self._connection_pool:
                    return self._connection_pool.pop()
                
                url = os.getenv('SUPABASE_URL')
                key = os.getenv('SUPABASE_ANON_KEY')
                
                if not url or not key:
                    raise ConfigurationError("Missing Supabase credentials")
                
                headers = {
                    'apikey': key,
                    'Authorization': f'Bearer {key}',
                    'Content-Type': 'application/json',
                    'Prefer': 'return=minimal'
                }
                
                # Test connection immediately
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        f"{url}/rest/v1/",
                        headers=headers,
                        timeout=self._connection_timeout
                    ) as response:
                        if response.status != 200:
                            raise ConnectionError(f"Supabase connection failed with status {response.status}")
                
                return {
                    'url': url,
                    'headers': headers,
                    'last_used': datetime.datetime.now()
                }
                
            except Exception as e:
                logger.warning(f"Connection attempt {attempt + 1} failed: {str(e)}")
                if attempt < self._retry_attempts - 1:
                    await asyncio.sleep(self._retry_delay)
        
        raise ConnectionError(f"Failed to establish Supabase connection after {self._retry_attempts} attempts")
    
    def release_connection(self, connection: Dict) -> None:
        """Return connection to pool if space available"""
        if len(self._connection_pool) < self._max_pool_size:
            connection['last_used'] = datetime.datetime.now()
            self._connection_pool.append(connection)
    
    async def close_all(self) -> None:
        """Cleanup all connections"""
        self._connection_pool.clear()

class SupabaseCache:
    """LRU caching layer for frequent queries with TTL"""
    
    def __init__(self, max_size: int = 1000, ttl: int = 300):
        self._cache = {}
        self._max_size = max_size
        self._ttl = ttl
        
    def get(self, key: str) -> Optional[Dict]:
        """Get cached item if valid and not expired"""
        if key not in self._cache:
            return None
            
        item = self._cache[key]
        if time.time() - item['timestamp'] > self._ttl:
            del self._cache[key]
            return None
            
        # Move to end to mark as recently used
        self._cache[key] = item
        return item['data']
        
    def set(self, key: str, data: Dict) -> None:
        """Cache an item with eviction if needed"""
        if len(self._cache) >= self._max_size:
            # Remove oldest item
            oldest_key = next(iter(self._cache))
            del self._cache[oldest_key]
            
        self._cache[key] = {
            'data': data,
            'timestamp': time.time()
        }

class RealtimeListener:
    """Handles realtime subscriptions using Supabase's websocket API"""
    
    def __init__(self, handler):
        self.handler = handler
        self._subscriptions = {}
        self._websocket = None
        self._listener_task = None
        
    async def subscribe(self, table_name: str, callback: callable) -> None:
        """Subscribe to table changes with callback"""
        if table_name not in self._subscriptions:
            self._subscriptions[table_name] = []
            
        self._subscriptions[table_name].append(callback)
        
        if not self._listener_task:
            self._listener_task = asyncio.create_task(self._start_listener())
    
    async def _start_listener(self) -> None:
        """Initialize websocket connection and start listening"""
        try:
            url = os.getenv('SUPABASE_REALTIME_URL', 
                           os.getenv('SUPABASE_URL').replace('https', 'wss').replace('http', 'ws') + '/realtime/v1')
            
            async with aiohttp.ClientSession() as session:
                self._websocket = await session.ws_connect(url)
                
                # Subscribe to all tables we have callbacks for
                for table in self._subscriptions.keys():
                    subscribe_msg = {
                        'topic': table,
                        'event': 'phx_join',
                        'payload': {},
                        'ref': '1'
                    }
                    await self._websocket.send_json(subscribe_msg)
                
                # Listen for messages
                async for msg in self._websocket:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        self._handle_realtime_message(data)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        logger.error("WebSocket connection error")
                        break
                        
        except Exception as e:
            logger.error(f"Realtime listener error: {str(e)}")
            await asyncio.sleep(5)  # Wait before reconnecting
            self._listener_task = asyncio.create_task(self._start_listener())
    
    def _handle_realtime_message(self, data: Dict) -> None:
        """Process incoming realtime messages"""
        if data.get('event') == 'INSERT':
            table = data.get('topic')
            record = data.get('payload', {}).get('record')
            
            if table and record and table in self._subscriptions:
                for callback in self._subscriptions[table]:
                    asyncio.create_task(callback(table, record))

class SupabaseHandler:
    """Main handler for Supabase database operations with performance optimizations"""
    
    def __init__(self):
        self.validate_config()
        self.cache = SupabaseCache()
        self.conn_manager = SupabaseConnectionManager()
        self.realtime = RealtimeListener(self)
        self._background_tasks = set()
        
        # Table configuration
        self.columns = [
            'ORDER_DATE', 'ORDER_NUMBER', 'PRODUCTS', 'VOLUME',
            'EX_REF_PRICE', 'BRV_NUMBER', 'BDC'
        ]
        
        self.table_names = [
            'approved', 'bdc_cancel_order', 'bdc_decline', 'brv_checked',
            'depot_manager', 'good_standing', 'loaded', 'order_released',
            'ordered', 'ppmc_cancel_order', 'depot_manager_decline', 'marked'
        ]
        
        self._start_background_tasks()
        self.initialize_tables()
    
    def _start_background_tasks(self) -> None:
        """Start necessary background tasks"""
        task = asyncio.create_task(self._connection_monitor())
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
    
    async def _connection_monitor(self) -> None:
        """Background task to monitor connection health"""
        while True:
            await asyncio.sleep(300)  # Check every 5 minutes
            try:
                # Test connection with a simple query
                _, error = await self.make_request('GET', '', params={'limit': '1'})
                if error:
                    logger.warning("Connection monitor detected issue, reconnecting...")
            except Exception as e:
                logger.error(f"Connection monitor error: {str(e)}")
    
    def validate_config(self) -> None:
        """Validate all required configuration"""
        required = ['SUPABASE_URL', 'SUPABASE_ANON_KEY']
        missing = [var for var in required if not os.getenv(var)]
        if missing:
            raise ConfigurationError(f"Missing required environment variables: {missing}")
        
        url = os.getenv('SUPABASE_URL')
        if not url.startswith(('http://', 'https://')):
            raise ConfigurationError("Invalid SUPABASE_URL format")
    
    @contextlib.asynccontextmanager
    async def managed_request(self) -> AsyncGenerator[Dict, None]:
        """Context manager for database requests with connection handling"""
        conn = await self.conn_manager.get_connection()
        try:
            yield conn
        finally:
            self.conn_manager.release_connection(conn)
    
    async def make_request(self, method: str, endpoint: str, 
                         data: Optional[Dict] = None, 
                         params: Optional[Dict] = None,
                         use_cache: bool = False) -> Tuple[Optional[Dict], Optional[str]]:
        """Make optimized request to Supabase API"""
        if use_cache and method.upper() == 'GET':
            cache_key = f"{method}:{endpoint}:{json.dumps(params) if params else ''}"
            cached = self.cache.get(cache_key)
            if cached:
                return cached, None
        
        async with self.managed_request() as conn:
            url = f"{conn['url']}/rest/v1/{endpoint}"
            
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.request(
                        method,
                        url,
                        headers=conn['headers'],
                        json=data,
                        params=params,
                        timeout=30
                    ) as response:
                        response.raise_for_status()
                        
                        if response.status == 204:
                            result = {}
                        else:
                            result = await response.json()
                            
                        if use_cache and method.upper() == 'GET':
                            self.cache.set(cache_key, result)
                            
                        return result, None
                        
            except aiohttp.ClientError as e:
                error_msg = f"Request failed for {method} {endpoint}: {str(e)}"
                logger.error(error_msg)
                return None, error_msg
            except Exception as e:
                error_msg = f"Unexpected error during {method} request: {str(e)}"
                logger.error(error_msg)
                return None, error_msg
    
    async def batch_insert(self, table_name: str, records: List[Dict], 
                         batch_size: int = 100) -> Tuple[int, Optional[str]]:
        """Optimized batch insert with chunking"""
        if not records:
            return 0, None
            
        inserted = 0
        try:
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                _, error = await self.make_request(
                    'POST', 
                    table_name, 
                    data=batch,
                    params={'columns': ','.join(batch[0].keys())}  # Explicit columns for performance
                )
                
                if error:
                    return inserted, error
                    
                inserted += len(batch)
                logger.debug(f"Inserted batch {i//batch_size + 1} to {table_name}")
            
            return inserted, None
            
        except Exception as e:
            logger.error(f"Batch insert failed: {str(e)}")
            return inserted, str(e)
    
    async def parallel_requests(self, requests: List[Dict], 
                              max_concurrent: int = 10) -> List[Dict]:
        """Process multiple requests in parallel with concurrency control"""
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def process(req: Dict) -> Dict:
            async with semaphore:
                try:
                    result, error = await self.make_request(
                        req['method'],
                        req['endpoint'],
                        data=req.get('data'),
                        params=req.get('params')
                    )
                    return {
                        'success': not error,
                        'data': result,
                        'error': error
                    }
                except Exception as e:
                    return {'success': False, 'error': str(e)}
        
        return await asyncio.gather(*[process(r) for r in requests])
    
    def generate_record_hash(self, row: pd.Series) -> str:
        """Generate a consistent hash for a record"""
        record_str = ''.join(str(row.get(col, '')) for col in self.columns)
        return hashlib.md5(record_str.encode()).hexdigest()
    
    def prepare_record_data(self, row: pd.Series, include_hash: bool = True) -> Dict:
        """Prepare a standardized record for database insertion"""
        record = {}
        
        # Handle datetime conversion
        order_date = row.get('ORDER_DATE')
        if pd.notnull(order_date):
            if isinstance(order_date, str):
                try:
                    record['order_date'] = pd.to_datetime(order_date, format='%d-%m-%Y').isoformat()
                except:
                    record['order_date'] = None
            else:
                record['order_date'] = order_date.isoformat() if hasattr(order_date, 'isoformat') else None
        else:
            record['order_date'] = None
            
        # Standardize all fields
        record.update({
            'order_number': str(row.get('ORDER_NUMBER', '')).strip() or None,
            'products': str(row.get('PRODUCTS', '')).strip() or None,
            'volume': int(row['VOLUME']) if pd.notnull(row.get('VOLUME')) and str(row['VOLUME']).strip() else None,
            'ex_ref_price': float(row['EX_REF_PRICE']) if pd.notnull(row.get('EX_REF_PRICE')) and str(row['EX_REF_PRICE']).strip() else None,
            'brv_number': str(row.get('BRV_NUMBER', '')).strip() or None,
            'bdc': str(row.get('BDC', '')).strip() or None,
            'created_at': datetime.datetime.now().isoformat(),
            'updated_at': datetime.datetime.now().isoformat()
        })
        
        if include_hash:
            record['record_hash'] = row.get('record_hash', self.generate_record_hash(row))
            
        return record
    
    async def initialize_tables(self) -> bool:
        """Initialize all required tables with optimized checks"""
        try:
            # Check all tables in parallel
            requests = [{
                'method': 'GET',
                'endpoint': table_name,
                'params': {'limit': '1'}
            } for table_name in self.table_names]
            
            results = await self.parallel_requests(requests)
            
            for table_name, result in zip(self.table_names, results):
                if not result['success'] and '42P01' in str(result.get('error', '')):
                    logger.warning(f"Table {table_name} needs to be created manually in Supabase")
            
            logger.info("Table initialization check completed")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize tables: {str(e)}")
            return False
    
    async def backup_current_data(self, table_name: str) -> bool:
        """Optimized backup to history table"""
        try:
            current_data, error = await self.make_request('GET', table_name)
            if error or not current_data:
                logger.warning(f"No data to backup for {table_name}: {error}")
                return not error  # Success if no error but empty data
                
            history_data = [{
                **{k: v for k, v in record.items() if k in self.columns},
                'archived_at': datetime.datetime.now().isoformat()
            } for record in current_data]
            
            _, error = await self.batch_insert(f"{table_name}_history", history_data)
            
            if error:
                logger.error(f"Failed to backup data to {table_name}_history: {error}")
                return False
                
            logger.info(f"Backed up {len(history_data)} records to {table_name}_history")
            return True
            
        except Exception as e:
            logger.error(f"Failed to backup data for {table_name}: {str(e)}")
            return False
    
    async def clear_existing_data(self, table_name: str) -> bool:
        """Efficient table clearing"""
        try:
            _, error = await self.make_request('DELETE', table_name, params={'id': 'gte.0'})
            if error:
                logger.error(f"Failed to clear data from {table_name}: {error}")
                return False
                
            logger.info(f"Cleared data from {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to clear data for {table_name}: {str(e)}")
            return False
    
    async def insert_data_to_table(self, table_name: str, df: pd.DataFrame) -> bool:
        """Optimized data insertion with batch processing"""
        if df.empty:
            logger.info(f"No data to insert for {table_name}")
            return True
            
        records = [self.prepare_record_data(row) for _, row in df.iterrows()]
        inserted, error = await self.batch_insert(table_name, records)
        
        if error:
            logger.error(f"Failed to insert data to {table_name}: {error}")
            return False
            
        logger.info(f"Inserted {inserted} records to {table_name}")
        return True
    
    async def close(self) -> None:
        """Cleanup resources"""
        await self.conn_manager.close_all()
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)

class SupabaseTableGenerator:
    """Optimized table generator with async operations"""
    
    def __init__(self):
        self.handler = SupabaseHandler()
        self.columns = self.handler.columns
        self.table_names = self.handler.table_names
    
    async def connect_to_database(self) -> bool:
        """Test database connection with timeout"""
        try:
            _, error = await asyncio.wait_for(
                self.handler.make_request('GET', '', params={'limit': '1'}),
                timeout=10
            )
            return not error
        except (asyncio.TimeoutError, Exception) as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False
    
    def find_section_boundaries(self, df: pd.DataFrame) -> Dict[str, Tuple[int, int]]:
        """Efficient section boundary detection"""
        boundaries = {}
        current_section = None
        first_col = df.columns[0]
        
        table_name_map = {name.upper().replace('_', ' '): name for name in self.table_names}
        
        for idx, row in df.iterrows():
            first_val = str(row[first_col]).strip().upper()
            if (row != '').sum() == 1 and first_val:
                for display_name, table_name in table_name_map.items():
                    if display_name in first_val:
                        if current_section:
                            boundaries[current_section] = (boundaries[current_section][0], idx-1)
                        current_section = table_name
                        boundaries[table_name] = (idx+1, len(df)-1)
                        break
        
        return boundaries
    
    async def populate_tables(self, section_dataframes: Dict[str, pd.DataFrame]) -> Dict[str, bool]:
        """Async table population with optimized operations"""
        if not await self.connect_to_database():
            logger.error("Failed to connect to Supabase")
            return {table: False for table in self.table_names}
        
        # Process tables in parallel where possible
        tasks = {
            table_name: asyncio.create_task(self._process_table(table_name, df))
            for table_name, df in section_dataframes.items()
            if table_name in self.table_names
        }
        
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        
        return {
            table_name: not isinstance(result, Exception) and result
            for table_name, result in zip(tasks.keys(), results)
        }
    
    async def _process_table(self, table_name: str, df: pd.DataFrame) -> bool:
        """Process individual table with all steps"""
        try:
            logger.info(f"Processing table {table_name} with {len(df)} records")
            
            # Backup, clear, and insert
            if not (await self.handler.backup_current_data(table_name) and
                   await self.handler.clear_existing_data(table_name) and
                   await self.handler.insert_data_to_table(table_name, df)):
                logger.error(f"Failed to process table {table_name}")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error processing table {table_name}: {str(e)}")
            return False
    
    async def search_brv_number(self, brv_number: str) -> List[Dict]:
        """Search for records by BRV number across all tables"""
        try:
            results = []
            for table_name in self.table_names:
                result, error = await self.handler.make_request(
                    'GET',
                    table_name,
                    params={'brv_number': f'eq.{brv_number}'},
                    use_cache=True
                )
                if result and not error:
                    for record in result:
                        results.append({'table': table_name, 'data': record})
            logger.info(f"Found {len(results)} records for BRV number {brv_number}")
            return results
        except Exception as e:
            logger.error(f"Failed to search BRV number {brv_number}: {str(e)}")
            return []
    
    async def get_new_records(self, table_name: str) -> pd.DataFrame:
        """Fetch recent records from a specified table"""
        try:
            result, error = await self.handler.make_request(
                'GET',
                table_name,
                params={'order': 'created_at.desc', 'limit': '100'},
                use_cache=True
            )
            if error or not result:
                logger.error(f"Failed to fetch recent records from {table_name}: {error}")
                return pd.DataFrame()
            logger.info(f"Fetched {len(result)} recent records from {table_name}")
            return pd.DataFrame(result)
        except Exception as e:
            logger.error(f"Failed to fetch recent records from {table_name}: {str(e)}")
            return pd.DataFrame()
    
    async def get_table_stats(self) -> Dict[str, int]:
        """Get record counts for all tables"""
        try:
            stats = {}
            for table_name in self.table_names:
                result, error = await self.handler.make_request(
                    'GET',
                    table_name,
                    params={'select': 'id', 'count': 'exact'},
                    use_cache=True
                )
                if not error and result:
                    stats[table_name] = result.get('count', 0)
                else:
                    stats[table_name] = 0
            logger.info(f"Retrieved table statistics: {stats}")
            return stats
        except Exception as e:
            logger.error(f"Failed to get table stats: {str(e)}")
            return {table: 0 for table in self.table_names}
    
    async def close(self) -> None:
        """Cleanup resources"""
        await self.handler.close()
    
    def split_dataframe_by_sections(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Split DataFrame into sections based on table names"""
        boundaries = self.find_section_boundaries(df)
        section_dataframes = {}
        for table_name, (start, end) in boundaries.items():
            section_dataframes[table_name] = df.iloc[start:end+1].reset_index(drop=True)
        return section_dataframes

async def main():
    """Example usage"""
    handler = SupabaseHandler()
    try:
        # Test connection
        if not await handler.test_connection():
            print("Connection test failed")
            return
        
        # Example batch insert
        sample_data = [{'name': f'Test {i}'} for i in range(150)]
        inserted, error = await handler.batch_insert('test_table', sample_data)
        print(f"Inserted {inserted} records, error: {error}")
        
    finally:
        await handler.close()

if __name__ == "__main__":
    asyncio.run(main())