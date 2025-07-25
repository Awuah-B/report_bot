#!/usr/bin/env python3
"""
Script to request data from the NPA API and populate PostgreSQL tables
Enhanced with comparison functionality to track new records
"""

import requests
import pandas as pd
import datetime
from io import BytesIO
import sqlalchemy
from sqlalchemy import create_engine, text
import hashlib
from config import get_db_connection_string, get_api_params
from utils import setup_logging
from typing import Dict, Tuple
import traceback

try:
    from fpdf import FPDF
except ImportError:
    FPDF = None

logger = setup_logging('npa_data.log')

class DataFetcher:
    """Handles data fetching and processing from the API"""

    def __init__(self):
        self.today = datetime.datetime.now()
        self.yesterday = self.today - datetime.timedelta(days=1)
        self.date_format = "%d-%m-%Y"
    
    def fetch_data(self) -> Tuple[pd.DataFrame, str]:
        """Fetch data from the API and return as DataFrame"""
        try:
            # Get static parameters from .env via config
            params = get_api_params()
            # Add dynamic date parameters
            params['strQuery2'] = self.yesterday.strftime(self.date_format)
            params['strQuery3'] = self.today.strftime(self.date_format)
            
            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
            }
            response = requests.get(
                "https://iml.npa-enterprise.com/NPAAPILIVE/Home/ExportDailyOrderReport",
                headers=headers,
                params=params,
                timeout=30
            )
            response.raise_for_status()
            df = pd.read_excel(BytesIO(response.content))
            return (df, None) if not df.empty else (None, "Received empty data from API")
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            return None, f"Failed to fetch data: {str(e)}"
        except Exception as e:
            logger.error(f"Unexpected error fetching data: {str(e)}")
            return None, f"Unexpected error: {str(e)}"
        
    def process_data(self, df) -> Tuple[pd.DataFrame, str]:
        """Process and clean the DataFrame"""
        try:
            if df is None or df.empty:
                return None, "No data to process"
            df = df.iloc[7:].copy()
            df = df.astype(str).replace('nan', '', regex=True)
            df = df[~df.apply(lambda row: all(val.strip() == '' for val in row), axis=1)]
            df = df.loc[:, ~df.apply(lambda col: all(val.strip() == '' for val in col), axis=0)]
            df = df[~df.apply(lambda row: any('Total #' in str(val) for val in row), axis=1)]
            last_column_name = df.columns[-1]
            mask = df.apply(lambda row: any(
                "BOST-KUMASI" in str(val) or "BOST - KUMASI" in str(val)
                for val in row
            ), axis=1) | df[last_column_name].str.strip().eq('')
            if not mask.any():
                return None, "No BOST-KUMASI records found"
            df = df[mask]
            if df.empty:
                return None, "No BOST-KUMASI records found"
            columns_to_drop = ['Unnamed: 6', 'Unnamed: 19', 'Unnamed: 20']
            df = df.drop(columns=[col for col in columns_to_drop if col in df.columns], errors='ignore')
            first_col = df.columns[0]
            mask = df.apply(lambda row: (row != '').sum() == 1 and row[first_col] != '', axis=1)
            if mask.any():
                special_rows = df[mask].copy().drop_duplicates(subset=[first_col], keep='first')
                df = pd.concat([df[~mask], special_rows]).sort_index()
            columns = {
                'Unnamed: 0': 'ORDER_DATE',
                'Unnamed: 2': 'ORDER_NUMBER',
                'Unnamed: 5': 'PRODUCTS',
                'Unnamed: 9': 'VOLUME',
                'Unnamed: 10': 'EX_REF_PRICE',
                'Unnamed: 12': 'BRV_NUMBER',
                'Unnamed: 15': 'BDC'
            }
            available_columns = [col for col in columns.keys() if col in df.columns]
            df = df[available_columns].rename(columns=columns)
            return df, None
        except Exception as e:
            logger.error(f"Error processing data: {str(e)}")
            return None, f"Data processing error: {str(e)}"

class PDFGenerator:
    """Handles PDF generation from DataFrame"""
    
    def __init__(self):
        self.font = "Arial"
        
    def generate(self, df, title, footnote):
        """Generate PDF from DataFrame"""
        try:
            if df is None or df.empty:
                return None, "No data available for PDF generation"
            if not FPDF:
                return None, "FPDF library not installed"
                    
            pdf = FPDF(orientation='L', unit='mm', format='A4')
            pdf.set_auto_page_break(auto=True, margin=15)
            pdf.add_page()
                
            pdf.set_font(self.font, 'B', 16)
            pdf.cell(0, 10, title, ln=True, align='C')
            pdf.ln(10)
                
            pdf.set_font(self.font, size=8)
            page_width = pdf.w - 20
            num_cols = len(df.columns)
            col_width = page_width / num_cols
            
            col_widths = [min(col_width, 40) for _ in df.columns]
                
            pdf.set_font(self.font, 'B', 8)
            for col, width in zip(df.columns, col_widths):
                col_text = str(col)[:15] + "..." if len(str(col)) > 15 else str(col)
                pdf.cell(width, 8, col_text, border=1, align='C')
            pdf.ln()
                
            pdf.set_font(self.font, size=7)
            for _, row in df.iterrows():
                if pdf.get_y() + 8 > pdf.h - 15:
                    pdf.add_page()
                    pdf.set_font(self.font, 'B', 8)
                    for col, width in zip(df.columns, col_widths):
                        col_text = str(col)[:15] + "..." if len(str(col)) > 15 else str(col)
                        pdf.cell(width, 8, col_text, border=1, align='C')
                    pdf.ln()
                    pdf.set_font(self.font, size=7)
                    
                for col, width in zip(df.columns, col_widths):
                    cell_text = str(row[col])[:20] + "..." if len(str(row[col])) > 20 else str(row[col])
                    pdf.cell(width, 8, cell_text, border=1)
                pdf.ln()
                
            if footnote:
                pdf.set_y(-20)
                pdf.set_font(self.font, 'I', 7)
                pdf.multi_cell(0, 6, footnote, align='C')

            pdf_output = BytesIO()
            pdf_string = pdf.output(dest='S')
            
            pdf_output.write(pdf_string.encode('utf-8') if isinstance(pdf_string, str) else pdf_string)
            pdf_output.seek(0)
            return pdf_output.getvalue(), None
                
        except Exception as e:
            logger.error(f"PDF generation error: {str(e)}")
            logger.error(f"PDF generation traceback: {traceback.format_exc()}")
            return None, f"PDF generation failed: {str(e)}"

def export_csv():
    """Export CSV with comprehensive error handling"""
    try:
        fetcher = DataFetcher()
        df, error = fetcher.fetch_data()
        if error:
            logger.error(f"Failed to fetch data: {error}")
            return None, error
        
        processed_df, error = fetcher.process_data(df)
        if error:
            logger.error(f"Failed to process data: {error}")
            return None, error
        
        csv_output = BytesIO()
        processed_df.to_csv(csv_output, index=False)
        csv_output.seek(0)
        return csv_output.getvalue(), None
    except Exception as e:
        logger.error(f"Unexpected error during CSV export: {str(e)}")
        return None, f"CSV export failed: {str(e)}"

class TableGenerator:
    """Creates and populates Postgres tables with API data"""
    
    def __init__(self):
        self.columns = [
            'ORDER_DATE', 'ORDER_NUMBER', 'PRODUCTS', 'VOLUME',
            'EX_REF_PRICE', 'BRV_NUMBER', 'BDC'
        ]
        self.table_names = [
            'Approved', 'BDC_Cancel_Order', 'BDC_Decline', 'BRV_Checked',
            'Depot_Manager', 'Good_Standing', 'Loaded', 'Order_Released',
            'Ordered', 'PPMC_Cancel_Order'
        ]
        self.db_connection_string = get_db_connection_string()
        self.engine = None
        self.initialize_tables()
        
    def connect_to_database(self) -> bool:
        """Establish database connection"""
        try:
            self.engine = create_engine(self.db_connection_string, pool_size=15, max_overflow=200)
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            return False
    
    def initialize_tables(self) -> bool:
        """Create necessary tables"""
        try:
            if not self.connect_to_database():
                return False
            for table_name in self.table_names:
                main_sql = self.create_table_schema(table_name, 'main')
                history_sql = self.create_table_schema(table_name, 'history')
                with self.engine.connect() as conn:
                    conn.execute(text(main_sql))
                    conn.execute(text(history_sql))
                    if table_name == 'Depot_Manager':
                        conn.execute(text(self.create_table_schema(table_name, 'new_records')))
                    conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to initialize tables: {str(e)}")
            return False
    
    def create_table_schema(self, table_name: str, table_type: str = 'main') -> str:
        """Generate CREATE TABLE SQL statement"""
        base_columns = """
            id SERIAL PRIMARY KEY,
            order_date VARCHAR(50),
            order_number VARCHAR(100),
            products VARCHAR(200),
            volume VARCHAR(50),
            ex_ref_price VARCHAR(50),
            brv_number VARCHAR(100),
            bdc VARCHAR(200),
            record_hash VARCHAR(64)"""
        timestamp_mapping = {
            'main': 'created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
            'history': 'archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
            'new_records': 'detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
        }
        suffix_mapping = {'main': '', 'history': '_history', 'new_records': '_new_records'}
        return f"""
        CREATE TABLE IF NOT EXISTS {table_name.lower()}{suffix_mapping[table_type]} (
            {base_columns},
            {timestamp_mapping[table_type]}
        );
        """
    
    def generate_record_hash(self, row: pd.Series) -> str:
        """Generate a hash for a record"""
        record_str = ''.join(str(row.get(col, '')) for col in self.columns)
        return hashlib.md5(record_str.encode()).hexdigest()
    
    def delete_duplicate_history_records(self, table_name: str) -> Tuple[int, str]:
        """Delete duplicate records from history table based on record_hash"""
        if table_name not in self.table_names:
            return 0, f"Table {table_name} not found"
        try:
            with self.engine.connect() as conn:
                # Count total records before deduplication
                count_query = text(f"SELECT COUNT(*) FROM {table_name.lower()}_history")
                total_records = conn.execute(count_query).scalar()

                # Delete duplicates, keeping the most recent record
                delete_query = text(f"""
                    DELETE FROM {table_name.lower()}_history
                    WHERE id IN (
                        SELECT id FROM (
                            SELECT id,
                                   ROW_NUMBER() OVER (PARTITION BY record_hash ORDER BY archived_at DESC) as rn
                            FROM {table_name.lower()}_history
                        ) t
                        WHERE rn > 1
                    )
                """)
                result = conn.execute(delete_query)
                conn.commit()
                
                deleted_count = result.rowcount
                return deleted_count, None
        except Exception as e:
            logger.error(f"Failed to delete duplicates from {table_name}_history: {str(e)}")
            return 0, f"Failed to delete duplicates: {str(e)}"
    
    def backup_current_data(self, table_name: str) -> bool:
        """Backup current data to history table"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name.lower()}"))
                if result.scalar() > 0:
                    conn.execute(text(f"""
                    INSERT INTO {table_name.lower()}_history 
                    (order_date, order_number, products, volume, ex_ref_price, brv_number, bdc, record_hash)
                    SELECT order_date, order_number, products, volume, ex_ref_price, brv_number, bdc, record_hash
                    FROM {table_name.lower()}
                    """))
                    conn.commit()
                return True
        except Exception as e:
            logger.error(f"Failed to backup data for {table_name}: {str(e)}")
            return False
    
    def get_existing_hashes(self, table_name: str) -> set:
        """Get existing record hashes"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT record_hash FROM {table_name.lower()}_history"))
                return {row[0] for row in result.fetchall()}
        except Exception as e:
            logger.error(f"Failed to get existing hashes for {table_name}: {str(e)}")
            return set()
    
    def identify_new_records(self, table_name: str, df: pd.DataFrame) -> pd.DataFrame:
        """Identify new records"""
        try:
            existing_hashes = self.get_existing_hashes(table_name)
            df_with_hash = df.copy()
            df_with_hash['record_hash'] = df_with_hash.apply(self.generate_record_hash, axis=1)
            return df_with_hash[~df_with_hash['record_hash'].isin(existing_hashes)]
        except Exception as e:
            logger.error(f"Failed to identify new records for {table_name}: {str(e)}")
            return pd.DataFrame()
    
    def prepare_record_data(self, row: pd.Series, include_hash: bool = True) -> dict:
        """Prepare a record for database insertion"""
        record = {col.lower(): str(row.get(col, '')).strip() or None for col in self.columns}
        if include_hash:
            record['record_hash'] = row.get('record_hash', self.generate_record_hash(row))
        return record
    
    def execute_bulk_insert(self, table_name: str, records: list, table_type: str = 'main') -> bool:
        """Execute bulk insert"""
        if not records:
            return True
        suffix_mapping = {'main': '', 'history': '_history', 'new_records': '_new_records'}
        insert_sql = f"""
        INSERT INTO {table_name.lower()}{suffix_mapping[table_type]} 
        (order_date, order_number, products, volume, ex_ref_price, brv_number, bdc, record_hash)
        VALUES (:order_date, :order_number, :products, :volume, :ex_ref_price, :brv_number, :bdc, :record_hash)
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text(insert_sql), records)
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Failed to insert data into {table_name}{suffix_mapping[table_type]}: {str(e)}")
            return False
    
    def save_new_records(self, table_name: str, new_records_df: pd.DataFrame) -> bool:
        """Save new records"""
        try:
            if new_records_df.empty:
                return True
            with self.engine.connect() as conn:
                conn.execute(text(f"DELETE FROM {table_name.lower()}_new_records"))
                conn.commit()
            records = [self.prepare_record_data(row) for _, row in new_records_df.iterrows()]
            return self.execute_bulk_insert(table_name, records, 'new_records')
        except Exception as e:
            logger.error(f"Failed to save new records for {table_name}: {str(e)}")
            return False
    
    def clear_existing_data(self, table_name: str) -> bool:
        """Clear existing data"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"DELETE FROM {table_name.lower()}"))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Failed to clear data from {table_name}: {str(e)}")
            return False
    
    def find_section_boundaries(self, df: pd.DataFrame) -> Dict[str, Tuple[int, int]]:
        """Find section boundaries in DataFrame"""
        boundaries = {}
        section_starts = {}
        first_col = df.columns[0]
        for idx, row in df.iterrows():
            first_col_value = str(row[first_col]).strip().upper()
            if (row != '').sum() == 1 and first_col_value != '':
                for table_name in self.table_names:
                    normalized_table_name = table_name.replace('_', ' ').upper()
                    if normalized_table_name in first_col_value:
                        section_starts[table_name] = idx
                        break
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
                section_df = section_df[~section_df.apply(
                    lambda row: all(val.strip() == '' for val in row), axis=1
                )]
                section_df = section_df[section_df.apply(
                    lambda row: (row != '').sum() > 1, axis=1
                )]
                if not section_df.empty:
                    section_dataframes[section_name] = section_df
            except Exception as e:
                logger.error(f"Error processing section {section_name}: {str(e)}")
        return section_dataframes
    
    def insert_data_to_table(self, table_name: str, df: pd.DataFrame) -> bool:
        """Insert data into table"""
        records = [self.prepare_record_data(row) for _, row in df.iterrows()]
        return self.execute_bulk_insert(table_name, records, 'main')
    
    def get_new_depot_manager_records(self) -> pd.DataFrame:
        """Retrieve new Depot Manager records"""
        try:
            with self.engine.connect() as conn:
                query = """
                SELECT order_date, order_number, products, volume, ex_ref_price, brv_number, bdc, detected_at
                FROM depot_manager_new_records
                ORDER BY detected_at DESC
                """
                result = conn.execute(text(query))
                records = result.fetchall()
                return pd.DataFrame(records, columns=['order_date', 'order_number', 'products', 'volume', 'ex_ref_price', 'brv_number', 'bdc', 'detected_at']) if records else pd.DataFrame()
        except Exception as e:
            logger.error(f"Failed to retrieve new Depot Manager records: {str(e)}")
            return pd.DataFrame()
    
    def populate_tables(self, section_dataframes: Dict[str, pd.DataFrame]) -> Dict[str, bool]:
        """Populate tables with processed data"""
        results = {}
        if not self.engine and not self.connect_to_database():
            return {table: False for table in self.table_names}
        for table_name in self.table_names:
            try:
                if table_name in section_dataframes:
                    df = section_dataframes[table_name]
                    if table_name == 'Depot_Manager':
                        new_records = self.identify_new_records(table_name, df)
                        self.save_new_records(table_name, new_records)
                    self.backup_current_data(table_name)
                    self.clear_existing_data(table_name)
                    results[table_name] = self.insert_data_to_table(table_name, df)
                else:
                    results[table_name] = True
            except Exception as e:
                logger.error(f"Failed to populate table {table_name}: {str(e)}")
                results[table_name] = False
        return results

def main():
    """Main execution function"""
    try:
        fetcher = DataFetcher()
        df, error = fetcher.fetch_data()
        if error:
            logger.error(f"Failed to fetch data: {error}")
            return False
        logger.info("Data fetched successfully")
        processed_df, error = fetcher.process_data(df)
        if error:
            logger.error(f"Failed to process data: {error}")
            return False
        logger.info("Data processed successfully")
        table_generator = TableGenerator()
        section_dataframes = table_generator.split_dataframe_by_sections(processed_df)
        results = table_generator.populate_tables(section_dataframes)
        failed_tables = [table for table, success in results.items() if not success]
        if failed_tables:
            logger.error(f"Failed to process tables: {failed_tables}")
            return False
        return True
    except Exception as e:
        logger.error(f"Unexpected error in main execution: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)