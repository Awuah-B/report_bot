#!/usr/bin/env python3
"""
Script to request data from the NPA API and populate Supabase tables
Enhanced with comparison functionality to track new records
Migrated from PostgreSQL to Supabase for better reliability and scalability
"""

import requests
import pandas as pd
import datetime
from io import BytesIO
import hashlib
from config import get_api_params
from utils import setup_logging
from typing import Dict, Tuple
import traceback
from supabase_handler import SupabaseTableGenerator

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
            
            logger.info(f"Fetching data for date range: {params['strQuery2']} to {params['strQuery3']}")
            
            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
            }
            
            response = requests.get(
                "https://iml.npa-enterprise.com/NPAAPILIVE/Home/ExportDailyOrderReport",
                headers=headers,
                params=params,
                timeout=60  # Increased timeout for better reliability
            )
            response.raise_for_status()
            
            # Read Excel data
            df = pd.read_excel(BytesIO(response.content))
            
            if df.empty:
                logger.warning("Received empty data from API")
                return None, "Received empty data from API"
            
            logger.info(f"Successfully fetched {len(df)} rows from API")
            return df, None
            
        except requests.exceptions.Timeout:
            error_msg = "API request timed out"
            logger.error(error_msg)
            return None, error_msg
        except requests.exceptions.RequestException as e:
            error_msg = f"API request failed: {str(e)}"
            logger.error(error_msg)
            return None, error_msg
        except Exception as e:
            error_msg = f"Unexpected error fetching data: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None, error_msg
        
    def process_data(self, df) -> Tuple[pd.DataFrame, str]:
        """Process and clean the DataFrame"""
        try:
            if df is None or df.empty:
                return None, "No data to process"
            
            logger.info(f"Processing DataFrame with {len(df)} rows and {len(df.columns)} columns")
            
            # Remove header rows (first 7 rows are usually headers)
            df = df.iloc[7:].copy()
            
            # Convert NaN to empty strings and clean data
            df = df.astype(str).replace('nan', '', regex=True)
            
            # Remove completely empty rows
            df = df[~df.apply(lambda row: all(val.strip() == '' for val in row), axis=1)]
            
            # Remove completely empty columns
            df = df.loc[:, ~df.apply(lambda col: all(val.strip() == '' for val in col), axis=0)]
            
            # Remove rows containing totals
            df = df[~df.apply(lambda row: any('Total #' in str(val) for val in row), axis=1)]
            
            # Filter for BOST-KUMASI records
            last_column_name = df.columns[-1]
            mask = df.apply(lambda row: any(
                "BOST-KUMASI" in str(val) or "BOST - KUMASI" in str(val)
                for val in row
            ), axis=1) | df[last_column_name].str.strip().eq('')
            
            if not mask.any():
                logger.warning("No BOST-KUMASI records found in data")
                return None, "No BOST-KUMASI records found"
            
            df = df[mask]
            
            if df.empty:
                logger.warning("No records remain after BOST-KUMASI filtering")
                return None, "No BOST-KUMASI records found"
            
            logger.info(f"Found {len(df)} BOST-KUMASI records")
            
            # Drop unnecessary columns
            columns_to_drop = ['Unnamed: 6', 'Unnamed: 19', 'Unnamed: 20']
            df = df.drop(columns=[col for col in columns_to_drop if col in df.columns], errors='ignore')
            
            # Handle special single-cell rows (section headers)
            first_col = df.columns[0]
            mask = df.apply(lambda row: (row != '').sum() == 1 and row[first_col] != '', axis=1)
            if mask.any():
                special_rows = df[mask].copy().drop_duplicates(subset=[first_col], keep='first')
                df = pd.concat([df[~mask], special_rows]).sort_index()
            
            # Rename columns to standardized names
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
            
            logger.info(f"Renamed columns: {list(df.columns)}")
            
            # Convert data types to match database schema
            try:
                df['ORDER_DATE'] = pd.to_datetime(df['ORDER_DATE'], errors='coerce')
                df['VOLUME'] = pd.to_numeric(df['VOLUME'], errors='coerce').astype('Int64')
                df['EX_REF_PRICE'] = pd.to_numeric(df['EX_REF_PRICE'], errors='coerce').astype(float)
                df['ORDER_NUMBER'] = df['ORDER_NUMBER'].astype(str)
                df['PRODUCTS'] = df['PRODUCTS'].astype(str)
                df['BRV_NUMBER'] = df['BRV_NUMBER'].astype(str)
                df['BDC'] = df['BDC'].astype(str)
                
                logger.info("Successfully converted data types")
            except Exception as e:
                logger.warning(f"Error converting data types: {str(e)}")
            
            # Remove any remaining empty or invalid rows
            df = df.dropna(how='all')
            
            logger.info(f"Final processed DataFrame: {len(df)} rows")
            return df, None
            
        except Exception as e:
            error_msg = f"Error processing data: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None, error_msg

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
            
            logger.info(f"Generating PDF with {len(df)} records")
            
            pdf = FPDF(orientation='L', unit='mm', format='A4')
            pdf.set_auto_page_break(auto=True, margin=15)
            pdf.add_page()
            
            # Add title
            pdf.set_font(self.font, 'B', 16)
            pdf.cell(0, 10, title, ln=True, align='C')
            pdf.ln(10)
            
            # Calculate column widths
            pdf.set_font(self.font, size=8)
            page_width = pdf.w - 20
            num_cols = len(df.columns)
            col_width = page_width / num_cols
            
            col_widths = [min(col_width, 40) for _ in df.columns]
            
            # Add headers
            pdf.set_font(self.font, 'B', 8)
            for col, width in zip(df.columns, col_widths):
                col_text = str(col)[:15] + "..." if len(str(col)) > 15 else str(col)
                pdf.cell(width, 8, col_text, border=1, align='C')
            pdf.ln()
            
            # Add data rows
            pdf.set_font(self.font, size=7)
            for _, row in df.iterrows():
                # Check if we need a new page
                if pdf.get_y() + 8 > pdf.h - 15:
                    pdf.add_page()
                    # Re-add headers on new page
                    pdf.set_font(self.font, 'B', 8)
                    for col, width in zip(df.columns, col_widths):
                        col_text = str(col)[:15] + "..." if len(str(col)) > 15 else str(col)
                        pdf.cell(width, 8, col_text, border=1, align='C')
                    pdf.ln()
                    pdf.set_font(self.font, size=7)
                
                # Add data cells
                for col, width in zip(df.columns, col_widths):
                    cell_value = row[col]
                    if pd.isna(cell_value):
                        cell_text = ""
                    else:
                        cell_text = str(cell_value)[:20] + "..." if len(str(cell_value)) > 20 else str(cell_value)
                    pdf.cell(width, 8, cell_text, border=1)
                pdf.ln()
            
            # Add footnote
            if footnote:
                pdf.set_y(-20)
                pdf.set_font(self.font, 'I', 7)
                pdf.multi_cell(0, 6, footnote, align='C')
            
            # Generate PDF output
            pdf_output = BytesIO()
            pdf_string = pdf.output(dest='S')
            
            if isinstance(pdf_string, str):
                pdf_output.write(pdf_string.encode('utf-8'))
            else:
                pdf_output.write(pdf_string)
            
            pdf_output.seek(0)
            logger.info("PDF generated successfully")
            return pdf_output.getvalue(), None
            
        except Exception as e:
            error_msg = f"PDF generation failed: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None, error_msg

def export_csv():
    """Export CSV with comprehensive error handling"""
    try:
        logger.info("Starting CSV export")
        
        fetcher = DataFetcher()
        df, error = fetcher.fetch_data()
        if error:
            logger.error(f"Failed to fetch data for CSV: {error}")
            return None, error
        
        processed_df, error = fetcher.process_data(df)
        if error:
            logger.error(f"Failed to process data for CSV: {error}")
            return None, error
        
        csv_output = BytesIO()
        processed_df.to_csv(csv_output, index=False)
        csv_output.seek(0)
        
        logger.info(f"CSV exported successfully with {len(processed_df)} records")
        return csv_output.getvalue(), None
        
    except Exception as e:
        error_msg = f"CSV export failed: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None, error_msg

def main():
    """Main execution function with Supabase integration"""
    try:
        logger.info("=== Starting NPA Data Processing with Supabase ===")
        
        # Initialize components
        fetcher = DataFetcher()
        table_generator = SupabaseTableGenerator()
        
        # Test Supabase connection
        if not table_generator.connect_to_database():
            error_msg = "Failed to connect to Supabase database"
            logger.error(error_msg)
            return False
        
        logger.info("Successfully connected to Supabase")
        
        # Fetch data from API
        logger.info("Fetching data from NPA API...")
        df, error = fetcher.fetch_data()
        if error:
            logger.error(f"Failed to fetch data: {error}")
            return False
        
        logger.info(f"Data fetched successfully: {len(df)} rows")
        
        # Process the data
        logger.info("Processing fetched data...")
        processed_df, error = fetcher.process_data(df)
        if error:
            logger.error(f"Failed to process data: {error}")
            return False
        
        logger.info(f"Data processed successfully: {len(processed_df)} rows")
        
        # Split data into sections
        logger.info("Splitting data into sections...")
        section_dataframes = table_generator.split_dataframe_by_sections(processed_df)
        logger.info(f"Data split into {len(section_dataframes)} sections: {list(section_dataframes.keys())}")
        
        # Populate Supabase tables
        logger.info("Populating Supabase tables...")
        results = table_generator.populate_tables(section_dataframes)
        
        # Check results
        successful_tables = [table for table, success in results.items() if success]
        failed_tables = [table for table, success in results.items() if not success]
        
        if failed_tables:
            logger.error(f"Failed to process tables: {failed_tables}")
            logger.info(f"Successfully processed tables: {successful_tables}")
            return False
        
        logger.info(f"All tables processed successfully: {successful_tables}")
        
        # Get statistics
        try:
            stats = table_generator.get_table_stats()
            logger.info(f"Table statistics: {stats}")
        except Exception as e:
            logger.warning(f"Could not get table statistics: {str(e)}")
        
        logger.info("=== NPA Data Processing completed successfully ===")
        return True
        
    except Exception as e:
        error_msg = f"Unexpected error in main execution: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False

if __name__ == "__main__":
    try:
        success = main()
        if success:
            print("✅ NPA Data processing completed successfully")
            exit(0)
        else:
            print("❌ NPA Data processing failed")
            exit(1)
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        print("⚠️ Process interrupted")
        exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        print(f"💥 Fatal error: {str(e)}")
        exit(1)