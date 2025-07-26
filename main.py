#!/usr/bin/env python3
"""
Enhanced NPA Data Processing Script with Supabase Integration
Features:
- Async/await pattern for better performance
- Improved error handling
- Better logging
- Integration with new config system
"""

import asyncio
import pandas as pd
import datetime
from io import BytesIO
import hashlib
from typing import Dict, Tuple, Optional
import traceback
import aiohttp

from config import CONFIG
from utils import setup_logging
from supabase_handler import SupabaseTableGenerator

try:
    from fpdf import FPDF
except ImportError:
    FPDF = None

logger = setup_logging('npa_data.log')

class DataFetcher:
    """Async data fetcher with improved error handling"""
    
    def __init__(self):
        self.today = datetime.datetime.now()
        self.yesterday = self.today - datetime.timedelta(days=1)
        self.date_format = "%d-%m-%Y"
        self.timeout = aiohttp.ClientTimeout(total=60)
    
    async def fetch_data(self) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """Fetch data from API with async requests"""
        try:
            params = {
                'lngCompanyId': CONFIG.api.company_id,
                'szITSfromPersol': CONFIG.api.its_from_persol,
                'strGroupBy': CONFIG.api.group_by,
                'strGroupBy1': CONFIG.api.group_by1,
                'strQuery1': CONFIG.api.query1,
                'strQuery2': self.yesterday.strftime(self.date_format),
                'strQuery3': self.today.strftime(self.date_format),
                'strQuery4': CONFIG.api.query4,
                'strPicHeight': CONFIG.api.pic_height,
                'strPicWeight': CONFIG.api.pic_weight,
                'intPeriodID': CONFIG.api.period_id,
                'iUserId': CONFIG.api.user_id,
                'iAppId': CONFIG.api.app_id
            }
            
            logger.info(f"Fetching data for {params['strQuery2']} to {params['strQuery3']}")
            
            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
            }
            
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.get(
                    "https://iml.npa-enterprise.com/NPAAPILIVE/Home/ExportDailyOrderReport",
                    headers=headers,
                    params=params
                ) as response:
                    response.raise_for_status()
                    content = await response.read()
                    
                    df = pd.read_excel(BytesIO(content))
                    if df.empty:
                        logger.warning("Received empty data from API")
                        return None, "Received empty data from API"
                    
                    logger.info(f"Fetched {len(df)} rows from API")
                    return df, None
                    
        except asyncio.TimeoutError:
            error_msg = "API request timed out"
            logger.error(error_msg)
            return None, error_msg
        except aiohttp.ClientError as e:
            error_msg = f"API request failed: {str(e)}"
            logger.error(error_msg)
            return None, error_msg
        except Exception as e:
            error_msg = f"Unexpected error fetching data: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None, error_msg
        
    async def process_data(self, df: pd.DataFrame) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """Process and clean DataFrame with better validation"""
        try:
            if df is None or df.empty:
                return None, "No data to process"
            
            logger.info(f"Processing {len(df)} rows, {len(df.columns)} columns")
            
            # Clean and filter data
            df = self._clean_data(df)
            df = self._filter_bost_kumasi(df)
            df = self._transform_columns(df)
            
            logger.info(f"Final processed DataFrame: {len(df)} rows")
            return df, None
            
        except Exception as e:
            error_msg = f"Error processing data: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None, error_msg
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Initial data cleaning steps"""
        # Remove header rows and empty rows/columns
        df = df.iloc[7:].copy()
        df = df.astype(str).replace('nan', '', regex=True)
        df = df[~df.apply(lambda row: all(val.strip() == '' for val in row), axis=1)]
        df = df.loc[:, ~df.apply(lambda col: all(val.strip() == '' for val in col), axis=0)]
        df = df[~df.apply(lambda row: any('Total #' in str(val) for val in row), axis=1)]
        return df
    
    def _filter_bost_kumasi(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter for BOST-KUMASI records"""
        last_col = df.columns[-1]
        mask = df.apply(lambda row: any(
            "BOST-KUMASI" in str(val) or "BOST - KUMASI" in str(val)
            for val in row
        ), axis=1) | df[last_col].str.strip().eq('')
        
        if not mask.any():
            raise ValueError("No BOST-KUMASI records found")
        
        return df[mask]
    
    def _transform_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform and standardize columns"""
        # Drop unnecessary columns
        cols_to_drop = ['Unnamed: 6', 'Unnamed: 19', 'Unnamed: 20']
        df = df.drop(columns=[col for col in cols_to_drop if col in df.columns], errors='ignore')
        
        # Handle section headers
        first_col = df.columns[0]
        mask = df.apply(lambda row: (row != '').sum() == 1 and row[first_col] != '', axis=1)
        if mask.any():
            special_rows = df[mask].copy().drop_duplicates(subset=[first_col], keep='first')
            df = pd.concat([df[~mask], special_rows]).sort_index()
        
        # Standardize column names and types
        column_map = {
            'Unnamed: 0': 'ORDER_DATE',
            'Unnamed: 2': 'ORDER_NUMBER',
            'Unnamed: 5': 'PRODUCTS',
            'Unnamed: 9': 'VOLUME',
            'Unnamed: 10': 'EX_REF_PRICE',
            'Unnamed: 12': 'BRV_NUMBER',
            'Unnamed: 15': 'BDC'
        }
        
        df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})
        
        # Convert data types
        df['ORDER_DATE'] = pd.to_datetime(df['ORDER_DATE'], format='%d-%m-%Y', errors='coerce')
        df['VOLUME'] = pd.to_numeric(df['VOLUME'], errors='coerce').astype('Int64')
        df['EX_REF_PRICE'] = pd.to_numeric(df['EX_REF_PRICE'], errors='coerce').astype(float)
        
        for col in ['ORDER_NUMBER', 'PRODUCTS', 'BRV_NUMBER', 'BDC']:
            if col in df.columns:
                df[col] = df[col].astype(str)
        
        return df.dropna(how='all')


class PDFGenerator:
    """Enhanced PDF generator with better formatting"""
    
    def __init__(self):
        self.font = "Arial"
        self.max_col_width = 40  # mm
        self.max_cell_chars = 20
        
    async def generate(self, df: pd.DataFrame, title: str, footnote: str) -> Tuple[Optional[bytes], Optional[str]]:
        """Generate PDF asynchronously"""
        if not FPDF:
            return None, "FPDF library not installed"
            
        try:
            pdf = FPDF(orientation='L', unit='mm', format='A4')
            pdf.set_auto_page_break(auto=True, margin=15)
            pdf.add_page()
            
            self._add_title(pdf, title)
            self._add_headers(pdf, df.columns)
            self._add_data_rows(pdf, df)
            
            if footnote:
                self._add_footnote(pdf, footnote)
            
            return pdf.output(dest='S').encode('latin1') if isinstance(pdf.output(dest='S'), str) else pdf.output(dest='S'), None
            
        except Exception as e:
            error_msg = f"PDF generation failed: {str(e)}"
            logger.error(error_msg)
            return None, error_msg
    
    def _add_title(self, pdf: FPDF, title: str) -> None:
        """Add title to PDF"""
        pdf.set_font(self.font, 'B', 16)
        pdf.cell(0, 10, title, ln=True, align='C')
        pdf.ln(10)
    
    def _add_headers(self, pdf: FPDF, columns: pd.Index) -> None:
        """Add column headers"""
        col_widths = self._calculate_column_widths(pdf, columns)
        pdf.set_font(self.font, 'B', 8)
        
        for col, width in zip(columns, col_widths):
            col_text = str(col)[:15] + "..." if len(str(col)) > 15 else str(col)
            pdf.cell(width, 8, col_text, border=1, align='C')
        pdf.ln()
    
    def _add_data_rows(self, pdf: FPDF, df: pd.DataFrame) -> None:
        """Add data rows to PDF"""
        col_widths = self._calculate_column_widths(pdf, df.columns)
        pdf.set_font(self.font, size=7)
        
        for _, row in df.iterrows():
            if pdf.get_y() + 8 > pdf.h - 15:
                pdf.add_page()
                self._add_headers(pdf, df.columns)
                
            for col, width in zip(df.columns, col_widths):
                cell_value = row[col]
                cell_text = "" if pd.isna(cell_value) else str(cell_value)[:self.max_cell_chars]
                pdf.cell(width, 8, cell_text, border=1)
            pdf.ln()
    
    def _add_footnote(self, pdf: FPDF, footnote: str) -> None:
        """Add footnote to PDF"""
        pdf.set_y(-20)
        pdf.set_font(self.font, 'I', 7)
        pdf.multi_cell(0, 6, footnote, align='C')
    
    def _calculate_column_widths(self, pdf: FPDF, columns: pd.Index) -> List[float]:
        """Calculate optimal column widths"""
        page_width = pdf.w - 20
        num_cols = len(columns)
        base_width = min(page_width / num_cols, self.max_col_width)
        return [base_width] * num_cols


async def export_csv() -> Tuple[Optional[bytes], Optional[str]]:
    """Export data to CSV with async processing"""
    try:
        logger.info("Starting CSV export")
        
        fetcher = DataFetcher()
        df, error = await fetcher.fetch_data()
        if error:
            return None, error
        
        processed_df, error = await fetcher.process_data(df)
        if error:
            return None, error
        
        csv_data = processed_df.to_csv(index=False).encode('utf-8')
        logger.info(f"Exported {len(processed_df)} records to CSV")
        return csv_data, None
        
    except Exception as e:
        error_msg = f"CSV export failed: {str(e)}"
        logger.error(error_msg)
        return None, error_msg


async def main() -> bool:
    """Async main execution function"""
    try:
        logger.info("=== Starting NPA Data Processing ===")
        
        # Initialize components
        fetcher = DataFetcher()
        table_generator = SupabaseTableGenerator()
        
        # Test connection
        if not await table_generator.connect_to_database():
            logger.error("Failed to connect to Supabase")
            return False
        
        # Fetch and process data
        logger.info("Fetching data from API...")
        df, error = await fetcher.fetch_data()
        if error:
            logger.error(f"Data fetch failed: {error}")
            return False
        
        logger.info("Processing data...")
        processed_df, error = await fetcher.process_data(df)
        if error:
            logger.error(f"Data processing failed: {error}")
            return False
        
        # Split and populate tables
        logger.info("Splitting data into sections...")
        sections = table_generator.split_dataframe_by_sections(processed_df)
        
        logger.info("Populating Supabase tables...")
        results = await table_generator.populate_tables(sections)
        
        # Check results
        failed_tables = [t for t, success in results.items() if not success]
        if failed_tables:
            logger.error(f"Failed tables: {failed_tables}")
            return False
        
        logger.info("=== Processing completed successfully ===")
        return True
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False


if __name__ == "__main__":
    try:
        success = asyncio.run(main())
        if success:
            print("✅ Processing completed successfully")
            exit(0)
        print("❌ Processing failed")
        exit(1)
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        print("⚠️ Process interrupted")
        exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        print(f"💥 Fatal error: {str(e)}")
        exit(1)