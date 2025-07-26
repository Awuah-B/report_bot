#!/usr/bin/env python3
"""
Test script to verify Supabase integration
Run this after setting up the Supabase database and environment variables
"""

import os
import sys
import traceback
from datetime import datetime
import pandas as pd

# Add current directory to path so we can import our modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_environment_variables():
    """Test if required environment variables are set"""
    print("🔍 Testing environment variables...")
    
    required_vars = ['SUPABASE_URL', 'SUPABASE_ANON_KEY']
    missing_vars = []
    
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            missing_vars.append(var)
        else:
            print(f"  ✅ {var}: {'*' * (len(value) - 10)}{value[-10:]}")
    
    if missing_vars:
        print(f"  ❌ Missing environment variables: {missing_vars}")
        return False
    
    print("  ✅ All environment variables are set")
    return True

def test_supabase_connection():
    """Test Supabase connection"""
    print("\n🔍 Testing Supabase connection...")
    
    try:
        from supabase_handler import SupabaseHandler
        
        handler = SupabaseHandler()
        
        if handler.test_connection():
            print("  ✅ Supabase connection successful")
            return True
        else:
            print("  ❌ Supabase connection failed")
            return False
            
    except ImportError as e:
        print(f"  ❌ Failed to import SupabaseHandler: {e}")
        return False
    except Exception as e:
        print(f"  ❌ Supabase connection error: {e}")
        return False

def test_table_operations():
    """Test basic table operations"""
    print("\n🔍 Testing table operations...")
    
    try:
        from supabase_handler import SupabaseTableGenerator
        
        table_gen = SupabaseTableGenerator()
        
        # Test connection
        if not table_gen.connect_to_database():
            print("  ❌ Database connection failed")
            return False
        
        print("  ✅ Database connection successful")
        
        # Test table statistics
        try:
            stats = table_gen.get_table_stats()
            print(f"  ✅ Retrieved table statistics: {len(stats)} tables")
            for table, count in stats.items():
                print(f"    - {table}: {count} records")
        except Exception as e:
            print(f"  ⚠️  Could not get table stats: {e}")
        
        return True
        
    except ImportError as e:
        print(f"  ❌ Failed to import SupabaseTableGenerator: {e}")
        return False
    except Exception as e:
        print(f"  ❌ Table operations error: {e}")
        traceback.print_exc()
        return False

def test_data_fetching():
    """Test data fetching from API"""
    print("\n🔍 Testing data fetching...")
    
    try:
        from main import DataFetcher
        
        fetcher = DataFetcher()
        
        print("  📡 Fetching data from NPA API...")
        df, error = fetcher.fetch_data()
        
        if error:
            print(f"  ❌ Failed to fetch data: {error}")
            return False
        
        if df is None or df.empty:
            print("  ⚠️  No data received from API")
            return False
        
        print(f"  ✅ Successfully fetched {len(df)} rows from API")
        
        # Test data processing
        print("  🔄 Processing data...")
        processed_df, error = fetcher.process_data(df)
        
        if error:
            print(f"  ❌ Failed to process data: {error}")
            return False
        
        if processed_df is None or processed_df.empty:
            print("  ⚠️  No data after processing")
            return False
        
        print(f"  ✅ Successfully processed data: {len(processed_df)} rows")
        print(f"    Columns: {list(processed_df.columns)}")
        
        return True
        
    except ImportError as e:
        print(f"  ❌ Failed to import DataFetcher: {e}")
        return False
    except Exception as e:
        print(f"  ❌ Data fetching error: {e}")
        traceback.print_exc()
        return False

def test_full_pipeline():
    """Test the complete data pipeline"""
    print("\n🔍 Testing full data pipeline...")
    
    try:
        from main import main as main_function
        
        print("  🚀 Running main data processing function...")
        success = main_function()
        
        if success:
            print("  ✅ Full pipeline completed successfully")
            return True
        else:
            print("  ❌ Full pipeline failed")
            return False
            
    except ImportError as e:
        print(f"  ❌ Failed to import main function: {e}")
        return False
    except Exception as e:
        print(f"  ❌ Full pipeline error: {e}")
        traceback.print_exc()
        return False

def test_new_records_detection():
    """Test new records detection"""
    print("\n🔍 Testing new records detection...")
    
    try:
        from supabase_handler import SupabaseTableGenerator
        
        table_gen = SupabaseTableGenerator()
        
        # Get new depot manager records
        new_records = table_gen.get_new_depot_manager_records()
        
        print(f"  ✅ Retrieved {len(new_records)} new depot manager records")
        
        if not new_records.empty:
            print("    Recent records:")
            for idx, (_, row) in enumerate(new_records.head(3).iterrows()):
                print(f"      {idx + 1}. BRV: {row.get('brv_number', 'N/A')}, Date: {row.get('order_date', 'N/A')}")
        
        return True
        
    except Exception as e:
        print(f"  ❌ New records detection error: {e}")
        traceback.print_exc()
        return False

def test_brv_search():
    """Test BRV number search functionality"""
    print("\n🔍 Testing BRV search functionality...")
    
    try:
        from supabase_handler import SupabaseTableGenerator
        
        table_gen = SupabaseTableGenerator()
        
        # Get a sample BRV number from new records
        new_records = table_gen.get_new_depot_manager_records()
        
        if new_records.empty:
            print("  ⚠️  No records available to test search")
            return True
        
        # Get first BRV number
        sample_brv = new_records.iloc[0]['brv_number']
        if pd.isna(sample_brv) or str(sample_brv).strip() == '':
            print("  ⚠️  No valid BRV number found to test")
            return True
        
        print(f"  🔍 Searching for BRV: {sample_brv}")
        
        search_results = table_gen.search_brv_number(str(sample_brv))
        
        print(f"  ✅ Found {len(search_results)} records for BRV {sample_brv}")
        
        for result in search_results:
            table_name = result['table'].replace('_', ' ').title()
            print(f"    - Found in: {table_name}")
        
        return True
        
    except Exception as e:
        print(f"  ❌ BRV search error: {e}")
        traceback.print_exc()
        return False

def run_all_tests():
    """Run all tests and provide summary"""
    print("🧪 Supabase Integration Test Suite")
    print("=" * 50)
    
    tests = [
        ("Environment Variables", test_environment_variables),
        ("Supabase Connection", test_supabase_connection),
        ("Table Operations", test_table_operations),
        ("Data Fetching", test_data_fetching),
        ("New Records Detection", test_new_records_detection),
        ("BRV Search", test_brv_search),
        ("Full Pipeline", test_full_pipeline),
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"\n❌ {test_name} test crashed: {e}")
            traceback.print_exc()
            results[test_name] = False
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 Test Results Summary")
    print("=" * 50)
    
    passed = 0
    total = len(results)
    
    for test_name, success in results.items():
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} {test_name}")
        if success:
            passed += 1
    
    print(f"\n🏆 Tests Passed: {passed}/{total}")
    
    if passed == total:
        print("🎉 All tests passed! Supabase integration is working correctly.")
        return True
    else:
        print("⚠️  Some tests failed. Please check the errors above.")
        return False

def main():
    """Main test function"""
    try:
        success = run_all_tests()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n⚠️  Tests interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Fatal error running tests: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()