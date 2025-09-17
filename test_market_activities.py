#!/usr/bin/env python3
"""
Test script to verify market activities data transformation works correctly
"""
import asyncio
import sys
import os

# Add the src directory to path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from services.fetch_activities import convert_batch_response_to_legacy_format

def test_data_transformation():
    """Test that the batch API response is converted to the expected format"""
    
    # Sample response from the batch API (based on your example)
    batch_api_response = {
        "5m": {
            "numTxs": 13,
            "volumeUSD": 3551.4783108961537,
            "numUsers": 11,
            "numBuys": 4,
            "numSells": 9,
            "buyVolumeUSD": 3136.7031358298636,
            "sellVolumeUSD": 414.7751750662898,
            "numBuyers": 3,
            "numSellers": 8,
            "priceChangePercent": 17.734245533176313
        },
        "1h": {
            "numTxs": 112,
            "volumeUSD": 14727.047343828961,
            "numUsers": 53,
            "numBuys": 48,
            "numSells": 64,
            "buyVolumeUSD": 10926.596016330268,
            "sellVolumeUSD": 3800.4513274986925,
            "numBuyers": 30,
            "numSellers": 38,
            "priceChangePercent": 52.58948427871279
        },
        "6h": {
            "numTxs": 780,
            "volumeUSD": 107664.8809400746,
            "numUsers": 236,
            "numBuys": 368,
            "numSells": 412,
            "buyVolumeUSD": 56125.86216909694,
            "sellVolumeUSD": 51539.01877097766,
            "numBuyers": 137,
            "numSellers": 181,
            "priceChangePercent": 20.049613587039808
        },
        "24h": {
            "numTxs": 22315,
            "volumeUSD": 3736784.7382915863,
            "numUsers": 4110,
            "numBuys": 11761,
            "numSells": 10554,
            "buyVolumeUSD": 1904286.8421200695,
            "sellVolumeUSD": 1832497.896171517,
            "numBuyers": 3933,
            "numSellers": 3483,
            "priceChangePercent": 3482.901834702171
        }
    }
    
    # Convert using our function
    converted = convert_batch_response_to_legacy_format(batch_api_response)
    
    # Test that all required fields are present
    required_time_periods = ["5m", "1h", "6h", "24h"]
    required_fields = ["numTxs", "volumeUSD", "numUsers", "priceChangePercent"]
    
    print("🧪 Testing data transformation...")
    
    for time_period in required_time_periods:
        assert time_period in converted, f"Missing time period: {time_period}"
        
        for field in required_fields:
            assert field in converted[time_period], f"Missing field {field} in {time_period}"
            
        # Test specific values to ensure data integrity
        if time_period == "5m":
            assert converted[time_period]["numTxs"] == 13
            assert converted[time_period]["volumeUSD"] == 3551.4783108961537
            assert converted[time_period]["numUsers"] == 11
            assert converted[time_period]["priceChangePercent"] == 17.734245533176313
    
    print("✅ Data transformation test passed!")
    
    # Test empty data case  
    empty_converted = convert_batch_response_to_legacy_format({})
    for time_period in required_time_periods:
        assert time_period in empty_converted
        assert empty_converted[time_period]["numTxs"] == 0
        assert empty_converted[time_period]["volumeUSD"] == 0.0
        assert empty_converted[time_period]["numUsers"] == 0
        
    print("✅ Empty data handling test passed!")
    
    return True

def test_database_field_mapping():
    """Test that the converted data has the exact fields the database sync expects"""
    
    # Sample converted data
    batch_data = {
        "5m": {
            "numTxs": 13,
            "volumeUSD": 3551.48,
            "numUsers": 11,
            "priceChangePercent": 17.73
        }
    }
    
    converted = convert_batch_response_to_legacy_format(batch_data)
    
    # Simulate what database sync service does
    activities_5m = converted.get("5m", {})
    
    # These are the exact field accesses from database_sync_service.py
    txns_5m = activities_5m.get("numTxs", 0) or 0
    volume_5m = activities_5m.get("volumeUSD", 0) or 0  
    traders_5m = activities_5m.get("numUsers", 0) or 0
    price_change_5m = activities_5m.get("priceChangePercent", 0) or 0
    
    assert txns_5m == 13
    assert volume_5m == 3551.48
    assert traders_5m == 11
    assert price_change_5m == 17.73
    
    print("✅ Database field mapping test passed!")
    
    return True

if __name__ == "__main__":
    print("🚀 Running market activities integration tests...\n")
    
    try:
        test_data_transformation()
        test_database_field_mapping()
        
        print("\n🎉 All tests passed! The integration should work correctly.")
        print("\nKey validations:")
        print("✅ Batch API response format is correctly converted")
        print("✅ Database sync service field mappings match exactly")
        print("✅ Empty data is handled gracefully") 
        print("✅ Data integrity is preserved during conversion")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        sys.exit(1)
