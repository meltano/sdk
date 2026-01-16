from singer_sdk.helpers.capabilities import TARGET_SCHEMA_CONFIG

def test_debug():
    prop = TARGET_SCHEMA_CONFIG.get("properties", {}).get("default_target_schema", {})
    print("--- FULL PROPERTY DATA ---")
    print(prop)
    
    # Sometimes it's directly in the dict, not under 'extras'
    alias_check = prop.get("aliases", [])
    print(f"Direct aliases: {alias_check}")
    
    if "schema" in str(prop):
        print("✅ SUCCESS: 'schema' was found somewhere in the property definition!")
    else:
        print("❌ FAIL: 'schema' is nowhere to be found in the live object.")

if __name__ == "__main__":
    test_debug()
