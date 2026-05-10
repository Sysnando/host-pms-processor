"""Quick script to check ESB configuration for local testing.

This helps verify that environment variables are correctly set before running tests.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import settings


def check_esb_config():
    """Check ESB configuration and display what's available."""
    print("\n" + "=" * 80)
    print("ESB CONFIGURATION CHECK")
    print("=" * 80)

    # Check USE_REAL_ESB flag
    use_real_esb = settings.use_real_esb
    print(f"\n1. USE_REAL_ESB: {use_real_esb}")
    if not use_real_esb:
        print("   ⚠️  Real ESB testing is DISABLED")
        print("   Set USE_REAL_ESB=true to enable")
    else:
        print("   ✅ Real ESB testing is ENABLED")

    # Check ESB Base URL
    print(f"\n2. ESB Base URL: {settings.esb.base_url}")

    # Check OAuth Token URL
    oauth_token_url = settings.esb.oauth_token_url
    full_token_url = f"{settings.esb.base_url.rstrip('/')}{oauth_token_url}"
    print(f"\n3. OAuth Token URL: {oauth_token_url}")
    print(f"   Full URL: {full_token_url}")

    # Check authentication credentials
    print("\n4. Authentication Credentials:")

    # Check top-level esb_basic_auth
    esb_basic_auth_top = settings.esb_basic_auth or ""
    print(f"   - settings.esb_basic_auth (ESB_BASIC_AUTH): ", end="")
    if esb_basic_auth_top.strip():
        print(f"✅ Set ({len(esb_basic_auth_top)} chars)")
    else:
        print("❌ Not set")

    # Check nested esb.basic_auth
    esb_basic_auth_nested = settings.esb.basic_auth or ""
    print(f"   - settings.esb.basic_auth: ", end="")
    if esb_basic_auth_nested.strip():
        print(f"✅ Set ({len(esb_basic_auth_nested)} chars)")
    else:
        print("❌ Not set")

    # Check OAuth client credentials
    oauth_client_id = settings.esb.oauth_client_id or ""
    oauth_client_secret = settings.esb.oauth_client_secret or ""

    print(f"   - ESB_OAUTH_CLIENT_ID: ", end="")
    if oauth_client_id.strip():
        print(f"✅ Set ({len(oauth_client_id)} chars)")
    else:
        print("❌ Not set")

    print(f"   - ESB_OAUTH_CLIENT_SECRET: ", end="")
    if oauth_client_secret.strip():
        print(f"✅ Set ({len(oauth_client_secret)} chars)")
    else:
        print("❌ Not set")

    # Determine which auth method will be used
    print("\n5. Authentication Method:")
    basic_auth_value = (
        settings.esb_basic_auth or settings.esb.basic_auth or ""
    ).strip()

    if basic_auth_value:
        print("   ✅ Will use ESB_BASIC_AUTH (pre-encoded)")
        # Validate format
        if basic_auth_value.startswith("Basic "):
            print("      ⚠️  Contains 'Basic ' prefix - will be stripped")
            basic_auth_value = basic_auth_value[6:].strip()
        print(f"      Base64 length: {len(basic_auth_value)} chars")
    elif oauth_client_id.strip() and oauth_client_secret.strip():
        print("   ✅ Will auto-encode from ESB_OAUTH_CLIENT_ID:ESB_OAUTH_CLIENT_SECRET")
        print(f"      Client ID: {oauth_client_id[:10]}...")
        print(f"      Client Secret: {'*' * len(oauth_client_secret)}")
    else:
        print("   ❌ No authentication credentials configured!")
        print("      Set ESB_BASIC_AUTH or (ESB_OAUTH_CLIENT_ID + ESB_OAUTH_CLIENT_SECRET)")

    # Check Redis configuration
    print("\n6. Redis Configuration:")
    print(f"   - Host: {settings.redis.host}")
    print(f"   - Port: {settings.redis.port}")
    print(f"   - DB: {settings.redis.db}")

    # Check hotel code
    print("\n7. Hotel Configuration:")
    hotel_code_s3 = (settings.hotel_code_s3 or settings.hotel.hotel_code_s3 or "").strip()
    print(f"   - HOTEL_CODE_S3: ", end="")
    if hotel_code_s3:
        print(f"✅ {hotel_code_s3}")
    else:
        print("❌ Not set (will process all hotels from ESB)")

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    ready_for_testing = (
        use_real_esb
        and (basic_auth_value or (oauth_client_id.strip() and oauth_client_secret.strip()))
    )

    if ready_for_testing:
        print("✅ Configuration looks good for real ESB testing!")
        print("\nRun the test with:")
        print("  python -m tests.test_local_run")
    else:
        print("❌ Configuration incomplete for real ESB testing")
        print("\nTo enable real ESB testing, set:")
        print("  USE_REAL_ESB=true")
        if not basic_auth_value and not (oauth_client_id.strip() and oauth_client_secret.strip()):
            print("  ESB_BASIC_AUTH=<your-base64-encoded-credentials>")
            print("    OR")
            print("  ESB_OAUTH_CLIENT_ID=<your-client-id>")
            print("  ESB_OAUTH_CLIENT_SECRET=<your-client-secret>")

    print("=" * 80 + "\n")

    return 0 if ready_for_testing else 1


if __name__ == "__main__":
    sys.exit(check_esb_config())
