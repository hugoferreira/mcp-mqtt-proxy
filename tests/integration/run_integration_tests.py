#!/usr/bin/env python3
"""
Script to run the integration tests with uv run.

Usage:
    uv run python tests/integration/run_integration_tests.py
"""

import subprocess
import sys
import os
import time

def main():
    """Run the publisher and listener integration tests."""
    print("Running integration tests with uv run...")
    
    # Directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Test files to run
    test_files = [
        os.path.join(script_dir, "test_mqtt_publisher_integration.py"),
        os.path.join(script_dir, "test_mqtt_listener_integration.py")
    ]
    
    results = []
    
    for test_file in test_files:
        print(f"\n\n{'='*80}")
        print(f"Running {os.path.basename(test_file)}...")
        print(f"{'='*80}\n")
        
        # Run the test with uv run
        cmd = ["uv", "run", "pytest", "-v", test_file]
        print(f"Command: {' '.join(cmd)}")
        print()
        
        try:
            # Run the test and capture output
            process = subprocess.run(
                cmd,
                check=False,  # Don't raise exception on non-zero exit code
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True
            )
            
            # Print the output
            print(process.stdout)
            
            # Store the result
            results.append((os.path.basename(test_file), process.returncode == 0))
            
            # If test failed, wait a moment before continuing to allow resources to clean up
            if process.returncode != 0:
                print(f"Test {os.path.basename(test_file)} failed with exit code {process.returncode}")
                time.sleep(3)
        except Exception as e:
            print(f"Error running test {test_file}: {e}")
            results.append((os.path.basename(test_file), False))
    
    # Print summary
    print("\n\n")
    print("="*40)
    print(" Integration Tests Summary ")
    print("="*40)
    all_passed = True
    for test, passed in results:
        status = "PASSED" if passed else "FAILED"
        print(f"{test}: {status}")
        if not passed:
            all_passed = False
    
    # Exit with appropriate status code
    if all_passed:
        print("\nAll integration tests passed!")
        sys.exit(0)
    else:
        print("\nSome integration tests failed.")
        sys.exit(1)

if __name__ == "__main__":
    main() 