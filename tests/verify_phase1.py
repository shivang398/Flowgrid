import time
from client.flowgrid_client import FlowgridClient

def test_docker_execution():
    client = FlowgridClient("127.0.0.1", 9999)
    try:
        client.connect()
        client.authenticate("flowgrid_admin_123")
        print("Connected to Flowgrid.")
        
        # Test 1: Simple echo in Alpine
        print("\n[Test 1] Running 'echo Hello Flowgrid' in Alpine...")
        tid1 = client.submit_docker_task(
            image="alpine",
            command="echo 'Hello Flowgrid from Docker!'"
        )
        result1 = client.get_result(tid1)
        print(f"Result 1: {result1}")
        
        # Test 2: Multi-line command / Environment variables
        print("\n[Test 2] Testing Environment Variables...")
        tid2 = client.submit_docker_task(
            image="alpine",
            command="sh -c 'echo $MY_VAR'",
            env={"MY_VAR": "Flowgrid-Enterprise-Edition"}
        )
        result2 = client.get_result(tid2)
        print(f"Result 2: {result2}")
        
        # Test 3: Backward compatibility (Python task)
        print("\n[Test 3] Verifying backward compatibility (Python task)...")
        tid3 = client.submit_task("add", 100, 200)
        result3 = client.get_result(tid3)
        print(f"Result 3: {result3}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.disconnect()

if __name__ == "__main__":
    test_docker_execution()
