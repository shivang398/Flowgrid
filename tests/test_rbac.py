from client.flowgrid_client import FlowgridClient

def test_rbac():
    client = FlowgridClient("127.0.0.1", 9999)
    try:
        client.connect()
        
        # 1. Try to submit without auth (Should fail)
        print("\n[RBAC Test] Attempting submission without authentication...")
        try:
            client.submit_task("add", 1, 1)
        except Exception as e:
            print(f"Expected Error: {e}")
            
        # 2. Authenticate with valid Admin Key
        print("\n[RBAC Test] Authenticating with Admin Key...")
        client.authenticate("flowgrid_admin_123")
        
        # 3. Submit task (Should succeed)
        print("[RBAC Test] Attempting submission with valid authentication...")
        tid = client.submit_task("add", 10, 20)
        print(f"Success! Task ID: {tid}")
        
        # 4. Get results
        result = client.get_result(tid)
        print(f"Result: {result}")

    except Exception as e:
        print(f"Test Failed: {e}")
    finally:
        client.disconnect()

if __name__ == "__main__":
    test_rbac()
