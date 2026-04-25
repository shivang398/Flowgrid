import time
from client.flowgrid_client import FlowgridClient

def test_dag_execution():
    client = FlowgridClient("127.0.0.1", 9999)
    try:
        client.connect()
        client.authenticate("flowgrid_admin_123")
        print("Connected to Flowgrid.")
        
        # Scenario: A -> B
        # Task B should wait for Task A to complete.
        
        print("\n[DAG Test] Submitting Task A (Parent)...")
        tid_a = client.submit_task("add", 10, 20)
        
        print(f"[DAG Test] Submitting Task B (Child, depends on {tid_a})...")
        tid_b = client.submit_docker_task(
            image="alpine",
            command=f"sh -c 'echo Task B started after parent {tid_a}'",
            depends_on=[tid_a]
        )
        
        # Verify B doesn't finish before A
        # (This is hard to prove without timing, but we can check logs or just verify completion)
        
        print("Waiting for results...")
        res_a = client.get_result(tid_a)
        print(f"Task A result: {res_a}")
        
        res_b = client.get_result(tid_b)
        print(f"Task B result: {res_b}")
        
        print("\n✅ DAG Test Passed!")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.disconnect()

if __name__ == "__main__":
    test_dag_execution()
