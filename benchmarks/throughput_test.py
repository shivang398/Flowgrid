import time
import argparse
from client.flowgrid_client import FlowgridClient
from common import get_logger

logger = get_logger("benchmarking")

def run_benchmark(host: str, port: int, num_tasks: int):
    client = FlowgridClient(host, port)
    client.connect()
    
    logger.info(f"Starting benchmark: {num_tasks} tasks...")
    
    start_time = time.time()
    task_ids = []
    
    # Submission phase
    for i in range(num_tasks):
        tid = client.submit_task("add", i, i)
        task_ids.append(tid)
    
    logger.info(f"All {num_tasks} tasks submitted. Waiting for results...")
    
    # Retrieval phase
    completed = 0
    for tid in task_ids:
        client.get_result(tid, wait=True)
        completed += 1
        if completed % 10 == 0:
            logger.info(f"Progress: {completed}/{num_tasks}")
            
    end_time = time.time()
    total_time = end_time - start_time
    throughput = num_tasks / total_time
    
    print("-" * 30)
    print("BENCHMARK RESULTS")
    print("-" * 30)
    print(f"Total Tasks:  {num_tasks}")
    print(f"Total Time:   {total_time:.2f} seconds")
    print(f"Throughput:   {throughput:.2f} tasks/second")
    print(f"Avg Latency:  {(total_time/num_tasks)*1000:.2f} ms/task")
    print("-" * 30)

    client.disconnect()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=9999)
    parser.add_argument("--tasks", type=int, default=100)
    args = parser.parse_args()
    
    run_benchmark(args.host, args.port, args.tasks)
