import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from common import run_benchmark, setup_visuals, OUTPUT_DIR, SINGLE_BLUE

NODE_COUNTS = list(range(8, 35, 2))
REQUESTS = 5000
WORKERS = 20
NUM_TRIALS = 8 

def run():
    setup_visuals()
    print(f"--- Performance Scaling (Nodes: {NODE_COUNTS[0]} to {NODE_COUNTS[-1]}) ---")
    print(f"--- Averaging over {NUM_TRIALS} trials per configuration ---")
    
    results = []

    for n in NODE_COUNTS:
        print(f"  Processing {n} nodes...")
        
        sum_put_latency = 0.0
        sum_get_latency = 0.0
        sum_throughput = 0.0
        successful_trials = 0

        for i in range(NUM_TRIALS):
            data = run_benchmark("perf", n, REQUESTS, WORKERS)
            
            if data:
                put_lats = data.get("put_latencies", [])
                get_lats = data.get("get_latencies", [])
                
                if not put_lats or not get_lats:
                    continue

                avg_put_trial = sum(put_lats) / len(put_lats)
                avg_get_trial = sum(get_lats) / len(get_lats)
                throughput_trial = data.get("throughput", 0)

                sum_put_latency += avg_put_trial
                sum_get_latency += avg_get_trial
                sum_throughput += throughput_trial
                successful_trials += 1

        if successful_trials > 0:
            final_put_avg = sum_put_latency / successful_trials
            final_get_avg = sum_get_latency / successful_trials
            final_throughput_avg = sum_throughput / successful_trials

            results.append({
                "Nodes": n,
                "Publish Latency": final_put_avg,
                "Fetch Latency": final_get_avg,
                "Throughput": final_throughput_avg
            })
        else:
            print(f"    !! All trials failed for {n} nodes.")

    df = pd.DataFrame(results)
    if df.empty: return

    print("\n" + "="*30)
    print("FINAL AVERAGED RESULTS")
    print("="*30)
    print(f"Nodes: {df['Nodes'].tolist()}")
    print(f"Publish Latency (ms): {df['Publish Latency'].tolist()}")
    print(f"Fetch Latency (ms): {df['Fetch Latency'].tolist()}")
    print(f"Throughput (Ops/s): {df['Throughput'].tolist()}")
    print("="*30 + "\n")

    def plot_line(y_col, title, filename, marker, ylabel, ylim=None):
        plt.figure()
        sns.lineplot(
            data=df, 
            x="Nodes", 
            y=y_col, 
            marker=marker, 
            color=SINGLE_BLUE, 
            linewidth=2.5,
            ms=8
        )
        plt.title(title)
        plt.ylabel(ylabel)
        plt.xlabel("Network Size (Nodes)")
        
        if ylim:
            plt.ylim(ylim)
            
        plt.savefig(f"{OUTPUT_DIR}/{filename}")
        plt.close()
        print(f"  Saved {filename}")

    plot_line(
        "Publish Latency", 
        "Average Publish Latency vs. Network Size", 
        "perf_latency_publish.png", 
        "o", 
        "Latency (ms)",
        ylim=(0, 1)
    )

    plot_line(
        "Fetch Latency", 
        "Average Fetch Latency vs. Network Size", 
        "perf_latency_fetch.png", 
        "o", 
        "Latency (ms)",
        ylim=(0, 10)
    )

    plot_line(
        "Throughput", 
        "Throughput vs. Network Size", 
        "perf_throughput.png", 
        "o", 
        "Operations / Sec",
        ylim=(5000, 10000)
    )

if __name__ == "__main__":
    run()