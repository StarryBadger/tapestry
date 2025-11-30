import math
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from common import run_benchmark, setup_visuals, OUTPUT_DIR, SINGLE_BLUE

NODE_COUNTS = [i for i in range(4, 65)]
REQUESTS = 500

def run():
    setup_visuals()
    print(f"--- Hop Count Scaling (Nodes: {NODE_COUNTS}) ---")
    results = []

    for n in NODE_COUNTS:
        data = run_benchmark("hops", n, REQUESTS)
        if data:
            results.append({
                "Nodes": n,
                "Avg Hops": data.get("avg_hops", 0)
            })

    df = pd.DataFrame(results)
    
    if df.empty: return

    plt.figure()
    sns.lineplot(data=df, x="Nodes", y="Avg Hops", marker="o", color=SINGLE_BLUE, label="Measured")
    
    theoretical = [math.log(x, 16) if x > 0 else 0 for x in df["Nodes"]]
    plt.plot(df["Nodes"], theoretical, '--', color='black', alpha=0.6, label="Log16(N)")
    
    plt.title("Routing Efficiency: Hops vs. Network Size")
    plt.ylabel("Average Hops")
    plt.legend()
    plt.savefig(f"{OUTPUT_DIR}/hops_scaling.png")
    plt.close()
    print("Hops plot saved.")

if __name__ == "__main__":
    run()