import matplotlib.pyplot as plt
from common import run_benchmark, setup_visuals, OUTPUT_DIR, SINGLE_BLUE

# --- Configuration ---
NODES = 8
REQUESTS = 25000
NUM_TRIALS = 1

def run():
    setup_visuals()
    print(f"--- Load Distribution Histogram ({NODES} Nodes) ---")

    best_data = None
    min_cv = float('inf')

    for i in range(NUM_TRIALS):
        data = run_benchmark("load", nodes=NODES, requests=REQUESTS)
        if data:
            cv = data.get("cv", 100.0)
            if cv < min_cv:
                min_cv = cv
                best_data = data

    if best_data and "node_storage" in best_data:
        counts = list(best_data["node_storage"].values())

        plt.figure()
        plt.bar(range(len(counts)), counts, color=SINGLE_BLUE, alpha=0.8)
        plt.title(f"Object Distribution - {NODES} Nodes")
        plt.xlabel("Node Index")
        plt.ylabel("Objects Stored")
        plt.savefig(f"{OUTPUT_DIR}/load_histogram.png")
        plt.close()
        print("Histogram saved.")


if __name__ == "__main__":
    run()