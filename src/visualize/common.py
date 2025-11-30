import subprocess
import json
import os
import seaborn as sns
import matplotlib.pyplot as plt

GO_BINARY = "go run cmd/benchmarker/main.go"
OUTPUT_DIR = "visualize/plots"

SINGLE_BLUE = "#1f77b4"

def setup_visuals():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    sns.set_theme(style="whitegrid", context="paper", font_scale=1.2)
    plt.rcParams['figure.figsize'] = (10, 6)

def run_benchmark(mode, nodes, requests, workers=10):
    """Runs the Go benchmarker and returns the JSON result."""
    cmd = f"{GO_BINARY} -mode {mode} -nodes {nodes} -requests {requests} -workers {workers}"
    print(f"  -> Running {mode} with {nodes} nodes...")
    try:
        subprocess.run(cmd, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)        
        if os.path.exists("results.json"):
            with open("results.json", "r") as f:
                return json.load(f)
    except Exception as e:
        print(f"  !! Error running {mode}: {e}")
    return None