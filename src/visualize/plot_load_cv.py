import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from common import run_benchmark, setup_visuals, OUTPUT_DIR, SINGLE_BLUE

FIXED_NODES = 16
OBJECT_COUNTS = list(range(1000, 20001, 1000))
NUM_TRIALS = 1

def run():
    setup_visuals()
    
    print(f"--- Load Balance: Varying Objects (Fixed {FIXED_NODES} Nodes) ---")
    results = []
    
    for obj_count in OBJECT_COUNTS:
        best_cv = float('inf')
        best_jain = 0.0
        best_gini = 0.0
        
        for _ in range(NUM_TRIALS):
            data = run_benchmark("load", FIXED_NODES, obj_count) 
            if data:
                cv = data.get("cv", 100.0)
                if cv < best_cv: 
                    best_cv = cv
                    best_jain = data.get("jain", 0.0)
                    best_gini = data.get("gini", 0.0)
        
        results.append({
            "Objects": obj_count,
            "CV": best_cv,
            "Jain": best_jain,
            "Gini": best_gini
        })

    df = pd.DataFrame(results)
    
    plot_kwargs = {
        "x": "Objects",
        "marker": "o",
        "color": SINGLE_BLUE,
        "linewidth": 2.5,
        "ms": 8 
    }
    
    # 1. CV Plot
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df, y="CV", **plot_kwargs)
    plt.title(f"Load Balance CV vs. Object Count ({FIXED_NODES} Nodes)")
    plt.ylabel("Coefficient of Variation (Lower is Better)")
    plt.xlabel("Number of Objects Stored")
    plt.ylim(0, 1)
    plt.savefig(f"{OUTPUT_DIR}/load_cv_objects_16n.png")
    plt.close()
    print(f"  Saved load_cv_objects_16n.png")

    # 2. Jain Plot
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df, y="Jain", **plot_kwargs)
    plt.title(f"Jain's Fairness Index vs. Object Count ({FIXED_NODES} Nodes)")
    plt.ylabel("Jain's Index (Higher is Better, Max 1.0)")
    plt.xlabel("Number of Objects Stored")
    plt.ylim(0, 1)
    plt.savefig(f"{OUTPUT_DIR}/load_jain_objects_16n.png")
    plt.close()
    print(f"  Saved load_jain_objects_16n.png")

    # 3. Gini Plot
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df, y="Gini", **plot_kwargs)
    plt.title(f"Gini Coefficient vs. Object Count ({FIXED_NODES} Nodes)")
    plt.ylabel("Gini Coefficient (Lower is Better, Min 0.0)")
    plt.xlabel("Number of Objects Stored")
    plt.ylim(0, 1)
    plt.savefig(f"{OUTPUT_DIR}/load_gini_objects_16n.png")
    plt.close()
    print(f"  Saved load_gini_objects_16n.png")

if __name__ == "__main__":
    run()