import matplotlib.pyplot as plt
import seaborn as sns
from common import run_benchmark, setup_visuals, OUTPUT_DIR, SINGLE_BLUE

CHURN_NODES = 20
CHURN_REQS = 1000
REPL_NODES = 20
REPL_REQS = 100

def run():
    setup_visuals()
    print("--- Resilience Experiments ---")

    data = run_benchmark("repl", nodes=REPL_NODES, requests=REPL_REQS)
    if data:
        plt.figure()
        sns.histplot(data.get("replication_delays", []), kde=True, color=SINGLE_BLUE)
        plt.title("Replication Delay Distribution")
        plt.xlabel("Time until Backup is Consistent (ms)")
        plt.savefig(f"{OUTPUT_DIR}/resilience_repl_delay.png")
        plt.close()

    data = run_benchmark("churn", nodes=CHURN_NODES, requests=CHURN_REQS, workers=10)
    if data:
        success = data.get("churn_success", 0)
        total = data.get("churn_total", 1)
        failed = total - success
        
        plt.figure()
        colors = [SINGLE_BLUE, "#a1c9f4"] 
        plt.pie([success, failed], labels=["Success", "Failed"], colors=colors, autopct='%1.1f%%', startangle=90)
        plt.title(f"Lookup Success Rate under Active Churn")
        plt.savefig(f"{OUTPUT_DIR}/resilience_churn.png")
        plt.close()
    
    print("Resilience plots saved.")

if __name__ == "__main__":
    run()