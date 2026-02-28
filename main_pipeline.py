import time
from bronze_layer import run_bronze
from silver_layer import run_silver
from gold_layer import run_gold

def main():
    print("==========================================")
    print("🌟 STARTING OMS LAKEHOUSE PIPELINE 🌟")
    print("==========================================")
    start_all = time.time()

    # 1. Bronze
    run_bronze()
    
    # 2. Silver
    run_silver()
    
    # 3. Gold
    run_gold()

    end_all = time.time()
    print("==========================================")
    print(f"🏆 PIPELINE FINISHED IN {round(end_all - start_all, 2)} SECONDS")
    print("==========================================")

if __name__ == "__main__":
    main()