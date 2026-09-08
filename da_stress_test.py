import os
import subprocess
import threading
import time
import json
import re

class OGStorageDAShooter:
    def __init__(self):
        """
        Phase 2: High-Performance SpaceTech DA Ingestor.
        Executes parallel stream ingestion and captures full execution telemetry.
        """
        self.blockchain_rpc = os.getenv("OG_RPC_URL", "https://evmrpc-testnet.0g.ai")
        self.indexer_url = os.getenv("OG_INDEXER_URL", "https://indexer-storage-testnet-turbo.0g.ai")
        
        # Path to 0g-storage-client binary
        self.client_binary = os.getenv("OG_CLIENT_BINARY", "./0g-storage-client/0g-storage-client")
        
        # Load operator keys securely from environment variables or empty fallback list
        # Example format in environment: OPERATOR_KEYS="key1,key2,key3"
        env_keys = os.getenv("OPERATOR_KEYS")
        if env_keys:
            self.SELF_OPERATOR_KEYS = [k.strip() for k in env_keys.split(",") if k.strip()]
        else:
            # Fallback placeholder list for initial setup
            self.SELF_OPERATOR_KEYS = [
                "YOUR_PRIVATE_KEY_1",
                "YOUR_PRIVATE_KEY_2",
                "YOUR_PRIVATE_KEY_3"
            ]
        
        self.chunk_size_mb = 350
        self.template_file = "heavy_telemetry_base.raw"
        
        # Epoch telemetry counters
        self.epoch_success = 0
        self.epoch_fail = 0
        self.stats_lock = threading.Lock()
        
        # Comprehensive log database for post-mortem analytics
        self.raw_logs_database = []
        
        self.MAX_ACCEPTABLE_DROP_RATE = 30.0
        self.abort_test_flag = False
        self.semaphore = threading.Semaphore(4)

    def setup_binary_env(self):
        """Prepares binary permissions and allocates base telemetry blocks."""
        if os.path.exists(self.client_binary):
            print(f"[➔] Setting executable permissions for {self.client_binary}...")
            subprocess.run(f"chmod +x {self.client_binary}", shell=True)
        else:
            print(f"[⚠️] Warning: Binary not found at {self.client_binary}")
            
        if not os.path.exists(self.template_file):
            print(f"[➔] Pre-allocating standard {self.chunk_size_mb}MB random telemetry block...")
            cmd = f"dd if=/dev/urandom of={self.template_file} bs=1M count={self.chunk_size_mb}"
            subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            print("[✓] Telemetry template satisfied.")

    def push_heavy_sector(self, chunk_id, satellite_id, current_multiplier):
        """Pushes an individual mutated fragment to the DA layer and records performance metrics."""
        if self.abort_test_flag:
            return
            
        with self.semaphore:
            if self.abort_test_flag:
                return 
                
            file_name = f"sat_{satellite_id}_chunk_{chunk_id:03d}.raw"
            assigned_key = self.SELF_OPERATOR_KEYS[(satellite_id - 1) % len(self.SELF_OPERATOR_KEYS)]
            
            tx_record = {
                "chunk_id": chunk_id,
                "satellite_id": satellite_id,
                "epoch_multiplier": current_multiplier,
                "status": "pending",
                "tx_hash": None,
                "error_reason": None,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            
            try:
                subprocess.run(f"cp {self.template_file} {file_name}", shell=True)
                
                # Append 16 random bytes to force raw Merkle root re-computation
                with open(file_name, "ab") as f:
                    f.write(os.urandom(16))
                
                cmd = (
                    f"{self.client_binary} upload "
                    f"--url {self.blockchain_rpc} "
                    f"--key {assigned_key} "
                    f"--indexer {self.indexer_url} "
                    f"--file {file_name}"
                )
                
                result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
                full_output = result.stdout + "\n" + result.stderr
                
                # Match transaction hash pattern (0x + 64 hex characters)
                tx_match = re.search(r"0x[a-fA-F0-9]{64}", full_output)
                
                tx_record["status"] = "success"
                if tx_match:
                    tx_record["tx_hash"] = tx_match.group(0)
                
                with self.stats_lock:
                    self.epoch_success += 1
                print(f"[✓ SAT #{satellite_id}] Chunk #{chunk_id:03d} INGESTED!")
                
            except subprocess.CalledProcessError as e:
                error_log = (e.stderr or e.stdout or "Unknown terminal execution error").strip()
                tx_record["status"] = "fail"
                tx_record["error_reason"] = error_log
                
                with self.stats_lock:
                    self.epoch_fail += 1
                print(f"[❌ SAT #{satellite_id}] Chunk #{chunk_id:03d} DROPPED.")
            except Exception as general_error:
                tx_record["status"] = "fail"
                tx_record["error_reason"] = str(general_error)
                with self.stats_lock:
                    self.epoch_fail += 1
            finally:
                if os.path.exists(file_name):
                    os.remove(file_name)
                
                with self.stats_lock:
                    self.raw_logs_database.append(tx_record)

    def run_kessler_epoch(self, kessler_multiplier):
        """Executes a single test epoch with scaled concurrency."""
        if self.abort_test_flag:
            return 0.0

        self.epoch_success = 0
        self.epoch_fail = 0
        chunks_in_epoch = 10 * kessler_multiplier
        
        print(f"\n[📊 COORDINATOR] Activating Kessler Cascade Multiplier: x{kessler_multiplier}")
        
        threads = []
        for i in range(1, chunks_in_epoch + 1):
            if self.abort_test_flag:
                break
            satellite_id = (i % 10) + 1
            
            t = threading.Thread(target=self.push_heavy_sector, args=(i, satellite_id, kessler_multiplier))
            threads.append(t)
            t.start()
            
            time.sleep(max(0.05, 0.5 / kessler_multiplier))

        for t in threads:
            t.join()
            
        total_ops = self.epoch_success + self.epoch_fail
        drop_rate = (self.epoch_fail / total_ops * 100) if total_ops > 0 else 0
        
        if total_ops >= 10 and drop_rate >= self.MAX_ACCEPTABLE_DROP_RATE:
            print(f"\n[🚨 EMERGENCY STOP] Drop Rate hit {drop_rate:.2f}%!")
            self.abort_test_flag = True
            
        return drop_rate

    def execute_da_cascade(self):
        """Main execution sequence for the DA density benchmark."""
        print("=============================================================")
        print("LAUNCHING PHASE 2: AUTOMATED DEAI KESSLER DENSITY BENCHMARK")
        print("=============================================================")
        
        self.setup_binary_env()
        
        for multiplier in range(1, 11):
            if self.abort_test_flag:
                break
            self.run_kessler_epoch(multiplier)
            if not self.abort_test_flag:
                time.sleep(5.0)
                
        if os.path.exists(self.template_file):
            os.remove(self.template_file)
                
        print("\n=============================================================")
        print("🏁 BENCHMARK CONCLUDED. DUMPING RAW DATA...")
        print("=============================================================")
        
        with open("benchmark_detailed_report.json", "w", encoding="utf-8") as f:
            json.dump(self.raw_logs_database, f, indent=4, ensure_ascii=False)

        print("📊 Telemetry data successfully saved to benchmark_detailed_report.json")

if __name__ == "__main__":
    shooter = OGStorageDAShooter()
    shooter.execute_da_cascade()
