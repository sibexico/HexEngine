#!/bin/bash
# HexEngine Performance Profiling Script (Bash version)
# Generates CPU, memory, and mutex profiles for key components
# Usage: ./generate_profiles.sh

set -e

echo "=== HexEngine Performance Profiling ==="
echo ""

# Create profiles directory if it doesn't exist
mkdir -p profiles
echo "Profiles directory ready"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Function to run a benchmark with profiling
run_profiled_benchmark() {
    local benchmark_name=$1
    local component=$2
    local profile_type=$3
    
    local output_file="profiles/${component}_${profile_type}_${TIMESTAMP}.prof"
    echo "Profiling: $benchmark_name ($profile_type)"
    
    local profile_flag="-${profile_type}profile=$output_file"
    
    if go test ./storage/ -bench="^${benchmark_name}$" -benchtime=3s -run='^$' $profile_flag 2>&1 > /dev/null; then
        if [ -f "$output_file" ]; then
            local file_size=$(stat -f%z "$output_file" 2>/dev/null || stat -c%s "$output_file" 2>/dev/null)
            echo "  ✓ Generated: $output_file ($file_size bytes)"
            return 0
        else
            echo "  ✗ Warning: Profile not generated"
            return 1
        fi
    else
        echo "  ✗ Error running benchmark"
        return 1
    fi
}

# 1. Sharded Page Table Profiling
echo ""
echo "--- Sharded Page Table ---"
run_profiled_benchmark "BenchmarkShardedPageTableConcurrent" "sharded_table" "cpu"
run_profiled_benchmark "BenchmarkShardedPageTableConcurrent" "sharded_table" "mem"

# 2. Buffer Pool Profiling
echo ""
echo "--- Buffer Pool Manager ---"
run_profiled_benchmark "BenchmarkBufferPoolConcurrent" "buffer_pool" "cpu"
run_profiled_benchmark "BenchmarkBufferPoolConcurrent" "buffer_pool" "mem"

# 3. Transaction Manager (Object Pools)
echo ""
echo "--- Transaction Manager ---"
run_profiled_benchmark "BenchmarkTransactionCommit" "transaction" "cpu"
run_profiled_benchmark "BenchmarkTransactionCommit" "transaction" "mem"

# 4. 2Q Cache Replacement
echo ""
echo "--- 2Q Cache Replacer ---"
run_profiled_benchmark "BenchmarkTwoQReplacerHotSet" "twoq_cache" "cpu"
run_profiled_benchmark "BenchmarkTwoQReplacerPin" "twoq_cache" "mem"

# 5. LRU Cache (for comparison)
echo ""
echo "--- LRU Replacer (Baseline) ---"
run_profiled_benchmark "BenchmarkLRUReplacerHotSet" "lru_cache" "cpu"

echo ""
echo "=== Profiling Complete ==="
echo ""
echo "Generated profiles in ./profiles/"
echo ""
echo "To analyze profiles, use pprof:"
echo "  go tool pprof -http=:8080 profiles/sharded_table_cpu_${TIMESTAMP}.prof"
echo "  go tool pprof -top profiles/buffer_pool_mem_${TIMESTAMP}.prof"
echo ""
echo "For more details, see PROFILING_GUIDE.md"
