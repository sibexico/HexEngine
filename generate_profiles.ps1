# HexEngine Performance Profiling Script
# Generates CPU, memory, and mutex profiles for key components
# Usage: .\generate_profiles.ps1

Write-Host "=== HexEngine Performance Profiling ===" -ForegroundColor Cyan
Write-Host ""

# Create profiles directory if it doesn't exist
if (!(Test-Path "profiles")) {
    New-Item -ItemType Directory -Path "profiles" | Out-Null
    Write-Host "Created profiles directory" -ForegroundColor Green
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"

# Function to run a benchmark with profiling
function Run-ProfiledBenchmark {
    param(
        [string]$BenchmarkName,
        [string]$Component,
        [string]$ProfileType
    )
    
    $outputFile = "profiles/${Component}_${ProfileType}_${timestamp}.prof"
    Write-Host "Profiling: $BenchmarkName ($ProfileType)" -ForegroundColor Yellow
    
    $profileFlag = "-${ProfileType}profile=$outputFile"
    
    try {
        go test ./storage/ -bench="^$BenchmarkName$" -benchtime=3s -run=^$ $profileFlag 2>&1 | Out-Null
        
        if (Test-Path $outputFile) {
            $fileSize = (Get-Item $outputFile).Length
            Write-Host "  Generated: $outputFile ($fileSize bytes)" -ForegroundColor Green
            return $true
        } else {
            Write-Host "  Warning: Profile not generated" -ForegroundColor Red
            return $false
        }
    } catch {
        Write-Host "  Error: $_" -ForegroundColor Red
        return $false
    }
}

# 1. Sharded Page Table Profiling
Write-Host "`n--- Sharded Page Table ---" -ForegroundColor Cyan
Run-ProfiledBenchmark "BenchmarkShardedPageTableConcurrent" "sharded_table" "cpu"
Run-ProfiledBenchmark "BenchmarkShardedPageTableConcurrent" "sharded_table" "mem"

# 2. Buffer Pool Profiling
Write-Host "`n--- Buffer Pool Manager ---" -ForegroundColor Cyan
Run-ProfiledBenchmark "BenchmarkBufferPoolConcurrent" "buffer_pool" "cpu"
Run-ProfiledBenchmark "BenchmarkBufferPoolConcurrent" "buffer_pool" "mem"

# 3. Transaction Manager (Object Pools)
Write-Host "`n--- Transaction Manager ---" -ForegroundColor Cyan
Run-ProfiledBenchmark "BenchmarkTransactionCommit" "transaction" "cpu"
Run-ProfiledBenchmark "BenchmarkTransactionCommit" "transaction" "mem"

# 4. 2Q Cache Replacement
Write-Host "`n--- 2Q Cache Replacer ---" -ForegroundColor Cyan
Run-ProfiledBenchmark "BenchmarkTwoQReplacerHotSet" "twoq_cache" "cpu"
Run-ProfiledBenchmark "BenchmarkTwoQReplacerPin" "twoq_cache" "mem"

# 5. LRU Cache (for comparison)
Write-Host "`n--- LRU Replacer (Baseline) ---" -ForegroundColor Cyan
Run-ProfiledBenchmark "BenchmarkLRUReplacerHotSet" "lru_cache" "cpu"

Write-Host "`n=== Profiling Complete ===" -ForegroundColor Cyan
Write-Host ""
Write-Host "Generated profiles in ./profiles/" -ForegroundColor Green
Write-Host ""
Write-Host "To analyze profiles, use pprof:" -ForegroundColor Yellow
Write-Host "  go tool pprof -http=:8080 profiles/sharded_table_cpu_${timestamp}.prof"
Write-Host "  go tool pprof -top profiles/buffer_pool_mem_${timestamp}.prof"
Write-Host ""
Write-Host "For more details, see PROFILING_GUIDE.md" -ForegroundColor Cyan
