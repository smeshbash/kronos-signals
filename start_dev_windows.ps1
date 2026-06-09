param(
    [int]$Module = 0
)

$ProjectDir = Split-Path -Parent $MyInvocation.MyCommand.Path

# ── Load .env into the current process environment ────────────────────────────
# Each child window launched by Start-Process inherits the parent's env vars,
# so setting them here propagates to every module without any per-module changes.
$envFile = Join-Path $ProjectDir ".env"
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        $line = $_.Trim()
        # Skip blank lines and comments
        if ($line -eq '' -or $line.StartsWith('#')) { return }
        $idx = $line.IndexOf('=')
        if ($idx -lt 1) { return }
        $key   = $line.Substring(0, $idx).Trim()
        $value = $line.Substring($idx + 1).Trim()
        # Strip surrounding quotes if present
        if (($value.StartsWith('"') -and $value.EndsWith('"')) -or
            ($value.StartsWith("'") -and $value.EndsWith("'"))) {
            $value = $value.Substring(1, $value.Length - 2)
        }
        [System.Environment]::SetEnvironmentVariable($key, $value, 'Process')
    }
    Write-Host ".env loaded ($envFile)"
} else {
    Write-Host "WARNING: .env not found at $envFile"
}

# ── Model status — 2026-06-09 ─────────────────────────────────────────────────
# 3 MODELS TRADING: M13 (kronos-mini 1H, filtered) + M15 (kronos-mini 4H) + M16 (kronos-base-4h)
# M14 (kronos-base 1H): generator runs, execution HALTED in 05_risk_check.py (DISABLED_MODEL_SOURCES).
#   Reason: 56 signals, both longs (WR=14%) and shorts (WR=35%) negative EV. Insufficient data
#   to validate any filter. Re-evaluate when 50+ v5 short signals have resolved.
# M13 (kronos-mini 1H): execution filtered via Option A (2026-06-09):
#   - All longs suspended (0-20% WR across all 4H states, 83 signals)
#   - Shorts: 4H bearish+RVOL 0.75-1.50x OR 4H neutral+RVOL<2.0x; 4H bullish=skip
# M15 (kronos-mini 4H): execution filtered (2026-06-09):
#   - All longs suspended (WR=21.4%, EV=-Rs376/trade, 28 signals)
#   - Shorts: skip when synthetic daily (last 6×4H) is bullish (WR=0%, n=3)
#   - RVOL gate omitted (n=17 too thin); re-evaluate after 40+ v5 shorts resolve
# M16 (kronos-base 4H): execution filtered (2026-06-09):
#   - All longs suspended (WR=27.8%, EV=-Rs357/trade, 18 signals)
#   - Shorts: require RVOL 0.75x-1.50x (WR=95% in band vs WR=31% below band)
#             AND skip when synthetic daily (last 6×4H) is bullish (WR=38.5%, n=13)
# Per-symbol halts still active in 06_execution.py (_MODEL_HALTED_SYMBOLS) for weak-edge assets.
$Modules = @{
    1  = @{ Name = "Data Collection";       Script = "01_data_collection.py";    Enabled = $true  }
    2  = @{ Name = "Slippage Model";         Script = "02_slippage_model.py";     Enabled = $true  }
    3  = @{ Name = "Macro Calendar";         Script = "03_macro_calendar.py";     Enabled = $true  }
    4  = @{ Name = "Signal Generator";       Script = "04_signal_generator.py";   Enabled = $false } # HALTED 2026-06-05: -431 Rs/trade expectancy, 91% long-bias, 7% WR on 29 trades — no edge
    5  = @{ Name = "Risk Check";             Script = "05_risk_check.py";         Enabled = $true  }
    6  = @{ Name = "Execution";              Script = "06_execution.py";          Enabled = $true  }
    7  = @{ Name = "Position Monitor";       Script = "07_position_monitor.py";   Enabled = $true  }
    8  = @{ Name = "Portfolio Manager";      Script = "08_portfolio_manager.py";  Enabled = $true  }
    9  = @{ Name = "Tax Tracker";            Script = "09_tax_tracker.py";        Enabled = $true  }
    10 = @{ Name = "Notification";           Script = "10_notification.py";       Enabled = $true  }
    11 = @{ Name = "Health Monitor";         Script = "11_health_monitor.py";     Enabled = $true  }
    12 = @{ Name = "Dashboard";              Script = "dashboard.py";             Enabled = $true  }
    13 = @{ Name = "Kronos-mini 1H";         Script = "13_mini_generator.py";     Enabled = $true  }
    14 = @{ Name = "Kronos-base 1H";           Script = "14_base_generator.py";     Enabled = $true  } # re-enabled 2026-06-08: per-asset TP/SL tuned; BTCUSD+XRPUSD halted in execution
    15 = @{ Name = "Kronos-mini 4H";           Script = "15_mini_4h_generator.py"; Enabled = $true  } # re-enabled 2026-06-08: per-asset TP/SL tuned; BNBUSD halted in execution
    16 = @{ Name = "Kronos-base 4H";           Script = "16_base_4h_generator.py"; Enabled = $true  }
}

function Stop-KronosModule {
    param([int]$Num)
    $m = $Modules[$Num]
    if (-not $m) { return }
    $script = $m.Script

    # Kill any Python process whose command line contains this script name
    $procs = Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue |
             Where-Object { $_.CommandLine -like "*$script*" }
    foreach ($p in $procs) {
        try {
            Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
            Write-Host "  Stopped stale M$Num PID $($p.ProcessId) ($script)"
        } catch {}
    }

    # For the dashboard: also release the port so the new process can bind cleanly
    if ($Num -eq 12) {
        $port = if ($env:KRONOS_DASHBOARD_PORT) { $env:KRONOS_DASHBOARD_PORT } else { '8050' }
        $listening = netstat -ano | Select-String ":$port\s+\S+\s+LISTENING"
        foreach ($line in $listening) {
            $pid_ = ($line.ToString().Trim() -split '\s+')[-1]
            if ($pid_ -match '^\d+$' -and [int]$pid_ -ne 0) {
                try {
                    Stop-Process -Id ([int]$pid_) -Force -ErrorAction SilentlyContinue
                    Write-Host "  Freed port $port (PID $pid_)"
                } catch {}
            }
        }
    }

    # Let the OS reclaim the port before the new process binds
    if ($procs -or $Num -eq 12) { Start-Sleep -Milliseconds 600 }
}

function Start-KronosModule {
    param([int]$Num)
    $m = $Modules[$Num]
    if (-not $m) {
        Write-Host "Unknown module: $Num"
        return
    }

    # Respect the Enabled flag - paused modules are skipped in the default
    # all-modules run but CAN be started explicitly via -Module <N>.
    if (-not $m.Enabled -and $Module -eq 0) {
        Write-Host "  Skipped M$Num : $($m.Name) (PAUSED - start with -Module $Num to override)"
        return
    }

    # Always stop any existing instance first to avoid duplicate processes
    Stop-KronosModule $Num

    $scriptPath = Join-Path $ProjectDir $m.Script
    $title = "Kronos M" + $Num + " - " + $m.Name

    # Build the command as a concatenation of already-expanded strings and
    # single-quoted literals so no characters need escaping.
    $cmd = '$host.UI.RawUI.WindowTitle = ' + "'" + $title + "'" `
         + '; Set-Location '               + "'" + $ProjectDir + "'" `
         + '; python '                     + "'" + $scriptPath + "'"

    Start-Process powershell.exe -ArgumentList "-NoExit", "-Command", $cmd
    Write-Host "  Started M$Num : $($m.Name)"
}

if ($Module -ne 0) {
    Start-KronosModule $Module
} else {
    Write-Host "Kronos - starting all modules (4 models active: M13 mini-1H + M14 base-1H + M15 mini-4H + M16 base-4H)"
    foreach ($num in ($Modules.Keys | Sort-Object)) {
        Start-KronosModule $num
        Start-Sleep -Milliseconds 400
    }
    Write-Host "All modules launched."
}
