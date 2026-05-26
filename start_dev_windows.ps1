param(
    [int]$Module = 0
)

$ProjectDir = Split-Path -Parent $MyInvocation.MyCommand.Path

$Modules = @{
    1  = @{ Name = "Data Collection";   Script = "01_data_collection.py" }
    2  = @{ Name = "Slippage Model";    Script = "02_slippage_model.py" }
    3  = @{ Name = "Macro Calendar";    Script = "03_macro_calendar.py" }
    4  = @{ Name = "Signal Generator";  Script = "04_signal_generator.py" }
    5  = @{ Name = "Risk Check";        Script = "05_risk_check.py" }
    6  = @{ Name = "Execution";         Script = "06_execution.py" }
    7  = @{ Name = "Position Monitor";  Script = "07_position_monitor.py" }
    8  = @{ Name = "Portfolio Manager"; Script = "08_portfolio_manager.py" }
    9  = @{ Name = "Tax Tracker";       Script = "09_tax_tracker.py" }
    10 = @{ Name = "Notification";      Script = "10_notification.py" }
    11 = @{ Name = "Health Monitor";    Script = "11_health_monitor.py" }
    12 = @{ Name = "Dashboard";         Script = "dashboard.py" }
}

function Start-KronosModule {
    param([int]$Num)
    $m = $Modules[$Num]
    if (-not $m) {
        Write-Host "Unknown module: $Num"
        return
    }
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
    Write-Host "Kronos - starting all modules"
    foreach ($num in ($Modules.Keys | Sort-Object)) {
        Start-KronosModule $num
        Start-Sleep -Milliseconds 400
    }
    Write-Host "All modules launched."
}
