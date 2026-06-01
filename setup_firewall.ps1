# Kronos Dashboard — Firewall Setup
# Run once as Administrator: Right-click PowerShell -> "Run as Administrator"
# Then: cd D:\projects\kronos && .\setup_firewall.ps1

Write-Host "Configuring Windows Firewall for Kronos Dashboard (port 8050)..." -ForegroundColor Cyan

# Remove any existing Kronos dashboard rules
Get-NetFirewallRule | Where-Object { $_.DisplayName -like "Kronos Dashboard*" } | ForEach-Object {
    Remove-NetFirewallRule -Name $_.Name
    Write-Host "  Removed: $($_.DisplayName)"
}

# 1. Allow from Tailscale subnet (100.64.0.0/10) — remote access via VPN
New-NetFirewallRule `
    -DisplayName   "Kronos Dashboard - Tailscale" `
    -Direction     Inbound `
    -Protocol      TCP `
    -LocalPort     8050 `
    -RemoteAddress "100.64.0.0/10" `
    -Action        Allow `
    -Profile       Any `
    -Description   "Kronos dashboard remote access via Tailscale VPN" | Out-Null
Write-Host "  [OK] Allow from Tailscale (100.64.0.0/10)" -ForegroundColor Green

# 2. Allow from localhost (health checks / same-machine access)
New-NetFirewallRule `
    -DisplayName   "Kronos Dashboard - Localhost" `
    -Direction     Inbound `
    -Protocol      TCP `
    -LocalPort     8050 `
    -RemoteAddress "127.0.0.1" `
    -Action        Allow `
    -Profile       Any | Out-Null
Write-Host "  [OK] Allow from localhost" -ForegroundColor Green

# 3. Block everything else on port 8050
New-NetFirewallRule `
    -DisplayName   "Kronos Dashboard - Block External" `
    -Direction     Inbound `
    -Protocol      TCP `
    -LocalPort     8050 `
    -Action        Block `
    -Profile       Any `
    -Description   "Block all non-Tailscale access to Kronos dashboard" | Out-Null
Write-Host "  [OK] Block all other inbound on 8050" -ForegroundColor Green

Write-Host "`nDone. Dashboard is accessible only via Tailscale or localhost." -ForegroundColor Cyan
Write-Host "Your Tailscale IP: $(tailscale ip -4 2>$null)" -ForegroundColor Yellow
