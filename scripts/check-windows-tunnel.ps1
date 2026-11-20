$ErrorActionPreference = 'Stop'

Write-Host '=== ToTS adapter ==='
Get-NetAdapter -IncludeHidden |
    Where-Object { $_.Name -match 'ToTS|Wintun|tots' -or $_.InterfaceDescription -match 'Wintun' } |
    Format-Table Name, InterfaceDescription, ifIndex, Status, LinkSpeed -AutoSize

Write-Host "`n=== IPv4 full-tunnel routes ==="
$prefixes = @('0.0.0.0/1', '128.0.0.0/1', '0.0.0.0/0')
foreach ($prefix in $prefixes) {
    $rows = @(Get-NetRoute -AddressFamily IPv4 -DestinationPrefix $prefix -PolicyStore ActiveStore -ErrorAction SilentlyContinue |
        Sort-Object RouteMetric, InterfaceMetric)
    Write-Host "-- $prefix --"
    if ($rows.Count -eq 0) {
        Write-Host 'MISSING' -ForegroundColor Red
    } else {
        $rows | Format-Table InterfaceIndex, NextHop, RouteMetric, InterfaceMetric -AutoSize
    }
}

Write-Host "`n=== ToTS IPv6 leak-protection rule ==="
$rules = @(Get-NetFirewallRule -Group 'ToTS Full Tunnel' -ErrorAction SilentlyContinue)
if ($rules.Count -eq 0) {
    Write-Host 'MISSING' -ForegroundColor Red
} else {
    $rules | Format-Table Name, Enabled, Direction, Action, Profile -AutoSize
}

Write-Host "`n=== DNS servers ==="
Get-DnsClientServerAddress -AddressFamily IPv4 |
    Format-Table InterfaceIndex, InterfaceAlias, ServerAddresses -AutoSize
