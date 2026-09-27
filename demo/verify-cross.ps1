param(
    [string]$Config = "server=127.0.0.1:16379;db=0",
    [string]$Prefix = "pekrredis:e2e:",
    [switch]$UseMock = $true,
    [switch]$NoBuild
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$csharpProject = Join-Path $repoRoot "demo\csharp\PekRRedisDemo\PekRRedisDemo.csproj"

function Quote-PS([string]$Text) {
    return "'" + $Text.Replace("'", "''") + "'"
}

function Invoke-Step([string]$Name, [string]$Command) {
    Write-Host "`n== $Name ==" -ForegroundColor Cyan
    Push-Location $repoRoot
    try {
        Invoke-Expression $Command
        if ($LASTEXITCODE -ne 0) {
            throw "步骤失败：$Name (exit=$LASTEXITCODE)"
        }
    }
    finally {
        Pop-Location
    }
}

function Start-BackgroundStep([string]$Name, [string]$Command) {
    $stdout = [System.IO.Path]::GetTempFileName()
    $stderr = [System.IO.Path]::GetTempFileName()
    $wrapped = "Set-Location $(Quote-PS $repoRoot); $Command"
    $proc = Start-Process powershell.exe -ArgumentList @("-NoProfile", "-Command", $wrapped) -PassThru -WindowStyle Hidden -RedirectStandardOutput $stdout -RedirectStandardError $stderr
    [pscustomobject]@{
        Name = $Name
        Process = $proc
        StdOut = $stdout
        StdErr = $stderr
    }
}

function Wait-BackgroundStep($Step, [int]$TimeoutSeconds = 30) {
    if (-not $Step.Process.WaitForExit($TimeoutSeconds * 1000)) {
        try { $Step.Process.Kill() } catch {}
        throw "后台步骤超时：$($Step.Name)"
    }

    $stdout = if (Test-Path $Step.StdOut) { Get-Content $Step.StdOut -Raw } else { "" }
    $stderr = if (Test-Path $Step.StdErr) { Get-Content $Step.StdErr -Raw } else { "" }

    if ($stdout) { Write-Host $stdout.TrimEnd() }
    if ($stderr) { Write-Host $stderr.TrimEnd() }

    if ($Step.Process.ExitCode -ne 0) {
        throw "后台步骤失败：$($Step.Name) (exit=$($Step.Process.ExitCode))"
    }
}

function Wait-Port([string]$TargetHost, [int]$Port, [int]$TimeoutSeconds = 20) {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $client = $null
        try {
            $client = New-Object System.Net.Sockets.TcpClient
            $iar = $client.BeginConnect($TargetHost, $Port, $null, $null)
            if ($iar.AsyncWaitHandle.WaitOne(500) -and $client.Connected) {
                $client.EndConnect($iar)
                return
            }
        }
        catch {}
        finally {
            if ($client) { $client.Dispose() }
        }
        Start-Sleep -Milliseconds 300
    }
    throw "等待端口超时：${TargetHost}:${Port}"
}

function Assert-OutputContains([string]$Name, [string]$Command, [string]$ExpectedText) {
    Write-Host "`n== $Name ==" -ForegroundColor Cyan
    Push-Location $repoRoot
    try {
        $output = Invoke-Expression $Command 2>&1 | Out-String
        if ($output) { Write-Host $output.TrimEnd() }
        if ($LASTEXITCODE -ne 0) {
            throw "步骤失败：$Name (exit=$LASTEXITCODE)"
        }
        if ($output -notmatch [regex]::Escape($ExpectedText)) {
            throw "步骤 $Name 未包含预期文本：$ExpectedText"
        }
    }
    finally {
        Pop-Location
    }
}

function RustDemo([string]$Args) {
    "cargo run --example demo -- $Args --config $(Quote-PS $Config) --prefix $(Quote-PS $Prefix)"
}

function CSharpDemo([string]$Args) {
    $noBuildArg = if ($NoBuild) { " --no-build" } else { "" }
    "dotnet run --project $(Quote-PS $csharpProject)$noBuildArg -- $Args --config $(Quote-PS $Config) --prefix $(Quote-PS $Prefix)"
}

$mock = $null
try {
    if (-not $NoBuild) {
        Invoke-Step "Build Rust demos" "cargo build --example demo --example mock_redis"
        Invoke-Step "Build C# demo" "dotnet build $(Quote-PS $csharpProject)"
    }

    if ($UseMock) {
        $mock = Start-BackgroundStep "mock redis" "cargo run --example mock_redis"
        Wait-Port -Host "127.0.0.1" -Port 16379 -TimeoutSeconds 30
        Write-Host "mock redis 已就绪：127.0.0.1:16379" -ForegroundColor DarkGreen
    }

    Invoke-Step "Rust selftest" (RustDemo "selftest")
    Invoke-Step "C# selftest" (CSharpDemo "selftest")
    Invoke-Step "Rust clean" (RustDemo "clean")

    Invoke-Step "C# write -> Rust verify" (CSharpDemo "write")
    Invoke-Step "Rust verify C# data" (RustDemo "verify")
    Invoke-Step "Rust write -> C# verify" (RustDemo "write")
    Invoke-Step "C# verify Rust data" (CSharpDemo "verify")

    Invoke-Step "C# push -> Rust consume" (CSharpDemo "push --count 5")
    Invoke-Step "Rust consume 5" (RustDemo "consume --count 5")
    Invoke-Step "Rust push -> C# consume" (RustDemo "push --count 3")
    Invoke-Step "C# consume 3" (CSharpDemo "consume --count 3")
    Invoke-Step "Rust qstatus" (RustDemo "qstatus")
    Invoke-Step "C# qstatus" (CSharpDemo "qstatus")

    Invoke-Step "C# stream push -> Rust consume" (CSharpDemo "stream-push --count 5 --group demo")
    Invoke-Step "Rust stream consume" (RustDemo "stream-consume --count 5 --group demo")
    Invoke-Step "Rust stream push -> C# consume" (RustDemo "stream-push --count 3 --group demo")
    Invoke-Step "C# stream consume" (CSharpDemo "stream-consume --count 3 --group demo")
    Invoke-Step "Rust stream push dead letters" (RustDemo "stream-push --count 2 --group demo")
    Invoke-Step "C# stream consume no ack" (CSharpDemo "stream-consume --count 2 --group demo --no-ack")
    Invoke-Step "Rust stream retry ack" (RustDemo "stream-consume --count 2 --group demo --retry-seconds 0")
    Invoke-Step "C# stream status" (CSharpDemo "stream-status --group demo")
    Invoke-Step "Rust stream status" (RustDemo "stream-status --group demo")

    Invoke-Step "C# delay push -> Rust consume" (CSharpDemo "delay-push --count 3 --delay 2")
    Invoke-Step "Rust delay consume" (RustDemo "delay-consume --count 3 --wait 15")
    Invoke-Step "Rust delay push -> C# consume" (RustDemo "delay-push --count 2 --delay 2")
    Invoke-Step "C# delay consume" (CSharpDemo "delay-consume --count 2 --wait 15")

    $rustSub = Start-BackgroundStep "Rust subscribe normal" (RustDemo "pubsub-subscribe --channel pubsub:demo --expect hello-from-csharp --timeout 10")
    Start-Sleep -Seconds 1
    Invoke-Step "C# publish normal" (CSharpDemo "pubsub-publish --channel pubsub:demo --message hello-from-csharp")
    Wait-BackgroundStep $rustSub 20

    $csharpSub = Start-BackgroundStep "C# subscribe normal" (CSharpDemo "pubsub-subscribe --channel pubsub:demo --expect hello-from-rust --timeout 10")
    Start-Sleep -Seconds 1
    Invoke-Step "Rust publish normal" (RustDemo "pubsub-publish --channel pubsub:demo --message hello-from-rust")
    Wait-BackgroundStep $csharpSub 20

    $rustPattern = Start-BackgroundStep "Rust subscribe pattern" (RustDemo "pubsub-subscribe --pattern --channel pubsub:* --expect hello-pattern-csharp --expect-channel pubsub:demo --timeout 10")
    Start-Sleep -Seconds 1
    Invoke-Step "C# publish pattern target" (CSharpDemo "pubsub-publish --channel pubsub:demo --message hello-pattern-csharp")
    Wait-BackgroundStep $rustPattern 20

    $csharpShard = Start-BackgroundStep "C# subscribe shard" (CSharpDemo "pubsub-subscribe --shard --channel pubsub:shard --expect hello-shard-rust --timeout 10")
    Start-Sleep -Seconds 1
    Invoke-Step "Rust publish shard" (RustDemo "pubsub-publish --shard --channel pubsub:shard --message hello-shard-rust")
    Wait-BackgroundStep $csharpShard 20

    $lockHolder = Start-BackgroundStep "C# hold lock" (CSharpDemo "lock --seconds 6")
    Start-Sleep -Seconds 1
    Assert-OutputContains "Rust lock should fail while C# holds" (RustDemo "lock --seconds 1") "未拿到锁"
    Wait-BackgroundStep $lockHolder 15
    Invoke-Step "Rust acquires lock after release" (RustDemo "lock --seconds 1")

    Invoke-Step "Rust report" (RustDemo "report")
    Invoke-Step "C# report" (CSharpDemo "report")

    Write-Host "`nCross verification completed successfully." -ForegroundColor Green
}
finally {
    if ($mock) {
        try {
            if (-not $mock.Process.HasExited) {
                $mock.Process.Kill()
                $mock.Process.WaitForExit()
            }
        }
        catch {}
    }
}