param(
    [string]$Config = "server=127.0.0.1:16379;db=0",
    [string]$Prefix = "pekrredis:demo:",
    [switch]$NoBuild
)

$ErrorActionPreference = "Stop"

[Console]::InputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

Set-Location (Split-Path -Parent $PSScriptRoot)

function Invoke-Step {
    param(
        [string]$Name,
        [scriptblock]$Action
    )

    Write-Host "=== $Name ==="
    & $Action
}

function Assert-Text {
    param(
        [string]$Text,
        [string]$Pattern,
        [string]$Message
    )

    if ($Text -notmatch $Pattern) {
        throw "$Message`n---- output ----`n$Text"
    }
}

function Assert-MatchCount {
    param(
        [string]$Text,
        [string]$Pattern,
        [int]$Expected,
        [string]$Message
    )

    $count = ([regex]::Matches($Text, $Pattern)).Count
    if ($count -ne $Expected) {
        throw "$Message`nExpected matches: $Expected`nActual matches: $count`n---- output ----`n$Text"
    }
}

function Read-Utf8Text {
    param([string]$Path)

    if (-not (Test-Path $Path)) {
        return ""
    }

    for ($i = 0; $i -lt 10; $i++) {
        try {
            $fs = [System.IO.File]::Open($Path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
            try {
                $reader = New-Object System.IO.StreamReader($fs, [System.Text.Encoding]::UTF8)
                try {
                    return $reader.ReadToEnd()
                }
                finally {
                    $reader.Dispose()
                }
            }
            finally {
                $fs.Dispose()
            }
        }
        catch {
            if ($i -eq 9) {
                throw
            }
            Start-Sleep -Milliseconds 50
        }
    }
}

function Invoke-ExeCapture {
    param(
        [string]$Exe,
        [string[]]$CommandArgs,
        [string]$Name
    )

    $safeName = ($Name -replace '[^a-zA-Z0-9_-]', '_')
    $outFile = Join-Path $PWD "target\$safeName.stdout.txt"
    $errFile = Join-Path $PWD "target\$safeName.stderr.txt"
    Remove-Item $outFile, $errFile -ErrorAction SilentlyContinue

    $proc = Start-Process $Exe -ArgumentList $CommandArgs -RedirectStandardOutput $outFile -RedirectStandardError $errFile -PassThru -Wait
    $stdout = Read-Utf8Text $outFile
    $stderr = Read-Utf8Text $errFile
    if ($proc.ExitCode -ne 0) {
        throw "$Name exited with code $($proc.ExitCode).`nSTDOUT:`n$stdout`nSTDERR:`n$stderr"
    }

    if ($stderr.Trim()) {
        return ($stdout + "`n" + $stderr).Trim()
    }

    $stdout
}

function Get-RustDemoExe {
    Join-Path $PWD "target\debug\examples\demo.exe"
}

function Get-CSharpDemoExe {
    Join-Path $PWD "demo\csharp\PekRRedisDemo\bin\Debug\net10.0\PekRRedisDemo.exe"
}

function Invoke-RustDemo {
    param([string[]]$CommandArgs)
    $exe = Get-RustDemoExe
    if (-not (Test-Path $exe)) {
        throw "Rust demo executable not found: $exe"
    }
    Invoke-ExeCapture -Exe $exe -CommandArgs $CommandArgs -Name ("rust-" + (($CommandArgs -join '-') -replace '[^a-zA-Z0-9_-]', '_'))
}

function Invoke-CSharpDemo {
    param([string[]]$CommandArgs)
    $exe = Get-CSharpDemoExe
    if (-not (Test-Path $exe)) {
        throw "C# demo executable not found: $exe"
    }
    Invoke-ExeCapture -Exe $exe -CommandArgs $CommandArgs -Name ("csharp-" + (($CommandArgs -join '-') -replace '[^a-zA-Z0-9_-]', '_'))
}

function Start-Subscriber {
    param(
        [string]$Exe,
        [string[]]$CommandArgs,
        [string]$OutFile,
        [string]$ErrFile,
        [string]$ReadyPattern
    )

    Remove-Item $OutFile, $ErrFile -ErrorAction SilentlyContinue
    $proc = Start-Process $Exe -ArgumentList $CommandArgs -RedirectStandardOutput $OutFile -RedirectStandardError $ErrFile -PassThru

    $deadline = [DateTime]::UtcNow.AddSeconds(10)
    while ([DateTime]::UtcNow -lt $deadline) {
        if ((Test-Path $OutFile) -and ((Read-Utf8Text $OutFile) -match $ReadyPattern)) {
            return $proc
        }
        Start-Sleep -Milliseconds 100
    }

    $stdout = Read-Utf8Text $OutFile
    $stderr = Read-Utf8Text $ErrFile
    throw "Subscriber failed to become ready.`nSTDOUT:`n$stdout`nSTDERR:`n$stderr"
}

function Wait-ProcessOutput {
    param(
        [System.Diagnostics.Process]$Process,
        [string]$OutFile,
        [string]$ErrFile,
        [string]$SuccessPattern,
        [string]$Name
    )

    Wait-Process -Id $Process.Id -ErrorAction SilentlyContinue
    $Process.Refresh()
    $stdout = Read-Utf8Text $OutFile
    $stderr = Read-Utf8Text $ErrFile
    if (($null -ne $Process.ExitCode) -and ($Process.ExitCode -ne 0)) {
        throw "$Name exited with code $($Process.ExitCode).`nSTDOUT:`n$stdout`nSTDERR:`n$stderr"
    }
    if ($stdout -notmatch $SuccessPattern) {
        throw "$Name failed.`nSTDOUT:`n$stdout`nSTDERR:`n$stderr"
    }
    $stdout
}

function Publish-UntilDelivered {
    param([scriptblock]$Publish)
    for ($i = 0; $i -lt 20; $i++) {
        $text = & $Publish
        if ($text -match 'delivered=([1-9][0-9]*)') {
            return $text
        }
        Start-Sleep -Milliseconds 100
    }
    throw "Publish never reported delivered>0"
}

function Test-ServerReachable {
    $output = Invoke-RustDemo @("report", "--config", $Config, "--prefix", $Prefix)
    Assert-Text $output "\[report/rust\]" "Redis server is not reachable via Rust demo"
}

function Test-LockInterop {
    $out = Join-Path $PWD "target\strict-lock-csharp.out"
    $err = Join-Path $PWD "target\strict-lock-csharp.err"
    $csharpExe = Get-CSharpDemoExe

    $proc = Start-Subscriber -Exe $csharpExe -CommandArgs @("lock", "--seconds", "8", "--config", $Config, "--prefix", $Prefix) -OutFile $out -ErrFile $err -ReadyPattern '\[lock/'

    $during = Invoke-RustDemo @("lock", "--seconds", "1", "--config", $Config, "--prefix", $Prefix)
    if ($during -match '=') {
        throw "Rust unexpectedly acquired lock while C# holds it.`n---- output ----`n$during"
    }

    $csharp = Wait-ProcessOutput -Process $proc -OutFile $out -ErrFile $err -SuccessPattern '=' -Name "C# lock holder"
    $after = Invoke-RustDemo @("lock", "--seconds", "1", "--config", $Config, "--prefix", $Prefix)
    Assert-Text $after '=' "Rust should acquire lock after C# releases it"

    [pscustomobject]@{
        DuringHold = $during
        CSharpHold = $csharp
        AfterRelease = $after
    }
}

function Test-PubSubInterop {
    $rustExe = Get-RustDemoExe
    $csharpExe = Get-CSharpDemoExe

    $normalOut = Join-Path $PWD "target\strict-pubsub-normal-csharp.out"
    $normalErr = Join-Path $PWD "target\strict-pubsub-normal-csharp.err"
    $normalProc = Start-Subscriber -Exe $csharpExe -CommandArgs @("pubsub-subscribe", "--timeout", "10", "--config", $Config, "--prefix", $Prefix, "--expect", "hello-from-rust") -OutFile $normalOut -ErrFile $normalErr -ReadyPattern "pubsub-subscribe"
    $normalPub = Publish-UntilDelivered { Invoke-RustDemo @("pubsub-publish", "--config", $Config, "--prefix", $Prefix, "--message", "hello-from-rust") }
    $normalSub = Wait-ProcessOutput -Process $normalProc -OutFile $normalOut -ErrFile $normalErr -SuccessPattern 'message=hello-from-rust' -Name "normal pubsub subscriber"

    $patternOut = Join-Path $PWD "target\strict-pubsub-pattern-rust.out"
    $patternErr = Join-Path $PWD "target\strict-pubsub-pattern-rust.err"
    $patternProc = Start-Subscriber -Exe $rustExe -CommandArgs @("pubsub-subscribe", "--pattern", "--channel", "pubsub:*", "--expect", "hello-from-csharp", "--expect-channel", "${Prefix}pubsub:demo", "--timeout", "10", "--config", $Config, "--prefix", $Prefix) -OutFile $patternOut -ErrFile $patternErr -ReadyPattern "pubsub-subscribe"
    $patternPub = Publish-UntilDelivered { Invoke-CSharpDemo @("pubsub-publish", "--config", $Config, "--prefix", $Prefix, "--channel", "pubsub:demo", "--message", "hello-from-csharp") }
    $patternSub = Wait-ProcessOutput -Process $patternProc -OutFile $patternOut -ErrFile $patternErr -SuccessPattern 'channel=.*pubsub:demo.*message=hello-from-csharp' -Name "pattern pubsub subscriber"

    $shardOut = Join-Path $PWD "target\strict-pubsub-shard-csharp.out"
    $shardErr = Join-Path $PWD "target\strict-pubsub-shard-csharp.err"
    $shardProc = Start-Subscriber -Exe $csharpExe -CommandArgs @("pubsub-subscribe", "--shard", "--channel", "pubsub:shard", "--timeout", "10", "--config", $Config, "--prefix", $Prefix, "--expect", "hello-from-rust-shard") -OutFile $shardOut -ErrFile $shardErr -ReadyPattern "pubsub-subscribe"
    $shardPub = Publish-UntilDelivered { Invoke-RustDemo @("pubsub-publish", "--config", $Config, "--prefix", $Prefix, "--channel", "pubsub:shard", "--message", "hello-from-rust-shard", "--shard") }
    $shardSub = Wait-ProcessOutput -Process $shardProc -OutFile $shardOut -ErrFile $shardErr -SuccessPattern 'message=hello-from-rust-shard' -Name "shard pubsub subscriber"

    [pscustomobject]@{
        NormalPublish = $normalPub
        NormalSubscribe = $normalSub
        PatternPublish = $patternPub
        PatternSubscribe = $patternSub
        ShardPublish = $shardPub
        ShardSubscribe = $shardSub
    }
}

if (-not $NoBuild) {
    Invoke-Step "Build Rust examples" { cargo build --examples | Out-Host }
    Invoke-Step "Build C# demo" { dotnet build demo\csharp\PekRRedisDemo\PekRRedisDemo.csproj | Out-Host }
}

Invoke-Step "Check server reachable" { Test-ServerReachable | Out-Host }

Invoke-Step "Offline selftest" {
    Invoke-RustDemo @("selftest") | Out-Host
    Invoke-CSharpDemo @("selftest") | Out-Host
}

Invoke-Step "Clean demo keys" {
    Invoke-RustDemo @("clean", "--config", $Config, "--prefix", $Prefix) | Out-Host
    Invoke-CSharpDemo @("clean", "--config", $Config, "--prefix", $Prefix) | Out-Host
}

Invoke-Step "Bidirectional fixed sample read/write" {
    $cWrite = Invoke-CSharpDemo @("write", "--config", $Config, "--prefix", $Prefix)
    $rVerify = Invoke-RustDemo @("verify", "--config", $Config, "--prefix", $Prefix)
    $rWrite = Invoke-RustDemo @("write", "--config", $Config, "--prefix", $Prefix)
    $cVerify = Invoke-CSharpDemo @("verify", "--config", $Config, "--prefix", $Prefix)
    Assert-Text $rVerify 'rust:receipt' "Rust verify did not write receipt"
    Assert-Text $cVerify 'csharp:receipt' "C# verify did not write receipt"
}

Invoke-Step "Advanced helper/direct API interop" {
    $cWriteAdvanced = Invoke-CSharpDemo @("write-advanced", "--config", $Config, "--prefix", $Prefix)
    $rVerifyAdvanced = Invoke-RustDemo @("verify-advanced", "--config", $Config, "--prefix", $Prefix)
    $rWriteAdvanced = Invoke-RustDemo @("write-advanced", "--config", $Config, "--prefix", $Prefix)
    $cVerifyAdvanced = Invoke-CSharpDemo @("verify-advanced", "--config", $Config, "--prefix", $Prefix)
    Assert-Text $rVerifyAdvanced 'rust:receipt' "Rust advanced verify did not write receipt"
    Assert-Text $cVerifyAdvanced 'csharp:receipt' "C# advanced verify did not write receipt"
}

Invoke-Step "Reliable queue bidirectional consume/ack" {
    $cPush = Invoke-CSharpDemo @("push", "--count", "5", "--config", $Config, "--prefix", $Prefix)
    $rConsume = Invoke-RustDemo @("consume", "--count", "5", "--config", $Config, "--prefix", $Prefix)
    $rPush = Invoke-RustDemo @("push", "--count", "3", "--config", $Config, "--prefix", $Prefix)
    $cConsume = Invoke-CSharpDemo @("consume", "--count", "3", "--config", $Config, "--prefix", $Prefix)
    $rStatus = Invoke-RustDemo @("qstatus", "--config", $Config, "--prefix", $Prefix)
    $cStatus = Invoke-CSharpDemo @("qstatus", "--config", $Config, "--prefix", $Prefix)
    Assert-MatchCount $rConsume 'msg-[0-9]{4}' 5 "Rust did not consume 5 queue messages from C#"
    Assert-MatchCount $cConsume 'msg-[0-9]{4}' 3 "C# did not consume 3 queue messages from Rust"
    Assert-Text $rStatus 'Key=.*Consumes=.*Acks=.*LastActive=' "Rust qstatus did not parse peer status JSON"
    Assert-Text $cStatus 'Key=.*Consumes=.*Acks=.*LastActive=' "C# qstatus did not parse peer status JSON"
}

Invoke-Step "Stream bidirectional consume and reclaim" {
    $cPush = Invoke-CSharpDemo @("stream-push", "--count", "5", "--config", $Config, "--prefix", $Prefix)
    $rConsume = Invoke-RustDemo @("stream-consume", "--count", "5", "--config", $Config, "--prefix", $Prefix)
    $rPush = Invoke-RustDemo @("stream-push", "--count", "3", "--config", $Config, "--prefix", $Prefix)
    $cConsume = Invoke-CSharpDemo @("stream-consume", "--count", "3", "--config", $Config, "--prefix", $Prefix)
    $rPush2 = Invoke-RustDemo @("stream-push", "--count", "2", "--config", $Config, "--prefix", $Prefix)
    $cNoAck = Invoke-CSharpDemo @("stream-consume", "--count", "2", "--no-ack", "--config", $Config, "--prefix", $Prefix)
    $rReclaim = Invoke-RustDemo @("stream-consume", "--count", "2", "--retry-seconds", "0", "--config", $Config, "--prefix", $Prefix)
    $rStatus = Invoke-RustDemo @("stream-status", "--config", $Config, "--prefix", $Prefix)
    $cStatus = Invoke-CSharpDemo @("stream-status", "--config", $Config, "--prefix", $Prefix)
    Assert-MatchCount $rConsume 'body=\[' 5 "Rust stream consumer did not receive 5 C# messages"
    Assert-MatchCount $cConsume 'body=\[' 3 "C# stream consumer did not receive 3 Rust messages"
    Assert-MatchCount $rReclaim 'body=\[' 2 "Rust stream reclaim did not receive 2 pending messages"
    Assert-Text $rStatus 'pending=0' "Rust stream status does not show zero pending"
    Assert-Text $cStatus 'pending=0' "C# stream status does not show zero pending"
}

Invoke-Step "Delay queue bidirectional delivery" {
    $cDelay = Invoke-CSharpDemo @("delay-push", "--count", "3", "--delay", "2", "--config", $Config, "--prefix", $Prefix)
    $rDue = Invoke-RustDemo @("delay-consume", "--count", "3", "--wait", "15", "--config", $Config, "--prefix", $Prefix)
    $rDelay = Invoke-RustDemo @("delay-push", "--count", "2", "--delay", "2", "--config", $Config, "--prefix", $Prefix)
    $cDue = Invoke-CSharpDemo @("delay-consume", "--count", "2", "--wait", "15", "--config", $Config, "--prefix", $Prefix)
    Assert-MatchCount $rDue 'delay-[0-9]{4}' 3 "Rust delay consumer did not receive 3 C# delayed messages"
    Assert-MatchCount $cDue 'delay-[0-9]{4}' 2 "C# delay consumer did not receive 2 Rust delayed messages"
}

Invoke-Step "Strict lock interop" {
    $null = Test-LockInterop
}

Invoke-Step "Strict pubsub interop" {
    $null = Test-PubSubInterop
}

Invoke-Step "Receipts clean" {
    $rReport = Invoke-RustDemo @("report", "--config", $Config, "--prefix", $Prefix)
    $cReport = Invoke-CSharpDemo @("report", "--config", $Config, "--prefix", $Prefix)
    Assert-Text $rReport '"Failures":\[\]' "Rust report contains failures"
    Assert-Text $cReport '"Failures":\[\]' "C# report contains failures"
}

Write-Host "=== STRICT INTEROP GATE PASSED ==="