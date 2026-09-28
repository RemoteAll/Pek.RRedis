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

function Get-MockRedisExe {
    Join-Path $PWD "target\debug\examples\mock_redis.exe"
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

function New-Utf8TempFile {
    param(
        [string]$Name,
        [string]$Content
    )

    $dir = Join-Path $PWD "target"
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir | Out-Null
    }

    $path = Join-Path $dir $Name
    [System.IO.File]::WriteAllText($path, $Content, [System.Text.Encoding]::UTF8)
    $path
}

function Start-MockRedis {
    param(
        [string]$Name,
        [int]$Port,
        [bool]$Tls,
        [string]$InfoMode,
        [string]$InfoText,
        [string]$InfoReplication,
        [string]$InfoSentinel,
        [string]$ClusterNodes,
        [string]$SlowlogData,
        [string]$LatencyData
    )

    $exe = Get-MockRedisExe
    if (-not (Test-Path $exe)) {
        throw "Mock Redis executable not found: $exe"
    }

    $files = @()
    $args = @("--port", $Port)
    if ($Tls) {
        $args += "--tls"
    }
    if ($InfoMode) {
        $args += @("--info-mode", $InfoMode)
    }
    if ($InfoText) {
        $file = New-Utf8TempFile -Name ("$Name.info.txt") -Content $InfoText
        $files += $file
        $args += @("--info-text-file", $file)
    }
    if ($InfoReplication) {
        $file = New-Utf8TempFile -Name ("$Name.replication.txt") -Content $InfoReplication
        $files += $file
        $args += @("--info-replication-file", $file)
    }
    if ($InfoSentinel) {
        $file = New-Utf8TempFile -Name ("$Name.sentinel.txt") -Content $InfoSentinel
        $files += $file
        $args += @("--info-sentinel-file", $file)
    }
    if ($ClusterNodes) {
        $file = New-Utf8TempFile -Name ("$Name.cluster-nodes.txt") -Content $ClusterNodes
        $files += $file
        $args += @("--cluster-nodes-file", $file)
    }
    if ($SlowlogData) {
        $file = New-Utf8TempFile -Name ("$Name.slowlog.txt") -Content $SlowlogData
        $files += $file
        $args += @("--slowlog-file", $file)
    }
    if ($LatencyData) {
        $file = New-Utf8TempFile -Name ("$Name.latency.txt") -Content $LatencyData
        $files += $file
        $args += @("--latency-file", $file)
    }

    $outFile = Join-Path $PWD "target\$Name.mock.stdout.txt"
    $errFile = Join-Path $PWD "target\$Name.mock.stderr.txt"
    $proc = Start-Subscriber -Exe $exe -CommandArgs $args -OutFile $outFile -ErrFile $errFile -ReadyPattern 'MOCK_ADDR='
    $stdout = Read-Utf8Text $outFile
    $addr = ([regex]::Match($stdout, 'MOCK_ADDR=([^\r\n]+)').Groups[1].Value)
    if (-not $addr) {
        throw "Mock Redis did not expose address. Output:`n$stdout"
    }

    [pscustomobject]@{
        Name = $Name
        Process = $proc
        Addr = $addr
        OutFile = $outFile
        ErrFile = $errFile
        Files = $files
    }
}

function Stop-MockRedis {
    param($Server)

    if ($null -ne $Server.Process -and -not $Server.Process.HasExited) {
        Stop-Process -Id $Server.Process.Id -Force -ErrorAction SilentlyContinue
    }
    foreach ($file in @($Server.Files) + @($Server.OutFile, $Server.ErrFile)) {
        if ($file) {
            Remove-Item $file -ErrorAction SilentlyContinue
        }
    }
}

function Test-ExistsOutput {
    param(
        [string]$Text,
        [bool]$Expected,
        [string]$Name
    )

    Assert-Text $Text ("exists=" + $Expected.ToString().ToLowerInvariant()) $Name
}

function Test-ReplicationInterop {
    $prefix = "${Prefix}repl:"
    $masterPort = 16380
    $replicaPort = 16381
    $masterAddr = "127.0.0.1:$masterPort"
    $replicaAddr = "127.0.0.1:$replicaPort"
    $masterInfo = "# Replication`r`nrole:master`r`nconnected_slaves:1`r`nslave0:ip=127.0.0.1,port=$replicaPort,state=online,offset=1,lag=0`r`n"
    $replicaInfo = "# Replication`r`nrole:slave`r`nmaster_host:127.0.0.1`r`nmaster_port:$masterPort`r`nconnected_slaves:0`r`n"

    $master = Start-MockRedis -Name "strict-repl-master" -Port $masterPort -Tls $false -InfoMode "" -InfoText "" -InfoReplication $masterInfo -InfoSentinel "" -ClusterNodes "" -SlowlogData "" -LatencyData ""
    $replica = Start-MockRedis -Name "strict-repl-replica" -Port $replicaPort -Tls $false -InfoMode "" -InfoText "" -InfoReplication $replicaInfo -InfoSentinel "" -ClusterNodes "" -SlowlogData "" -LatencyData ""
    try {
        $topologyConfig = "server=$masterAddr,$replicaAddr;db=0;mode=replication;readfromreplicas=true"
        $masterConfig = "server=$masterAddr;db=0"
        $replicaConfig = "server=$replicaAddr;db=0"

        Invoke-RustDemo @("clean", "--config", $masterConfig, "--prefix", $prefix) | Out-Host
        Invoke-RustDemo @("clean", "--config", $replicaConfig, "--prefix", $prefix) | Out-Host

        Invoke-CSharpDemo @("write", "--config", $topologyConfig, "--prefix", $prefix) | Out-Host
        $masterHas = Invoke-RustDemo @("exists", "--key", "csharp:marker", "--config", $masterConfig, "--prefix", $prefix)
        $replicaMiss = Invoke-RustDemo @("exists", "--key", "csharp:marker", "--config", $replicaConfig, "--prefix", $prefix)
        Test-ExistsOutput $masterHas $true "Replication write should land on master"
        Test-ExistsOutput $replicaMiss $false "Replication write should not land on replica"

        Invoke-RustDemo @("clean", "--config", $masterConfig, "--prefix", $prefix) | Out-Host
        Invoke-RustDemo @("clean", "--config", $replicaConfig, "--prefix", $prefix) | Out-Host

        Invoke-RustDemo @("write", "--config", $topologyConfig, "--prefix", $prefix) | Out-Host
        $masterHasRust = Invoke-CSharpDemo @("exists", "--key", "rust:marker", "--config", $masterConfig, "--prefix", $prefix)
        $replicaMissRust = Invoke-RustDemo @("exists", "--key", "rust:marker", "--config", $replicaConfig, "--prefix", $prefix)
        Test-ExistsOutput $masterHasRust $true "Rust replication topology write should land on master"
        Test-ExistsOutput $replicaMissRust $false "Rust replication topology write should not land on replica"
    }
    finally {
        Stop-MockRedis $master
        Stop-MockRedis $replica
    }
}

function Test-SentinelInterop {
    $prefix = "${Prefix}sentinel:"
    $sentinelPort = 16382
    $masterPort = 16383
    $replicaPort = 16384
    $sentinelAddr = "127.0.0.1:$sentinelPort"
    $masterAddr = "127.0.0.1:$masterPort"
    $replicaAddr = "127.0.0.1:$replicaPort"
    $sentinelInfo = "# Sentinel`r`nredis_mode:sentinel`r`nsentinel_masters:1`r`nmaster0:name=redis-master,status=ok,address=$masterAddr,slaves=1,sentinels=1`r`n"
    $masterInfo = "# Replication`r`nrole:master`r`nconnected_slaves:1`r`nslave0:ip=127.0.0.1,port=$replicaPort,state=online,offset=1,lag=0`r`n"
    $replicaInfo = "# Replication`r`nrole:slave`r`nmaster_host:127.0.0.1`r`nmaster_port:$masterPort`r`nconnected_slaves:0`r`n"

    $sentinel = Start-MockRedis -Name "strict-sentinel" -Port $sentinelPort -Tls $false -InfoMode "" -InfoText $sentinelInfo -InfoReplication "" -InfoSentinel $sentinelInfo -ClusterNodes "" -SlowlogData "" -LatencyData ""
    $master = Start-MockRedis -Name "strict-sentinel-master" -Port $masterPort -Tls $false -InfoMode "" -InfoText "" -InfoReplication $masterInfo -InfoSentinel "" -ClusterNodes "" -SlowlogData "" -LatencyData ""
    $replica = Start-MockRedis -Name "strict-sentinel-replica" -Port $replicaPort -Tls $false -InfoMode "" -InfoText "" -InfoReplication $replicaInfo -InfoSentinel "" -ClusterNodes "" -SlowlogData "" -LatencyData ""
    try {
        $topologyConfig = "server=$sentinelAddr;db=0;mode=sentinel;sentinelmastername=redis-master;readfromreplicas=true"
        $masterConfig = "server=$masterAddr;db=0"
        $replicaConfig = "server=$replicaAddr;db=0"

        Invoke-RustDemo @("clean", "--config", $masterConfig, "--prefix", $prefix) | Out-Host
        Invoke-RustDemo @("clean", "--config", $replicaConfig, "--prefix", $prefix) | Out-Host

        Invoke-CSharpDemo @("write", "--config", $topologyConfig, "--prefix", $prefix) | Out-Host
        $masterHas = Invoke-RustDemo @("exists", "--key", "csharp:marker", "--config", $masterConfig, "--prefix", $prefix)
        $replicaMiss = Invoke-RustDemo @("exists", "--key", "csharp:marker", "--config", $replicaConfig, "--prefix", $prefix)
        Test-ExistsOutput $masterHas $true "Sentinel write should land on discovered master"
        Test-ExistsOutput $replicaMiss $false "Sentinel write should not land on replica"

        Invoke-RustDemo @("clean", "--config", $masterConfig, "--prefix", $prefix) | Out-Host
        Invoke-RustDemo @("clean", "--config", $replicaConfig, "--prefix", $prefix) | Out-Host

        Invoke-RustDemo @("write", "--config", $topologyConfig, "--prefix", $prefix) | Out-Host
        $masterHasRust = Invoke-CSharpDemo @("exists", "--key", "rust:marker", "--config", $masterConfig, "--prefix", $prefix)
        $replicaMissRust = Invoke-RustDemo @("exists", "--key", "rust:marker", "--config", $replicaConfig, "--prefix", $prefix)
        Test-ExistsOutput $masterHasRust $true "Rust sentinel topology write should land on discovered master"
        Test-ExistsOutput $replicaMissRust $false "Rust sentinel topology write should not land on replica"
    }
    finally {
        Stop-MockRedis $sentinel
        Stop-MockRedis $master
        Stop-MockRedis $replica
    }
}

function Test-ClusterInterop {
    $prefixA = "${Prefix}cluster-a:"
    $prefixB = "${Prefix}cluster-b:"
    $seedPort = 16385
    $targetPort = 16386
    $seedAddr = "127.0.0.1:$seedPort"
    $targetAddr = "127.0.0.1:$targetPort"
    $clusterNodes = "master-a $seedAddr@0 master - 0 0 1 connected 0-5460`nmaster-b $targetAddr@0 master - 0 0 2 connected 5461-16383"

    $seed = Start-MockRedis -Name "strict-cluster-seed" -Port $seedPort -Tls $false -InfoMode "" -InfoText "" -InfoReplication "" -InfoSentinel "" -ClusterNodes $clusterNodes -SlowlogData "" -LatencyData ""
    $target = Start-MockRedis -Name "strict-cluster-target" -Port $targetPort -Tls $false -InfoMode "" -InfoText "" -InfoReplication "" -InfoSentinel "" -ClusterNodes "" -SlowlogData "" -LatencyData ""
    try {
        $topologyConfig = "server=$seedAddr;db=0;mode=cluster"
        $seedConfig = "server=$seedAddr;db=0"
        $targetConfig = "server=$targetAddr;db=0"
        $slotKeyOutput = Invoke-RustDemo @("find-slot-key", "--from", "5461", "--to", "16383", "--key-prefix", "cluster:key:{", "--key-suffix", "}")
        $slotKeyMatch = [regex]::Match($slotKeyOutput, 'key=([^\s]+)')
        if (-not $slotKeyMatch.Success) {
            throw "Unable to parse cluster slot key from output:`n$slotKeyOutput"
        }
        $slotKey = $slotKeyMatch.Groups[1].Value

        Invoke-RustDemo @("clean", "--config", $seedConfig, "--prefix", $prefixA) | Out-Host
        Invoke-RustDemo @("clean", "--config", $targetConfig, "--prefix", $prefixA) | Out-Host

        Invoke-CSharpDemo @("set-key", "--key", $slotKey, "--value", "csharp-cluster", "--config", $topologyConfig, "--prefix", $prefixA) | Out-Host
        $seedMiss = Invoke-RustDemo @("exists", "--key", $slotKey, "--config", $seedConfig, "--prefix", $prefixA)
        $targetHas = Invoke-RustDemo @("exists", "--key", $slotKey, "--config", $targetConfig, "--prefix", $prefixA)
        Test-ExistsOutput $seedMiss $false "Cluster write should not remain on seed node"
        Test-ExistsOutput $targetHas $true "Cluster write should route to target slot owner"

        Invoke-RustDemo @("clean", "--config", $seedConfig, "--prefix", $prefixB) | Out-Host
        Invoke-RustDemo @("clean", "--config", $targetConfig, "--prefix", $prefixB) | Out-Host

        Invoke-RustDemo @("set-key", "--key", $slotKey, "--value", "rust-cluster", "--config", $topologyConfig, "--prefix", $prefixB) | Out-Host
        $seedMissRust = Invoke-CSharpDemo @("exists", "--key", $slotKey, "--config", $seedConfig, "--prefix", $prefixB)
        $targetHasRust = Invoke-CSharpDemo @("exists", "--key", $slotKey, "--config", $targetConfig, "--prefix", $prefixB)
        Test-ExistsOutput $seedMissRust $false "Rust cluster write should not remain on seed node"
        Test-ExistsOutput $targetHasRust $true "Rust cluster write should route to target slot owner"
    }
    finally {
        Stop-MockRedis $seed
        Stop-MockRedis $target
    }
}

function Test-OperationalInterop {
    $portA = 16387
    $portB = 16388
    $configA = "server=127.0.0.1:$portA;db=0"
    $configB = "server=127.0.0.1:$portB;db=0"
    $slowlogSeed = "101|1727424000|12345|SET ops:key 42"
    $latencySeed = "command|1727424001|15|42"

    $caseA = Start-MockRedis -Name "strict-ops-csharp-reset" -Port $portA -Tls $false -InfoMode "" -InfoText "" -InfoReplication "" -InfoSentinel "" -ClusterNodes "" -SlowlogData $slowlogSeed -LatencyData $latencySeed
    try {
        Invoke-CSharpDemo @("verify-ops", "--config", $configA, "--prefix", $Prefix) | Out-Host
        Invoke-RustDemo @("verify-ops", "--config", $configA, "--prefix", $Prefix) | Out-Host
        Invoke-CSharpDemo @("reset-ops", "--config", $configA, "--prefix", $Prefix) | Out-Host
        Invoke-RustDemo @("verify-ops-empty", "--config", $configA, "--prefix", $Prefix) | Out-Host
    }
    finally {
        Stop-MockRedis $caseA
    }

    $caseB = Start-MockRedis -Name "strict-ops-rust-reset" -Port $portB -Tls $false -InfoMode "" -InfoText "" -InfoReplication "" -InfoSentinel "" -ClusterNodes "" -SlowlogData $slowlogSeed -LatencyData $latencySeed
    try {
        Invoke-RustDemo @("verify-ops", "--config", $configB, "--prefix", $Prefix) | Out-Host
        Invoke-RustDemo @("reset-ops", "--config", $configB, "--prefix", $Prefix) | Out-Host
        Invoke-CSharpDemo @("verify-ops-empty", "--config", $configB, "--prefix", $Prefix) | Out-Host
    }
    finally {
        Stop-MockRedis $caseB
    }
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

function Test-TlsInterop {
    $port = 16389
    $tlsServer = Start-MockRedis -Name "strict-tls" -Port $port -Tls $true -InfoMode "" -InfoText "" -InfoReplication "" -InfoSentinel "" -ClusterNodes "" -SlowlogData "" -LatencyData ""
    try {
        $tlsConfig = "server=rediss://127.0.0.1:$port;db=0;ssl=true;tlsinsecure=true;tlsservername=localhost"
        Invoke-RustDemo @("clean", "--config", $tlsConfig, "--prefix", $Prefix) | Out-Host
        Invoke-CSharpDemo @("clean", "--config", $tlsConfig, "--prefix", $Prefix) | Out-Host

        $cWrite = Invoke-CSharpDemo @("write", "--config", $tlsConfig, "--prefix", $Prefix)
        $rVerify = Invoke-RustDemo @("verify", "--config", $tlsConfig, "--prefix", $Prefix)
        $rWrite = Invoke-RustDemo @("write", "--config", $tlsConfig, "--prefix", $Prefix)
        $cVerify = Invoke-CSharpDemo @("verify", "--config", $tlsConfig, "--prefix", $Prefix)
        Assert-Text $rVerify 'rust:receipt' "Rust TLS verify did not write receipt"
        Assert-Text $cVerify 'csharp:receipt' "C# TLS verify did not write receipt"
    }
    finally {
        Stop-MockRedis $tlsServer
    }
}

function Test-AsyncInterop {
    $prefix = "${Prefix}async:"

    Invoke-RustDemo @("clean", "--config", $Config, "--prefix", $prefix) | Out-Host
    Invoke-CSharpDemo @("clean", "--config", $Config, "--prefix", $prefix) | Out-Host

    $cWrite = Invoke-CSharpDemo @("write", "--config", $Config, "--prefix", $prefix)
    $rVerify = Invoke-RustDemo @("verify-async", "--config", $Config, "--prefix", $prefix)
    $rWrite = Invoke-RustDemo @("write-async", "--config", $Config, "--prefix", $prefix)
    $cVerify = Invoke-CSharpDemo @("verify", "--config", $Config, "--prefix", $prefix)
    Assert-Text $rVerify 'rust:receipt' "Rust async verify did not write receipt"
    Assert-Text $cVerify 'csharp:receipt' "C# verify did not confirm Rust async write"

    $cPush = Invoke-CSharpDemo @("push", "--count", "3", "--config", $Config, "--prefix", $prefix)
    $rConsume = Invoke-RustDemo @("consume-async", "--count", "3", "--config", $Config, "--prefix", $prefix)
    $rPush = Invoke-RustDemo @("push-async", "--count", "2", "--config", $Config, "--prefix", $prefix)
    $cConsume = Invoke-CSharpDemo @("consume", "--count", "2", "--config", $Config, "--prefix", $prefix)
    Assert-MatchCount $rConsume 'msg-[0-9]{4}' 3 "Rust async consumer did not receive 3 C# queue messages"
    Assert-MatchCount $cConsume 'msg-[0-9]{4}' 2 "C# consumer did not receive 2 Rust async queue messages"
}

function Test-ServiceInterop {
    $deferredPrefixA = "${Prefix}svc-deferred-a:"
    $deferredPrefixB = "${Prefix}svc-deferred-b:"
    $statPrefixA = "${Prefix}svc-stat-a:"
    $statPrefixB = "${Prefix}svc-stat-b:"
    $eventPrefixA = "${Prefix}svc-event-a:"
    $eventPrefixB = "${Prefix}svc-event-b:"

    Invoke-RustDemo @("clean", "--config", $Config, "--prefix", $deferredPrefixA) | Out-Host
    Invoke-CSharpDemo @("clean", "--config", $Config, "--prefix", $deferredPrefixA) | Out-Host
    Invoke-RustDemo @("deferred-add", "--config", $Config, "--prefix", $deferredPrefixA, "--name", "deferred:demo", "--keys", "a,b,a") | Out-Host
    $cDeferred = Invoke-CSharpDemo @("deferred-process", "--config", $Config, "--prefix", $deferredPrefixA, "--name", "deferred:demo", "--batch-size", "10", "--timeout", "5")
    Assert-Text $cDeferred 'processed=2 keys=a,b' "C# deferred process did not receive Rust deferred batch"

    Invoke-RustDemo @("clean", "--config", $Config, "--prefix", $deferredPrefixB) | Out-Host
    Invoke-CSharpDemo @("clean", "--config", $Config, "--prefix", $deferredPrefixB) | Out-Host
    Invoke-CSharpDemo @("deferred-add", "--config", $Config, "--prefix", $deferredPrefixB, "--name", "deferred:demo", "--keys", "x,y,x") | Out-Host
    $rDeferred = Invoke-RustDemo @("deferred-process", "--config", $Config, "--prefix", $deferredPrefixB, "--name", "deferred:demo", "--batch-size", "10")
    Assert-Text $rDeferred 'processed=2 keys=x,y' "Rust deferred process did not receive C# deferred batch"

    Invoke-RustDemo @("clean", "--config", $Config, "--prefix", $statPrefixA) | Out-Host
    Invoke-CSharpDemo @("clean", "--config", $Config, "--prefix", $statPrefixA) | Out-Host
    Invoke-RustDemo @("stat-stage", "--config", $Config, "--prefix", $statPrefixA, "--name", "stat:demo", "--key", "station:1", "--pairs", "pv=2,uv=3", "--delay", "0") | Out-Host
    $cStat = Invoke-CSharpDemo @("stat-process-once", "--config", $Config, "--prefix", $statPrefixA, "--name", "stat:demo", "--timeout", "5")
    Assert-Text $cStat 'key=station:1 data=pv=2,uv=3' "C# stat process did not persist Rust stat sample"

    Invoke-RustDemo @("clean", "--config", $Config, "--prefix", $statPrefixB) | Out-Host
    Invoke-CSharpDemo @("clean", "--config", $Config, "--prefix", $statPrefixB) | Out-Host
    Invoke-CSharpDemo @("stat-stage", "--config", $Config, "--prefix", $statPrefixB, "--name", "stat:demo", "--key", "station:2", "--pairs", "pv=5,uv=8", "--delay", "0") | Out-Host
    $rStat = Invoke-RustDemo @("stat-process-once", "--config", $Config, "--prefix", $statPrefixB, "--name", "stat:demo", "--timeout", "5")
    Assert-Text $rStat 'key=station:2 .*data=pv=5,uv=8' "Rust stat process did not persist C# stat sample"

    Invoke-RustDemo @("clean", "--config", $Config, "--prefix", $eventPrefixA) | Out-Host
    Invoke-CSharpDemo @("clean", "--config", $Config, "--prefix", $eventPrefixA) | Out-Host
    $eventRustOut = Join-Path $PWD "target\strict-eventbus-rust.out"
    $eventRustErr = Join-Path $PWD "target\strict-eventbus-rust.err"
    $eventRustProc = Start-Subscriber -Exe (Get-RustDemoExe) -CommandArgs @("eventbus-subscribe", "--config", $Config, "--prefix", $eventPrefixA, "--topic", "eventbus:demo", "--group", "demo-rust", "--timeout", "8") -OutFile $eventRustOut -ErrFile $eventRustErr -ReadyPattern 'ready'
    Invoke-CSharpDemo @("eventbus-publish", "--config", $Config, "--prefix", $eventPrefixA, "--topic", "eventbus:demo", "--group", "demo-rust", "--name", "csharp-event", "--count", "7") | Out-Host
    $eventRust = Wait-ProcessOutput -Process $eventRustProc -OutFile $eventRustOut -ErrFile $eventRustErr -SuccessPattern 'name=csharp-event count=7' -Name "Rust eventbus subscriber"

    Invoke-RustDemo @("clean", "--config", $Config, "--prefix", $eventPrefixB) | Out-Host
    Invoke-CSharpDemo @("clean", "--config", $Config, "--prefix", $eventPrefixB) | Out-Host
    $eventCSharpOut = Join-Path $PWD "target\strict-eventbus-csharp.out"
    $eventCSharpErr = Join-Path $PWD "target\strict-eventbus-csharp.err"
    $eventCSharpProc = Start-Subscriber -Exe (Get-CSharpDemoExe) -CommandArgs @("eventbus-subscribe", "--config", $Config, "--prefix", $eventPrefixB, "--topic", "eventbus:demo", "--group", "demo-csharp", "--timeout", "8") -OutFile $eventCSharpOut -ErrFile $eventCSharpErr -ReadyPattern 'ready'
    Invoke-RustDemo @("eventbus-publish", "--config", $Config, "--prefix", $eventPrefixB, "--topic", "eventbus:demo", "--group", "demo-csharp", "--name", "rust-event", "--count", "9") | Out-Host
    $eventCSharp = Wait-ProcessOutput -Process $eventCSharpProc -OutFile $eventCSharpOut -ErrFile $eventCSharpErr -SuccessPattern 'name=rust-event count=9' -Name "C# eventbus subscriber"

    [pscustomobject]@{
        DeferredRustToCSharp = $cDeferred
        DeferredCSharpToRust = $rDeferred
        StatRustToCSharp = $cStat
        StatCSharpToRust = $rStat
        EventBusCSharpToRust = $eventRust
        EventBusRustToCSharp = $eventCSharp
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

Invoke-Step "Replication topology interop" {
    Test-ReplicationInterop
}

Invoke-Step "Sentinel topology interop" {
    Test-SentinelInterop
}

Invoke-Step "Cluster topology interop" {
    Test-ClusterInterop
}

Invoke-Step "Operational API interop" {
    Test-OperationalInterop
}

Invoke-Step "TLS transport interop" {
    Test-TlsInterop
}

Invoke-Step "Async API interop" {
    Test-AsyncInterop
}

Invoke-Step "Service-layer interop" {
    $null = Test-ServiceInterop
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