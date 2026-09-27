// Pek.RRedis 互通验证 Demo —— C# 侧（DH.NRedis）
//
// 与 Rust 侧 examples/demo.rs 使用**同一套固定样本（fixtures）**：
//   1) write   ：写入样本数据（字符串/整数/布尔/时间/JSON/哈希/列表/集合/有序集合/队列）
//   2) verify  ：读取并校验样本（可校验对方写入的数据），并写下回执 receipt
//   3) push    ：向可靠队列推入 N 条消息
//   4) consume ：用可靠队列消费 N 条消息并确认
//   5) qstatus ：查看可靠队列、Ack 队列与消费者状态（Status JSON 可被对方解读）
//   6) lock    ：申请分布式锁、持有若干秒后释放（可与对方进程抢锁）
//   7) stream-push / stream-consume / stream-status：Stream 消息队列（消费组 + 死信抢占）
//   8) pubsub-publish / pubsub-subscribe：跨语言 PubSub（普通/模式/分片）
//   9) selftest：离线校验编码器字节格式（无需 Redis）
//  10) report  ：查看双方回执
//  11) clean   ：清理本 Demo 的键
//  12) auto    ：write + verify + report
//
// 用法：
//   dotnet run --project demo\csharp\PekRRedisDemo -- selftest
//   dotnet run --project demo\csharp\PekRRedisDemo -- auto --config "server=127.0.0.1:6379;password=xxx;db=15"
// 环境变量 REDIS_CONFIG 可作为默认连接串。

using System.Text;
using NewLife.Caching;
using NewLife.Caching.Queues;
using NewLife.Data;

Console.OutputEncoding = Encoding.UTF8;

var argsList = args.ToList();
var command = argsList.Count > 0 && !argsList[0].StartsWith('-') ? argsList[0] : "auto";

string GetOpt(string name, string fallback)
{
    var i = argsList.IndexOf(name);
    return i >= 0 && i + 1 < argsList.Count ? argsList[i + 1] : fallback;
}

var config = GetOpt("--config", Environment.GetEnvironmentVariable("REDIS_CONFIG") ?? "server=127.0.0.1:6379;db=15");
var prefix = GetOpt("--prefix", "pekrredis:demo:");

// ===== 固定样本规范（与 Rust 侧严格一致）=====
const string Side = "csharp";
const string OtherSide = "rust";
var SampleTime = new DateTime(2026, 9, 26, 10, 0, 0, 123); // 裸值编码为 "2026-09-26 10:00:00.123"
// JSON 内时间：C# FastJson 不写毫秒（System.Text.Json 写 ISO 8601），因此样本用整秒，两端均可精确往返
var SampleJsonTime = new DateTime(2026, 9, 26, 10, 0, 0);
const string SampleString = "Hello 互通";
const int SampleInt = 123456789;
const int SampleCount = 7;
const string SampleName = "互通Demo";

var failures = new List<string>();

void Check(bool ok, string what, string? detail = null)
{
    Console.WriteLine(ok ? $"  ✔ {what}" : $"  ✘ {what}  {(detail == null ? "" : detail)}");
    if (!ok) failures.Add(detail == null ? what : $"{what}：{detail}");
}

FullRedis Connect()
{
    var rds = FullRedis.Create(config);
    rds.Prefix = prefix;
    return rds;
}

switch (command)
{
    case "selftest":
        SelfTest();
        break;

    case "write":
        using (var rds = Connect()) Write(rds);
        break;

    case "verify":
        using (var rds = Connect()) Verify(rds);
        break;

    case "push":
        using (var rds = Connect()) Push(rds, int.TryParse(GetOpt("--count", "5"), out var n) ? n : 5);
        break;

    case "consume":
        using (var rds = Connect()) Consume(rds, int.TryParse(GetOpt("--count", "5"), out var n2) ? n2 : 5);
        break;

    case "qstatus":
        using (var rds = Connect()) QueueStatus(rds);
        break;

    case "lock":
        using (var rds = Connect()) Lock(rds, int.TryParse(GetOpt("--seconds", "3"), out var s) ? s : 3);
        break;

    case "stream-push":
        using (var rds = Connect())
            await StreamPush(rds, int.TryParse(GetOpt("--count", "5"), out var sp) ? sp : 5, GetOpt("--group", "demo"));
        break;

    case "stream-consume":
        using (var rds = Connect())
            await StreamConsume(rds,
                int.TryParse(GetOpt("--count", "5"), out var sc) ? sc : 5,
                GetOpt("--group", "demo"),
                argsList.Contains("--no-ack"),
                int.TryParse(GetOpt("--retry-seconds", "60"), out var sr) ? sr : 60);
        break;

    case "stream-status":
        using (var rds = Connect()) StreamStatus(rds, GetOpt("--group", "demo"));
        break;

    case "delay-push":
        using (var rds = Connect())
            DelayPush(rds, int.TryParse(GetOpt("--count", "3"), out var dp) ? dp : 3, int.TryParse(GetOpt("--delay", "2"), out var dd) ? dd : 2);
        break;

    case "delay-consume":
        using (var rds = Connect())
            DelayConsume(rds, int.TryParse(GetOpt("--count", "3"), out var dc) ? dc : 3, int.TryParse(GetOpt("--wait", "15"), out var dw) ? dw : 15);
        break;

    case "pubsub-publish":
        using (var rds = Connect())
            PubSubPublish(
                rds,
                GetOpt("--channel", "pubsub:demo"),
                GetOpt("--message", $"hello-from-{Side}"),
                argsList.Contains("--shard"));
        break;

    case "pubsub-subscribe":
        using (var rds = Connect())
            await PubSubSubscribe(
                rds,
                GetOpt("--channel", "pubsub:demo"),
                GetOpt("--expect", $"hello-from-{OtherSide}"),
                argsList.Contains("--pattern") ? GetOpt("--expect-channel", "pubsub:demo") : null,
                int.TryParse(GetOpt("--timeout", "10"), out var pt) ? pt : 10,
                argsList.Contains("--pattern"),
                argsList.Contains("--shard"));
        break;

    case "report":
        using (var rds = Connect()) Report(rds);
        break;

    case "clean":
        using (var rds = Connect()) Clean(rds);
        break;

    case "auto":
        using (var rds = Connect())
        {
            Write(rds);
            Verify(rds);
            Report(rds);
        }
        break;

    default:
        Console.WriteLine($"未知命令：{command}（可用：selftest/write/verify/push/consume/qstatus/lock/stream-push/stream-consume/stream-status/delay-push/delay-consume/pubsub-publish/pubsub-subscribe/report/clean/auto）");
        return 2;
}

if (failures.Count > 0)
{
    Console.WriteLine($"\n结果：失败 {failures.Count} 项");
    foreach (var f in failures) Console.WriteLine($"  - {f}");
    return 1;
}

Console.WriteLine("\n结果：全部通过");
return 0;

// ======================= 各命令实现 =======================

void SelfTest()
{
    Console.WriteLine("[selftest] 离线校验编码器字节格式（与 Rust 侧编码器逐字节对齐）");

    var encoder = new RedisJsonEncoder { ThrowOnError = true };
    string Encode(object value) => encoder.Encode(value)!.ToStr();

    Check(Encode("hello") == "hello", "字符串原样（无引号）", Encode("hello"));
    Check(Encode(123) == "123", "整数文本", Encode(123));
    Check(Encode(true) == "True", "布尔 True", Encode(true));
    Check(Encode(false) == "False", "布尔 False", Encode(false));
    Check(Encode(1.5) == "1.5", "浮点 1.5", Encode(1.5));
    Check(Encode(SampleTime) == "2026-09-26 10:00:00.123", "时间 yyyy-MM-dd HH:mm:ss.fff", Encode(SampleTime));

    // 解码方向：C# 能读 Rust 写入的格式
    Check(encoder.Decode(new ArrayPacket(Encoding.UTF8.GetBytes("OK")), typeof(Boolean)) is true, "解码 \"OK\" → true");
    Check(encoder.Decode(new ArrayPacket(Encoding.UTF8.GetBytes("True")), typeof(Boolean)) is true, "解码 \"True\" → true");
    var dt = encoder.Decode(new ArrayPacket(Encoding.UTF8.GetBytes("2026-09-26 10:00:00.123")), typeof(DateTime));
    Check(dt is DateTime d && d == SampleTime, "解码时间文本", dt?.ToString());

    // JSON 复杂对象
    var model = new DemoModel { Name = SampleName, CreateTime = SampleJsonTime, Count = SampleCount };
    var json = Encode(model);
    Console.WriteLine($"  · JSON 编码：{json}");
    var back = encoder.Decode(new ArrayPacket(Encoding.UTF8.GetBytes(json)), typeof(DemoModel)) as DemoModel;
    Check(back != null && back.Name == SampleName && back.Count == SampleCount && back.CreateTime == SampleJsonTime,
        "JSON 往返", json);

    // 跨语言：Rust 侧写入的 JSON 时间为 ISO 8601，C# 必须能读
    const string rustJson = "{\"Name\":\"互通Demo\",\"CreateTime\":\"2026-09-26T10:00:00\",\"Count\":7}";
    var rustModel = encoder.Decode(new ArrayPacket(Encoding.UTF8.GetBytes(rustJson)), typeof(DemoModel)) as DemoModel;
    Check(rustModel != null && rustModel.Name == SampleName && rustModel.Count == SampleCount && rustModel.CreateTime == SampleJsonTime,
        "JSON 读取 Rust 的 ISO 时间格式", rustModel == null ? "解码为 null" : rustModel.CreateTime.ToString("yyyy-MM-dd HH:mm:ss.fff"));
}

void Write(FullRedis rds)
{
    Console.WriteLine($"[write/{Side}] 写入固定样本 → prefix={prefix}");

    // 先清空固定样本键（列表/队列是追加语义，必须先重置才能被对方精确校验）
    rds.Remove("str", "int", "bool", "dt", "json", "hash", "list", "set", "zset", "queue");

    // 字符串/整数/布尔/时间：走 C# 默认编码器（与 Rust 编码器一致）
    rds.Set("str", SampleString, 0);
    rds.Set("int", SampleInt, 0);
    rds.Set("bool", true, 0);
    rds.Set("dt", SampleTime, 0);
    rds.Set<DemoModel>("json", new DemoModel { Name = SampleName, CreateTime = SampleJsonTime, Count = SampleCount }, 0);

    // 哈希（字段 a=1、b=2）
    var hash = rds.GetDictionary<Int32>("hash");
    hash["a"] = 1;
    hash["b"] = 2;

    // 列表 [1,2,3]
    var list = rds.GetList<Int32>("list");
    list.Add(1);
    list.Add(2);
    list.Add(3);

    // 集合 {x,y}
    ((RedisSet<String>)rds.GetSet<String>("set")).SAdd("x", "y");

    // 有序集合 m1=1.5 / m2=0.5
    var zset = rds.GetSortedSet<String>("zset");
    zset.Add("m1", 1.5);
    zset.Add("m2", 0.5);

    // 普通队列：LPUSH q1 → LPUSH q2，RPOP 顺序为 q1、q2
    var queue = rds.GetQueue<String>("queue");
    queue.Add("q1");
    queue.Add("q2");

    // 写入本侧标记与时间戳（供对方确认数据来源）
    rds.Set($"{Side}:marker", DateTime.UtcNow.ToString("O"), 3600);

    Console.WriteLine("  ✔ 已写入：str/int/bool/dt/json/hash/list/set/zset/queue/{side}:marker");
}

void Verify(FullRedis rds)
{
    Console.WriteLine($"[verify/{Side}] 校验固定样本（含对方 {OtherSide} 写入的数据）");

    // 数据来源
    var mine = rds.Get<String>($"{Side}:marker");
    var other = rds.Get<String>($"{OtherSide}:marker");
    Console.WriteLine($"  · 本侧标记：{(mine == null ? "无" : "有")}；对方 {OtherSide} 标记：{(other == null ? "无（对方尚未运行 write）" : other)}");

    // 原始字节：确认字符串/布尔/时间的存储格式
    Check(rds.Get<String>("str") == SampleString, "str 读回", rds.Get<String>("str"));
    Check(rds.Get<IPacket>("str")!.ToStr() == SampleString, "str 原始字节（无引号）");
    Check(rds.Get<Int32>("int") == SampleInt, "int 读回", rds.Get<Int32>("int").ToString());
    Check(rds.Get<Boolean>("bool"), "bool 读回");
    Check(rds.Get<IPacket>("bool")!.ToStr() == "True", "bool 原始字节 = True", rds.Get<IPacket>("bool")!.ToStr());
    Check(rds.Get<DateTime>("dt") == SampleTime, "dt 读回", rds.Get<DateTime>("dt").ToString());
    Check(rds.Get<IPacket>("dt")!.ToStr() == "2026-09-26 10:00:00.123", "dt 原始字节", rds.Get<IPacket>("dt")!.ToStr());

    // JSON 对象（字段名 PascalCase；时间在不同 JsonHost 下可能是 "2026-09-26 10:00:00" 或 ISO 8601，此处用字段名与数值校验）
    var jsonText = rds.Get<String>("json");
    Check(jsonText != null && jsonText.Contains("\"Name\":") && jsonText.Contains("\"Count\":7"),
        "json 原始文本", jsonText);
    var model = rds.Get<DemoModel>("json");
    Check(model != null && model.Name == SampleName && model.Count == SampleCount && model.CreateTime == SampleJsonTime,
        "json 反序列化", model == null ? "null" : $"{model.Name}/{model.Count}/{model.CreateTime:yyyy-MM-dd HH:mm:ss.fff}");

    // 哈希
    var dic = rds.GetDictionary<Int32>("hash");
    var hasA = dic.TryGetValue("a", out var va) && va == 1;
    var hasB = dic.TryGetValue("b", out var vb) && vb == 2;
    Check(hasA && hasB, "hash a=1,b=2", $"{va}/{vb}");

    // 列表
    var items = rds.GetList<Int32>("list").ToArray();
    Check(items.SequenceEqual([1, 2, 3]), "list [1,2,3]", string.Join(",", items));

    // 集合
    var set = rds.GetSet<String>("set");
    Check(set.Contains("x") && set.Contains("y"), "set {x,y}");

    // 有序集合
    var zset = rds.GetSortedSet<String>("zset");
    Check(Math.Abs(zset.GetScore("m1") - 1.5) < 1e-9 && Math.Abs(zset.GetScore("m2") - 0.5) < 1e-9, "zset 分数 1.5/0.5");

    // 队列：消费对方入队的消息（跨语言消费验证），随后恢复
    var queue = rds.GetQueue<String>("queue");
    var q1 = queue.TakeOne(-1);
    var q2 = queue.TakeOne(-1);
    Check(q1 == "q1" && q2 == "q2", "queue 消费顺序 q1,q2", $"{q1},{q2}");
    queue.Add("q1");
    queue.Add("q2");

    // 回执
    var receipt = new DemoReceipt
    {
        Side = Side,
        Time = DateTime.Now,
        Failures = failures.ToList(),
    };
    rds.Set($"{Side}:receipt", receipt.ToJsonText(), 3600);
    Console.WriteLine($"  · 已写入回执 {prefix}{Side}:receipt");
}

void Push(FullRedis rds, int count)
{
    Console.WriteLine($"[push/{Side}] 向可靠队列推入 {count} 条消息");
    var queue = rds.GetReliableQueue<String>("reliable");
    for (var i = 1; i <= count; i++) queue.Add($"msg-{i:0000}");
    Console.WriteLine($"  ✔ 队列长度：{queue.Count}（消息格式 msg-0001 ...）");
}

void Consume(FullRedis rds, int count)
{
    Console.WriteLine($"[consume/{Side}] 用可靠队列消费 {count} 条消息并确认（对方 push 的消息同样可消费）");
    var queue = rds.GetReliableQueue<String>("reliable");
    var got = 0;
    for (var i = 0; i < count; i++)
    {
        var msg = queue.TakeOne(-1);
        if (msg == null) break;

        Console.WriteLine($"  · 消费到 {msg}（Ack 队列：{queue.AckKey}）");
        queue.Acknowledge(msg);
        got++;
    }
    Console.WriteLine($"  ✔ 已确认 {got} 条；剩余队列长度：{queue.Count}");
}

void QueueStatus(FullRedis rds)
{
    Console.WriteLine($"[qstatus/{Side}] 可靠队列状态（可看到对方消费者的 Status JSON）");
    var queue = rds.GetReliableQueue<String>("reliable");
    Console.WriteLine($"  · 主队列长度：{queue.Count}");

    var acks = rds.Search($"{prefix}reliable:Ack:*", 0, 1000).ToArray();
    Console.WriteLine($"  · Ack 队列 {acks.Length} 个：{string.Join(", ", acks.Select(Trim))}");

    foreach (var key in rds.Search($"{prefix}reliable:Status:*", 0, 1000))
    {
        var shortKey = Trim(key);
        var json = rds.Get<String>(shortKey);
        // 用 C# 自己的类型解析对方（Rust）写入的状态 JSON
        var status = rds.Get<RedisQueueStatus>(shortKey);
        var parsed = status == null
            ? "解析失败"
            : $"Key={status.Key} Machine={status.MachineName} Consumes={status.Consumes} Acks={status.Acks} LastActive={status.LastActive:yyyy-MM-dd HH:mm:ss.fff}";
        Console.WriteLine($"  · 状态 {shortKey}");
        Console.WriteLine($"     原始：{json}");
        Console.WriteLine($"     解析：{parsed}");
    }

    string Trim(string full) => full.StartsWith(prefix) ? full[prefix.Length..] : full;
}

void Lock(FullRedis rds, int seconds)
{
    Console.WriteLine($"[lock/{Side}] 申请分布式锁 {prefix}lock（持有 {seconds} 秒）");
    using var handle = rds.AcquireLock("lock", seconds * 1000 + 1000, seconds * 1000, false);
    if (handle == null)
    {
        Console.WriteLine("  ✘ 未拿到锁（对方正持有）");
        return;
    }

    Console.WriteLine($"  ✔ 已持锁，锁值 = {rds.Get<String>("lock")}（格式 令牌|绝对过期毫秒）");
    Thread.Sleep(seconds * 1000);
    Console.WriteLine("  · 释放锁");
}

void Report(FullRedis rds)
{
    Console.WriteLine($"[report/{Side}] 双方回执");
    foreach (var side in new[] { Side, OtherSide })
    {
        var json = rds.Get<String>($"{side}:receipt");
        Console.WriteLine($"  · {side,-6}：{json ?? "无（对方尚未运行 verify）"}");
    }
}

// ======================= PubSub =======================

void PubSubPublish(FullRedis rds, string channel, string message, bool shard)
{
    Console.WriteLine($"[pubsub-publish/{Side}] {(shard ? "SPUBLISH" : "PUBLISH")} channel={channel} message={message}");
    var pubsub = new PubSub(rds, channel);
    var delivered = shard ? pubsub.SPublish(message) : pubsub.Publish(message);
    Console.WriteLine($"  ✔ delivered={delivered}");
}

async Task PubSubSubscribe(
    FullRedis rds,
    string channel,
    string expectedMessage,
    string? expectedChannel,
    int timeoutSeconds,
    bool pattern,
    bool shard)
{
    Console.WriteLine($"[pubsub-subscribe/{Side}] {(pattern ? "PSUBSCRIBE" : shard ? "SSUBSCRIBE" : "SUBSCRIBE")} channel={channel} timeout={timeoutSeconds}s expect={expectedMessage}");

    var pubsub = new PubSub(rds, channel);
    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(timeoutSeconds));
    var received = new TaskCompletionSource<(string? Pattern, string Channel, string Message)>(TaskCreationOptions.RunContinuationsAsynchronously);

    try
    {
        if (pattern)
        {
            await pubsub.PSubscribeAsync((pat, ch, msg) =>
            {
                Console.WriteLine($"  · 收到 pattern={pat} channel={ch} message={msg}");
                received.TrySetResult((pat, ch, msg));
                cts.Cancel();
            }, cts.Token);
        }
        else if (shard)
        {
            await pubsub.SSubscribeAsync((ch, msg) =>
            {
                Console.WriteLine($"  · 收到 channel={ch} message={msg}");
                received.TrySetResult((null, ch, msg));
                cts.Cancel();
            }, cts.Token);
        }
        else
        {
            await pubsub.SubscribeAsync((ch, msg) =>
            {
                Console.WriteLine($"  · 收到 channel={ch} message={msg}");
                received.TrySetResult((null, ch, msg));
                cts.Cancel();
            }, cts.Token);
        }
    }
    catch (OperationCanceledException)
    {
        // 收到消息后主动取消，或等待超时；结果在下方统一判断。
    }

    if (!received.Task.IsCompleted)
    {
        Check(false, "PubSub 收到预期消息", $"timeout={timeoutSeconds}s channel={channel}");
        return;
    }

    var result = await received.Task;
    var ok = result.Message == expectedMessage && (expectedChannel == null || result.Channel == expectedChannel);
    Check(ok, "PubSub 收到预期消息", $"channel={result.Channel} message={result.Message}");
}

// ======================= Stream 消息队列 =======================

async Task StreamPush(FullRedis rds, int count, string group)
{
    Console.WriteLine($"[stream-push/{Side}] 写入 {count} 条 Stream 消息（group={group}）");

    // 基元与对象消息各用一个视图（同一键、同一消费组）
    var s1 = rds.GetStream<String>("stream:demo");
    s1.SetGroup(group);
    var s2 = rds.GetStream<DemoModel>("stream:demo");

    for (var i = 1; i <= count; i++)
    {
        var code = $"stream-{i:0000}";
        if (i % 2 == 1)
        {
            var id = s1.Add(code);
            Console.WriteLine($"  · XADD 基元 {code} → {id}（字段 {s1.PrimitiveKey}）");
        }
        else
        {
            var id = s2.Add(new DemoModel { Name = code, CreateTime = SampleTime, Count = i });
            Console.WriteLine($"  · XADD 对象 {code} → {id}（字段 Name/CreateTime/Count）");
        }
    }

    Console.WriteLine($"  ✔ 流长度：{s1.Count}");
}

async Task StreamConsume(FullRedis rds, int count, string group, bool noAck, int retrySeconds)
{
    Console.WriteLine($"[stream-consume/{Side}] 消费最多 {count} 条（group={group}，{(noAck ? "不确认" : "确认")}，retry={retrySeconds}s）");

    var stream = rds.GetStream<String>("stream:demo");
    stream.SetGroup(group);
    stream.RetryInterval = retrySeconds;

    var msgs = await stream.TakeMessagesAsync(count, 1);
    if (msgs == null || msgs.Count == 0)
    {
        Console.WriteLine("  · 没有消息");
        return;
    }

    var ids = new List<String>();
    foreach (var m in msgs)
    {
        Console.WriteLine($"  · {m.Id} body=[{string.Join(",", m.Body ?? [])}]");
        ids.Add(m.Id);
    }

    if (!noAck)
    {
        var n = stream.Acknowledge(ids.ToArray());
        var pending = stream.GetPending(group)?.Count ?? 0;
        Console.WriteLine($"  ✔ 已确认 {n} 条；挂起数：{pending}");
    }
    else
    {
        Console.WriteLine("  · 未确认（留作死信，可用另一侧 --retry-seconds 0 抢占）");
    }
}

void StreamStatus(FullRedis rds, string group)
{
    var stream = rds.GetStream<String>("stream:demo");
    stream.SetGroup(group);

    var info = stream.GetInfo();
    Console.WriteLine($"[stream-status/{Side}] XLEN={stream.Count} last-id={info?.LastGeneratedId} groups={info?.Groups}");

    foreach (var g in stream.GetGroups())
        Console.WriteLine($"  · 组 {g.Name} consumers={g.Consumers} pending={g.Pending} last-delivered={g.LastDeliveredId}");

    var pi = stream.GetPending(group);
    var detail = pi?.Consumers == null ? "" : string.Join(",", pi.Consumers.Select(kv => $"{kv.Key}={kv.Value}"));
    Console.WriteLine($"  · 挂起：{pi?.Count ?? 0} {detail}");

    foreach (var c in stream.GetConsumers(group))
        Console.WriteLine($"  · 消费者 {c.Name} pending={c.Pending} idle={c.Idle}ms");
}

void Clean(FullRedis rds)
{
    // 注意：C# `Remove(pattern)` 内部 Search 默认 count=-1 不返回结果（DH.NRedis 现有边界行为），
    // 这里显式分页扫描后批量删除。
    var keys = rds.Search($"{prefix}*", 0, 1000).ToArray();
    var n = keys.Length > 0 ? rds.Remove(keys) : 0;
    Console.WriteLine($"[clean/{Side}] 已删除 {n} 个键（模式 {prefix}*）");
}

// ======================= 延迟队列 =======================

void DelayPush(FullRedis rds, int count, int delaySeconds)
{
    Console.WriteLine($"[delay-push/{Side}] 写入 {count} 条延迟消息（delay={delaySeconds}s）");
    var queue = rds.GetDelayQueue<String>("delay:demo");
    // 注意：C# 零填充用 {i:0000}（{i:04} 会被当作自定义格式，把 4 当字面量）
    for (var i = 1; i <= count; i++) queue.Add($"delay-{i:0000}", delaySeconds);
    Console.WriteLine($"  ✔ 延迟队列长度：{queue.Count}（score = Unix 秒 + 延迟，与 Rust 一致）");
}

void DelayConsume(FullRedis rds, int count, int waitSeconds)
{
    Console.WriteLine($"[delay-consume/{Side}] 等待并消费最多 {count} 条（最多等 {waitSeconds}s）");
    var queue = rds.GetDelayQueue<String>("delay:demo");

    var got = 0;
    var watch = System.Diagnostics.Stopwatch.StartNew();
    while (got < count && watch.Elapsed.TotalSeconds < waitSeconds)
    {
        var value = queue.TakeOne(-1); // 负数：不等待，只尝试一次
        if (value == null)
        {
            Thread.Sleep(200);
            continue;
        }
        Console.WriteLine($"  · 取到 {value}");
        got++;
    }

    Console.WriteLine($"  ✔ 共取到 {got} 条；剩余：{queue.Count}");
}

/// <summary>固定样本模型（与 Rust 侧 DemoModel 字段一致，属性名 PascalCase）</summary>
public class DemoModel
{
    public String Name { get; set; } = "";
    public DateTime CreateTime { get; set; }
    public Int32 Count { get; set; }
}

/// <summary>校验回执</summary>
public class DemoReceipt
{
    public String Side { get; set; } = "";
    public DateTime Time { get; set; }
    public List<String> Failures { get; set; } = [];

    public String ToJsonText() => System.Text.Json.JsonSerializer.Serialize(this);
}
