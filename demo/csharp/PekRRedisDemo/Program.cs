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
//   9) write-advanced / verify-advanced：高级 API 面互通（GETEX/BITFIELD/HGETDEL/LMOVE/SMISMEMBER/ZMPOP/FUNCTION...）
//  10) verify-ops / reset-ops / verify-ops-empty：运维 API 面互通（SLOWLOG/LATENCY）
//  11) deferred-add / deferred-process：RedisDeferred 跨语言集合去重与批处理
//  12) stat-stage / stat-process-once：RedisStat 跨语言统计聚合与延迟落盘
//  13) eventbus-publish / eventbus-subscribe：RedisEventBus 跨语言事件发布与订阅
//  11) exists  ：只读探针，检查某个键是否存在（给严格拓扑联调用）
//  12) set-key ：写入任意单键字符串（给严格拓扑/TLS 联调用）
//  13) selftest：离线校验编码器字节格式（无需 Redis）
//  14) report  ：查看双方回执
//  15) clean   ：清理本 Demo 的键
//  16) auto    ：write + verify + report
//
// 用法：
//   dotnet run --project demo\csharp\PekRRedisDemo -- selftest
//   dotnet run --project demo\csharp\PekRRedisDemo -- auto --config "server=127.0.0.1:6379;password=xxx;db=15"
// 环境变量 REDIS_CONFIG 可作为默认连接串。

using System.Text;
using NewLife.Caching;
using NewLife.Caching.Queues;
using NewLife.Caching.Services;
using NewLife.Data;
using NewLife.Messaging;

Console.OutputEncoding = Encoding.UTF8;

var argsList = args.ToList();
var command = argsList.Count > 0 && !argsList[0].StartsWith('-') ? argsList[0] : "auto";

string GetOpt(string name, string fallback)
{
    var i = argsList.IndexOf(name);
    return i >= 0 && i + 1 < argsList.Count ? argsList[i + 1] : fallback;
}

String[] SplitCsv(String text) => text.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

List<KeyValuePair<String, Int32>> ParsePairs(String text)
{
    var list = new List<KeyValuePair<String, Int32>>();
    foreach (var item in SplitCsv(text))
    {
        var parts = item.Split('=', 2, StringSplitOptions.TrimEntries);
        if (parts.Length == 2 && Int32.TryParse(parts[1], out var value))
            list.Add(new KeyValuePair<String, Int32>(parts[0], value));
    }
    return list;
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
const string AdvancedFunctionLibrary = "#!lua name=advlib\nredis.register_function('echo', function(keys, args) return args[1] end)\n";
const long OpsSlowlogId = 101;
const long OpsSlowlogTimestamp = 1727424000;
const long OpsSlowlogDurationUs = 12345;
string[] OpsSlowlogCommand = ["SET", "ops:key", "42"];
const string OpsLatencyEvent = "command";
const long OpsLatencyTimestamp = 1727424001;
const long OpsLatencyLatestMs = 15;
const long OpsLatencyMaxMs = 42;
const string OpsDoctorText = "latency spikes";

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

    case "write-advanced":
        using (var rds = Connect()) WriteAdvanced(rds);
        break;

    case "verify-advanced":
        using (var rds = Connect()) VerifyAdvanced(rds);
        break;

    case "deferred-add":
        using (var rds = Connect()) DeferredAdd(rds, GetOpt("--name", "deferred:demo"), SplitCsv(GetOpt("--keys", "")));
        break;

    case "deferred-process":
        using (var rds = Connect()) return DeferredProcess(rds, GetOpt("--name", "deferred:demo"), Int32.TryParse(GetOpt("--batch-size", "10"), out var dbs) ? dbs : 10, Int32.TryParse(GetOpt("--timeout", "10"), out var dpt) ? dpt : 10);

    case "stat-stage":
        using (var rds = Connect()) return StatStage(rds, GetOpt("--name", "stat:demo"), GetOpt("--key", "station:1"), ParsePairs(GetOpt("--pairs", "pv=1")), Int32.TryParse(GetOpt("--delay", "0"), out var ssd) ? ssd : 0);

    case "stat-process-once":
        using (var rds = Connect()) return await StatProcessOnce(rds, GetOpt("--name", "stat:demo"), Int32.TryParse(GetOpt("--timeout", "10"), out var spt) ? spt : 10);

    case "eventbus-publish":
        using (var rds = Connect()) return await EventBusPublish(rds, GetOpt("--topic", "eventbus:demo"), GetOpt("--group", "demo"), GetOpt("--name", $"event-from-{Side}"), Int32.TryParse(GetOpt("--count", "1"), out var ebc) ? ebc : 1);

    case "eventbus-subscribe":
        using (var rds = Connect()) return await EventBusSubscribe(rds, GetOpt("--topic", "eventbus:demo"), GetOpt("--group", "demo"), Int32.TryParse(GetOpt("--timeout", "10"), out var ebt) ? ebt : 10);

    case "verify-ops":
        using (var rds = Connect()) VerifyOps(rds);
        break;

    case "reset-ops":
        using (var rds = Connect()) ResetOps(rds);
        break;

    case "verify-ops-empty":
        using (var rds = Connect()) VerifyOpsEmpty(rds);
        break;

    case "exists":
        using (var rds = Connect()) Exists(rds, GetOpt("--key", "rust:marker"));
        break;

    case "set-key":
        using (var rds = Connect()) SetKey(
            rds,
            GetOpt("--key", "probe"),
            GetOpt("--value", "value"),
            Int32.TryParse(GetOpt("--expire", "0"), out var se) ? se : 0);
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
        Console.WriteLine($"未知命令：{command}（可用：selftest/set-key/write/verify/write-advanced/verify-advanced/verify-ops/reset-ops/verify-ops-empty/deferred-add/deferred-process/stat-stage/stat-process-once/eventbus-publish/eventbus-subscribe/exists/push/consume/qstatus/lock/stream-push/stream-consume/stream-status/delay-push/delay-consume/pubsub-publish/pubsub-subscribe/report/clean/auto）");
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

    WriteReceipt(rds);
}

void WriteReceipt(FullRedis rds)
{
    var receipt = new DemoReceipt
    {
        Side = Side,
        Time = DateTime.Now,
        Failures = failures.ToList(),
    };
    rds.Set($"{Side}:receipt", receipt.ToJsonText(), 3600);
    Console.WriteLine($"  · 已写入回执 {prefix}{Side}:receipt");
}

String? RespString(Object? value) => value switch
{
    IPacket pk => pk.ToStr(),
    null => null,
    _ => value.ToString(),
};

Int64 RespInt(Object? value) => Int64.Parse(RespString(value) ?? "0", System.Globalization.CultureInfo.InvariantCulture);

Double RespDouble(Object? value) => Double.Parse(RespString(value) ?? "0", System.Globalization.CultureInfo.InvariantCulture);

void WriteAdvanced(FullRedis rds)
{
    Console.WriteLine($"[write-advanced/{Side}] 写入高级 API 联调样本 → prefix={prefix}");

    try { rds.FunctionDelete("advlib"); } catch { }
    rds.Remove(
        "adv:writer",
        "adv:getex",
        "adv:bits",
        "adv:hash",
        "adv:list:move:src",
        "adv:list:move:dst",
        "adv:list:multi:1",
        "adv:list:multi:2",
        "adv:list:block:empty",
        "adv:list:block:right",
        "adv:list:block:left",
        "adv:set:1",
        "adv:set:2",
        "adv:zset:score",
        "adv:zset:rand",
        "adv:zset:range",
        "adv:zset:range:dest",
        "adv:zset:pop:1",
        "adv:zset:pop:2");

    rds.Set("adv:writer", Side, 3600);
    rds.Set("adv:getex", $"from-{Side}", 0);
    rds.Set("adv:bits", new Byte[] { 0b1010_0000 }, 0);

    var hash = rds.GetDictionary<String>("adv:hash");
    hash["del"] = "value-del";
    hash["ex"] = "value-ex";

    var moveSrc = rds.GetList<String>("adv:list:move:src");
    moveSrc.Add("1");
    moveSrc.Add("2");
    moveSrc.Add("3");
    var multi = rds.GetList<String>("adv:list:multi:2");
    multi.Add("m1");
    multi.Add("m2");
    var blockRight = rds.GetList<String>("adv:list:block:right");
    blockRight.Add("ra");
    blockRight.Add("rb");
    var blockLeft = rds.GetList<String>("adv:list:block:left");
    blockLeft.Add("la");
    blockLeft.Add("lb");

    ((RedisSet<String>)rds.GetSet<String>("adv:set:1")).SAdd("a", "b", "c");
    ((RedisSet<String>)rds.GetSet<String>("adv:set:2")).SAdd("b", "c", "d");

    var zscore = rds.GetSortedSet<String>("adv:zset:score");
    zscore.Add("a", 1.0);
    zscore.Add("b", 2.0);
    var zrand = rds.GetSortedSet<String>("adv:zset:rand");
    zrand.Add("ra", 1.0);
    zrand.Add("rb", 2.0);
    zrand.Add("rc", 3.0);
    var zrange = rds.GetSortedSet<String>("adv:zset:range");
    zrange.Add("a", 1.0);
    zrange.Add("b", 2.0);
    zrange.Add("c", 3.0);
    var zpop = rds.GetSortedSet<String>("adv:zset:pop:1");
    zpop.Add("p1", 1.0);
    zpop.Add("p2", 2.0);

    var lib = rds.FunctionLoad(AdvancedFunctionLibrary, true);
    Console.WriteLine($"  ✔ 已写入高级样本：adv:* + function lib={lib}");
}

void VerifyAdvanced(FullRedis rds)
{
    Console.WriteLine($"[verify-advanced/{Side}] 校验高级 API 面（含对方 {OtherSide} 写入的数据）");

    var writer = rds.Get<String>("adv:writer");
    Check(writer == OtherSide, "adv writer marker", writer);

    var getex = rds.GetEx<String>("adv:getex", 120);
    Check(getex == $"from-{OtherSide}", "GETEX 读取对方样本", getex);
    var expire = rds.ExpireTime("adv:getex");
    Check(expire > 0, "EXPIRETIME > 0", expire.ToString());
    var pexpire = rds.PExpireTime("adv:getex");
    Check(pexpire > 0, "PEXPIRETIME > 0", pexpire.ToString());
    var persist = rds.GetEx<String>("adv:getex", 0);
    Check(persist == $"from-{OtherSide}", "GETEX PERSIST 读回", persist);
    Check(rds.ExpireTime("adv:getex") == -1, "GETEX PERSIST 清除过期", rds.ExpireTime("adv:getex").ToString());
    Check(rds.ObjectIdleTime("adv:getex") == 0, "OBJECT IDLETIME", rds.ObjectIdleTime("adv:getex")?.ToString());
    Check(rds.ObjectFreq("adv:getex") == 0, "OBJECT FREQ", rds.ObjectFreq("adv:getex")?.ToString());

    var bits = rds.BitField("adv:bits", "GET", "u8", "0") ?? [];
    Check(bits.SequenceEqual([160L]), "BITFIELD GET u8 0", string.Join(",", bits));

    var deleted = rds.Execute<String?>("adv:hash", (rc, k) => rc.Execute<String>("HGETDEL", k, "FIELDS", 1, "del"), true);
    Check(deleted == "value-del", "HGETDEL 返回旧值", deleted);
    Check(!rds.GetDictionary<String>("adv:hash").ContainsKey("del"), "HGETDEL 删除字段");
    var kept = rds.Execute<String?>("adv:hash", (rc, k) => rc.Execute<String>("HGETEX", k, "EX", 60, "FIELDS", 1, "ex"), true);
    Check(kept == "value-ex", "HGETEX 返回字段值", kept);

    var moved = rds.LMove<String>("adv:list:move:src", "adv:list:move:dst", "RIGHT", "LEFT");
    Check(moved == "3", "LMOVE RIGHT->LEFT", moved);
    var blmoved = rds.BLMove<String>("adv:list:move:src", "adv:list:move:dst", "LEFT", "RIGHT", 1);
    Check(blmoved == "1", "BLMOVE LEFT->RIGHT", blmoved);
    var movedList = rds.GetList<String>("adv:list:move:dst").ToArray();
    Check(movedList.SequenceEqual(["3", "1"]), "LMOVE/BLMOVE 目标列表顺序", string.Join(",", movedList));

    var lmpop = rds.LMPop<String>(["adv:list:multi:1", "adv:list:multi:2"], true, 2);
    Check(
        lmpop != null
            && lmpop.Item1.EndsWith("adv:list:multi:2", StringComparison.Ordinal)
            && (lmpop.Item2 ?? []).SequenceEqual(["m1", "m2"]),
        "LMPOP 多键弹出",
        lmpop == null ? "null" : $"{lmpop.Item1} => {string.Join(",", lmpop.Item2 ?? [])}");

    var brpopRaw = rds.Execute<Object[]?>("adv:list:block:empty", (rc, k) => rc.Execute<Object[]>("BRPOP", $"{prefix}adv:list:block:empty", $"{prefix}adv:list:block:right", 1), true);
    var brpop = brpopRaw != null && brpopRaw.Length == 2
        ? new Tuple<String, String?>(RespString(brpopRaw[0]) ?? "", RespString(brpopRaw[1]))
        : null;
    Check(
        brpop != null && brpop.Item1.EndsWith("adv:list:block:right", StringComparison.Ordinal) && brpop.Item2 == "rb",
        "BRPOP 多键阻塞弹出",
        brpop == null ? "null" : $"{brpop.Item1} => {brpop.Item2}");

    var blpopRaw = rds.Execute<Object[]?>("adv:list:block:empty", (rc, k) => rc.Execute<Object[]>("BLPOP", $"{prefix}adv:list:block:empty", $"{prefix}adv:list:block:left", 1), true);
    var blpop = blpopRaw != null && blpopRaw.Length == 2
        ? new Tuple<String, String?>(RespString(blpopRaw[0]) ?? "", RespString(blpopRaw[1]))
        : null;
    Check(
        blpop != null && blpop.Item1.EndsWith("adv:list:block:left", StringComparison.Ordinal) && blpop.Item2 == "la",
        "BLPOP 多键阻塞弹出",
        blpop == null ? "null" : $"{blpop.Item1} => {blpop.Item2}");

    var smi = rds.SMIsMember("adv:set:1", "a", "x", "c") ?? [];
    Check(smi.SequenceEqual([1, 0, 1]), "SMISMEMBER 成员存在性", string.Join(",", smi));
    Check(rds.SInterCard(["adv:set:1", "adv:set:2"], 0) == 2, "SINTERCARD 交集基数", rds.SInterCard(["adv:set:1", "adv:set:2"], 0).ToString());

    var zscores = rds.ZMScore("adv:zset:score", "a", "b") ?? [];
    Check(zscores.SequenceEqual([1.0, 2.0]), "ZMSCORE 批量分数", string.Join(",", zscores));
    var zrand = rds.ZRandMember<String>("adv:zset:rand", 2) ?? [];
    Check(zrand.Length == 2 && zrand.All(item => item is "ra" or "rb" or "rc"), "ZRANDMEMBER 随机成员", string.Join(",", zrand));
    Check(rds.GetSortedSet<String>("adv:zset:range").RangeStore("adv:zset:range:dest", 1.5, 3.0, true, false, 0, 10) == 2, "ZRANGESTORE 存储数量");
    var stored = rds.GetSortedSet<String>("adv:zset:range:dest").RangeByScore(0.0, 10.0, 0, 10) ?? [];
    Check(stored.SequenceEqual(["b", "c"]), "ZRANGESTORE 结果可读", string.Join(",", stored));

    var zmpopRaw = rds.Execute<Object[]?>("adv:zset:pop:2", (rc, k) => rc.Execute<Object[]>("ZMPOP", 2, $"{prefix}adv:zset:pop:2", $"{prefix}adv:zset:pop:1", "MIN", "COUNT", 2), true);
    var zmpopKey = zmpopRaw != null && zmpopRaw.Length >= 1 ? RespString(zmpopRaw[0]) : null;
    var zmpopItems = new Dictionary<String, Double>();
    if (zmpopRaw != null && zmpopRaw.Length >= 2 && zmpopRaw[1] is Object[] pairs)
    {
        foreach (var pair in pairs)
        {
            if (pair is not Object[] entry || entry.Length < 2) continue;
            var member = RespString(entry[0]);
            if (member == null) continue;
            zmpopItems[member] = RespDouble(entry[1]);
        }
    }
    var zmpopOk = zmpopKey != null
        && zmpopKey.EndsWith("adv:zset:pop:1", StringComparison.Ordinal)
        && zmpopItems.Count == 2
        && zmpopItems.TryGetValue("p1", out var s1)
        && zmpopItems.TryGetValue("p2", out var s2)
        && Math.Abs(s1 - 1.0) < 1e-9
        && Math.Abs(s2 - 2.0) < 1e-9;
    Check(zmpopOk, "ZMPOP 弹出最小分成员", zmpopKey == null ? "null" : $"{zmpopKey} => {string.Join(",", zmpopItems.Select(kv => $"{kv.Key}:{kv.Value}"))}");

    Check(rds.Wait(1, 10) == 0, "WAIT 单实例确认数", rds.Wait(1, 10).ToString());

    var libs = rds.FunctionList("advlib") ?? [];
    Check(libs.Length > 0, "FUNCTION LIST 可见对方函数库", libs.Length.ToString());
    var echo = rds.Execute<String?>(rc => rc.Execute<String>("FCALL", "echo", 0, "hello-interop"));
    Check(echo == "hello-interop", "FCALL 回声函数", echo);
    var echoRo = rds.Execute<String?>(rc => rc.Execute<String>("FCALL_RO", "echo", 0, "hello-ro"));
    Check(echoRo == "hello-ro", "FCALL_RO 回声函数", echoRo);
    Check(rds.FunctionDelete("advlib") == "OK", "FUNCTION DELETE 删除函数库");
    var libsAfter = rds.FunctionList("advlib") ?? [];
    Check(libsAfter.Length == 0, "FUNCTION DELETE 后列表为空", libsAfter.Length.ToString());

    WriteReceipt(rds);
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

void DeferredAdd(FullRedis rds, String name, String[] keys)
{
    Console.WriteLine($"[deferred-add/{Side}] name={name} keys={String.Join(',', keys)}");
    using var deferred = new RedisDeferred(rds, name);
    var added = deferred.Add(keys);
    Console.WriteLine($"  ✔ added={added}");
}

Int32 DeferredProcess(FullRedis rds, String name, Int32 batchSize, Int32 timeoutSeconds)
{
    Console.WriteLine($"[deferred-process/{Side}] name={name} batch-size={batchSize} timeout={timeoutSeconds}s");

    using var deferred = new RedisDeferred(rds, name)
    {
        BatchSize = batchSize,
        Period = 100,
    };

    var done = new TaskCompletionSource<String[]>(TaskCreationOptions.RunContinuationsAsynchronously);
    deferred.Process += (_, e) =>
    {
        var keys = (e.Keys ?? []).OrderBy(x => x, StringComparer.Ordinal).ToArray();
        done.TrySetResult(keys);
    };

    _ = deferred.Add();

    if (!done.Task.Wait(TimeSpan.FromSeconds(timeoutSeconds)))
    {
        Console.WriteLine("  ✘ timeout waiting deferred batch");
        return 1;
    }

    var keys = done.Task.Result;
    Console.WriteLine($"  ✔ processed={keys.Length} keys={String.Join(',', keys)}");
    return 0;
}

Int32 StatStage(FullRedis rds, String name, String key, IList<KeyValuePair<String, Int32>> pairs, Int32 delay)
{
    Console.WriteLine($"[stat-stage/{Side}] name={name} key={key} delay={delay}s pairs={String.Join(',', pairs.Select(e => $"{e.Key}={e.Value}"))}");

    // 注意：AddDelayQueue 会启动后台转移大循环（把到期消息从 {name}:Delay 搬进主队列），
    // 而本演示进程紧接着退出，后台线程存在被杀死在 ZREM→LPUSH 之间的竞态（实测约 1/3 概率丢消息）。
    // 因此调用方应使用足够大的 delay（>0，保证本进程存活期内消息不到期），让转移确定性地发生在消费者侧。
    using var stat = new RedisStat(rds, name) { OnSave = (_, _) => { } };
    foreach (var item in pairs) stat.Increment(key, item.Key, item.Value);
    stat.AddDelayQueue(key, delay);
    Console.WriteLine("  ✔ queued=1");
    return 0;
}

async Task<Int32> StatProcessOnce(FullRedis rds, String name, Int32 timeoutSeconds)
{
    Console.WriteLine($"[stat-process-once/{Side}] name={name} timeout={timeoutSeconds}s");

    var done = new TaskCompletionSource<(String Key, IDictionary<String, Int32> Data)>(TaskCreationOptions.RunContinuationsAsynchronously);
    using var stat = new RedisStat(rds, name)
    {
        OnSave = (key, data) => done.TrySetResult((key, new Dictionary<String, Int32>(data)))
    };

    // 消费者主动驱动延迟转移：对方可能只把消息写进延迟队列（{name}:Delay），
    // 需要转移到主队列后才能被本进程消费者取到（消费大循环只消费主队列）。
    // C# 后台大循环按 TransferInterval（默认 10 秒）节拍扫描，这里显式驱动保持联调实时性；
    // 与后台循环的 ZREM 争夺安全，语义同 Rust 侧 transfer_due_once。
    var sub = (FullRedis)rds.CreateSub(rds.Db + 1);
    var delayQ = new RedisDelayQueue<String>(sub, $"{name}:Delay");
    var mainQ = sub.GetReliableQueue<String>(name);
    var watch = System.Diagnostics.Stopwatch.StartNew();
    while (!done.Task.IsCompleted && watch.Elapsed < TimeSpan.FromSeconds(timeoutSeconds))
    {
        foreach (var m in delayQ.Take(10)) mainQ.Add(m);
        if (done.Task.IsCompleted) break;
        Thread.Sleep(50);
    }

    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(timeoutSeconds));
    try
    {
        var result = await done.Task.WaitAsync(cts.Token);
        var text = String.Join(',', result.Data.OrderBy(e => e.Key, StringComparer.Ordinal).Select(e => $"{e.Key}={e.Value}"));
        Console.WriteLine($"  ✔ key={result.Key} data={text}");
        return 0;
    }
    catch (OperationCanceledException)
    {
        Console.WriteLine("  ✘ timeout waiting stat save");
        return 1;
    }
}

async Task<Int32> EventBusPublish(FullRedis rds, String topic, String group, String name, Int32 count)
{
    Console.WriteLine($"[eventbus-publish/{Side}] topic={topic} group={group} name={name} count={count}");
    using var bus = new RedisEventBus<ServiceEventDemo>(rds, topic, group);
    await bus.PublishAsync(new ServiceEventDemo { Name = name, Count = count });
    Console.WriteLine("  ✔ published=1");
    return 0;
}

async Task<Int32> EventBusSubscribe(FullRedis rds, String topic, String group, Int32 timeoutSeconds)
{
    Console.WriteLine($"[eventbus-subscribe/{Side}] topic={topic} group={group} timeout={timeoutSeconds}s");
    using var bus = new RedisEventBus<ServiceEventDemo>(rds, topic, group);

    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(timeoutSeconds));
    try
    {
        var receiveTask = bus.ReceiveAsync(cts.Token);
        Console.WriteLine("  · ready");
        var result = await receiveTask;
        Console.WriteLine($"  ✔ name={result.Name} count={result.Count}");
        return 0;
    }
    catch (OperationCanceledException)
    {
        Console.WriteLine("  ✘ timeout waiting event");
        return 1;
    }
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

void Exists(FullRedis rds, string key)
{
    var value = rds.Get<String>(key);
    Console.WriteLine($"[exists/{Side}] key={key} exists={(value != null).ToString().ToLowerInvariant()} value={value ?? ""}");
}

void SetKey(FullRedis rds, string key, string value, int expire)
{
    rds.Set(key, value, expire);
    Console.WriteLine($"[set-key/{Side}] key={key} value={value} expire={expire}");
}

void VerifyOps(FullRedis rds)
{
    Console.WriteLine($"[verify-ops/{Side}] 校验运维 API 面（SLOWLOG/LATENCY）");

    var slowlogLen = rds.Execute<Int32>(rc => rc.Execute<Int32>("SLOWLOG", "LEN"));
    Check(slowlogLen == 1, "SLOWLOG LEN == 1", slowlogLen.ToString());

    var slowlogRaw = rds.Execute<Object[]?>(rc => rc.Execute<Object[]>("SLOWLOG", "GET", 10));
    var slowlogEntry = slowlogRaw?.FirstOrDefault() as Object[];
    Check(slowlogEntry != null, "SLOWLOG GET 返回条目", slowlogRaw == null ? "null" : $"count={slowlogRaw.Length}");
    if (slowlogEntry != null)
    {
        Check(RespInt(slowlogEntry.ElementAtOrDefault(0)) == OpsSlowlogId, "SLOWLOG id", RespInt(slowlogEntry.ElementAtOrDefault(0)).ToString());
        Check(RespInt(slowlogEntry.ElementAtOrDefault(1)) == OpsSlowlogTimestamp, "SLOWLOG timestamp", RespInt(slowlogEntry.ElementAtOrDefault(1)).ToString());
        Check(RespInt(slowlogEntry.ElementAtOrDefault(2)) == OpsSlowlogDurationUs, "SLOWLOG duration_us", RespInt(slowlogEntry.ElementAtOrDefault(2)).ToString());

        var command = (slowlogEntry.ElementAtOrDefault(3) as Object[])?.Select(RespString).Where(x => x != null).Cast<String>().ToArray() ?? [];
        Check(command.SequenceEqual(OpsSlowlogCommand), "SLOWLOG command", String.Join(",", command));
    }

    var historyRaw = rds.Execute<Object[]?>(rc => rc.Execute<Object[]>("LATENCY", "HISTORY", OpsLatencyEvent));
    var historyEntry = historyRaw?.FirstOrDefault() as Object[];
    Check(historyEntry != null && historyRaw?.Length == 1, "LATENCY HISTORY 命中样本事件", historyRaw == null ? "null" : $"count={historyRaw.Length}");
    if (historyEntry != null)
    {
        Check(RespInt(historyEntry.ElementAtOrDefault(0)) == OpsLatencyTimestamp, "LATENCY HISTORY timestamp", RespInt(historyEntry.ElementAtOrDefault(0)).ToString());
        Check(RespInt(historyEntry.ElementAtOrDefault(1)) == OpsLatencyLatestMs, "LATENCY HISTORY latest", RespInt(historyEntry.ElementAtOrDefault(1)).ToString());
    }

    var latestRaw = rds.Execute<Object[]?>(rc => rc.Execute<Object[]>("LATENCY", "LATEST"));
    var latestEntry = (latestRaw ?? []).OfType<Object[]>().FirstOrDefault(x => RespString(x.ElementAtOrDefault(0)) == OpsLatencyEvent);
    Check(latestEntry != null, "LATENCY LATEST 包含样本事件", latestRaw == null ? "null" : $"count={latestRaw.Length}");
    if (latestEntry != null)
    {
        Check(RespInt(latestEntry.ElementAtOrDefault(1)) == OpsLatencyTimestamp, "LATENCY LATEST timestamp", RespInt(latestEntry.ElementAtOrDefault(1)).ToString());
        Check(RespInt(latestEntry.ElementAtOrDefault(2)) == OpsLatencyLatestMs, "LATENCY LATEST latest", RespInt(latestEntry.ElementAtOrDefault(2)).ToString());
        Check(RespInt(latestEntry.ElementAtOrDefault(3)) == OpsLatencyMaxMs, "LATENCY LATEST max", RespInt(latestEntry.ElementAtOrDefault(3)).ToString());
    }

    var doctor = rds.Execute<String?>(rc => rc.Execute<String>("LATENCY", "DOCTOR"));
    Check((doctor ?? "").Contains(OpsDoctorText, StringComparison.OrdinalIgnoreCase), "LATENCY DOCTOR 返回诊断文本", doctor);
}

void ResetOps(FullRedis rds)
{
    Console.WriteLine($"[reset-ops/{Side}] 重置运维 API 样本（SLOWLOG/LATENCY）");

    var before = rds.Execute<Int32>(rc => rc.Execute<Int32>("SLOWLOG", "LEN"));
    Check(before == 1, "SLOWLOG RESET 前条数 == 1", before.ToString());
    _ = rds.Execute<String?>(rc => rc.Execute<String>("SLOWLOG", "RESET"));
    var after = rds.Execute<Int32>(rc => rc.Execute<Int32>("SLOWLOG", "LEN"));
    Check(after == 0, "SLOWLOG RESET 后条数 == 0", after.ToString());

    var reset = rds.Execute<Int32>(rc => rc.Execute<Int32>("LATENCY", "RESET", OpsLatencyEvent));
    Check(reset == 1, "LATENCY RESET 清空样本事件", reset.ToString());
    var historyRaw = rds.Execute<Object[]?>(rc => rc.Execute<Object[]>("LATENCY", "HISTORY", OpsLatencyEvent));
    Check(historyRaw == null || historyRaw.Length == 0, "LATENCY HISTORY 已清空", historyRaw == null ? "null" : $"count={historyRaw.Length}");
}

void VerifyOpsEmpty(FullRedis rds)
{
    Console.WriteLine($"[verify-ops-empty/{Side}] 校验运维 API 样本已被清空");

    var slowlogLen = rds.Execute<Int32>(rc => rc.Execute<Int32>("SLOWLOG", "LEN"));
    Check(slowlogLen == 0, "SLOWLOG LEN == 0", slowlogLen.ToString());

    var slowlogRaw = rds.Execute<Object[]?>(rc => rc.Execute<Object[]>("SLOWLOG", "GET", 10));
    Check(slowlogRaw == null || slowlogRaw.Length == 0, "SLOWLOG GET 为空", slowlogRaw == null ? "null" : $"count={slowlogRaw.Length}");

    var historyRaw = rds.Execute<Object[]?>(rc => rc.Execute<Object[]>("LATENCY", "HISTORY", OpsLatencyEvent));
    Check(historyRaw == null || historyRaw.Length == 0, "LATENCY HISTORY 为空", historyRaw == null ? "null" : $"count={historyRaw.Length}");

    var latestRaw = rds.Execute<Object[]?>(rc => rc.Execute<Object[]>("LATENCY", "LATEST"));
    var hasEvent = (latestRaw ?? []).OfType<Object[]>().Any(x => RespString(x.ElementAtOrDefault(0)) == OpsLatencyEvent);
    Check(!hasEvent, "LATENCY LATEST 不含样本事件", latestRaw == null ? "null" : $"count={latestRaw.Length}");
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
        if (!String.IsNullOrEmpty(m.Id)) ids.Add(m.Id);
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

public class ServiceEventDemo
{
    public String Name { get; set; } = "";
    public Int32 Count { get; set; }
}
