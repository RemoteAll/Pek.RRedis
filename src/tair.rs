//! 阿里云 Tair 扩展命令（对应 DH.NRedis `FullRedis` 的 `Tair扩展（阿里云）` 区域）。
//!
//! 这些命令仅在**阿里云 Tair（KVStore）** 实例上可用；在标准 Redis 上执行会返回
//! `ERR unknown command`。Rust 侧与 C# 完全相同的命令形状与返回语义，便于两端代码等价迁移。
//!
//! | C# | pek-rredis |
//! |----|------------|
//! | `ExSet<T>` | [`FullRedis::ex_set`] |
//! | `ExGet<T>` | [`FullRedis::ex_get`] |
//! | `ExIncrBy` | [`FullRedis::ex_incr_by`] |
//! | `ExHSet<T>` | [`FullRedis::ex_hset`] |
//! | `ExHGet<T>` | [`FullRedis::ex_hget`] |
//! | `ExHMGet<T>` | [`FullRedis::ex_hmget`] |
//! | `ExHGetWithVer<T>` | [`FullRedis::ex_hget_with_ver`] |
//! | `ExHIncrBy` | [`FullRedis::ex_hincr_by`] |
//! | `ExHPExpire` | [`FullRedis::ex_hpttl`] |
//! | `ExHKeys` / `ExHVals` / `ExHLen` / `ExHDel` | [`FullRedis::ex_hkeys`] 等 |

use crate::encoder::{FromRedisPayload, ToRedisPayload};
use crate::error::Result;
use crate::full::FullRedis;
use crate::resp::RespValue;
use crate::util::{decode, decode_bytes, int_or, payload};

impl FullRedis {
    /// TairString：带版本号写入（`EXSET key value [EX expire] VER version`，对应 C# `ExSet`）。
    ///
    /// 返回 `OK`；版本不匹配返回错误（由服务端决定）。
    pub fn ex_set<V: ToRedisPayload>(
        &self,
        key: &str,
        value: V,
        version: i64,
        expire_seconds: i32,
    ) -> Result<Option<String>> {
        let key = self.get_key(key);
        let payload = payload(&value)?;
        let version = version.to_string();

        let rs = if expire_seconds > 0 {
            self.redis().execute(&[
                b"EXSET",
                key.as_bytes(),
                &payload,
                b"EX",
                expire_seconds.to_string().as_bytes(),
                b"VER",
                version.as_bytes(),
            ])?
        } else {
            self.redis().execute(&[
                b"EXSET",
                key.as_bytes(),
                &payload,
                b"VER",
                version.as_bytes(),
            ])?
        };

        Ok(rs.as_string())
    }

    /// TairString：读取值及版本号（`EXGET key`，对应 C# `ExGet<T>`），返回 `(值, 版本号)`。
    pub fn ex_get<V: FromRedisPayload>(&self, key: &str) -> Result<Option<(Option<V>, i64)>> {
        let key = self.get_key(key);
        let rs = self.redis().execute(&[b"EXGET", key.as_bytes()])?;
        let Some(mut items) = rs.into_array() else {
            return Ok(None);
        };
        if items.len() < 2 {
            return Ok(None);
        }
        let version = items.pop().and_then(|v| v.as_i64()).unwrap_or(0);
        let value = items.pop().unwrap_or(RespValue::Bulk(Vec::new()));
        Ok(Some((decode(value), version)))
    }

    /// TairString：带版本号自增（`EXINCRBY key increment [EX expire] VER version`，对应 C# `ExIncrBy`）。
    pub fn ex_incr_by(
        &self,
        key: &str,
        increment: i64,
        version: i64,
        expire_seconds: i32,
    ) -> Result<Option<(i64, i64)>> {
        let key = self.get_key(key);
        let increment = increment.to_string();
        let version = version.to_string();

        let rs = if expire_seconds > 0 {
            self.redis().execute(&[
                b"EXINCRBY",
                key.as_bytes(),
                increment.as_bytes(),
                b"EX",
                expire_seconds.to_string().as_bytes(),
                b"VER",
                version.as_bytes(),
            ])?
        } else {
            self.redis().execute(&[
                b"EXINCRBY",
                key.as_bytes(),
                increment.as_bytes(),
                b"VER",
                version.as_bytes(),
            ])?
        };

        let Some(mut items) = rs.into_array() else {
            return Ok(None);
        };
        if items.len() < 2 {
            return Ok(None);
        }
        let ver = items.pop().and_then(|v| v.as_i64()).unwrap_or(0);
        let val = items.pop().and_then(|v| v.as_i64()).unwrap_or(0);
        Ok(Some((val, ver)))
    }

    /// TairHash：字段写入（`EXHSET key field value [EX expire] [NX] [VER ver]`，对应 C# `ExHSet`）。
    pub fn ex_hset<V: ToRedisPayload>(
        &self,
        key: &str,
        field: &str,
        value: V,
        expire_seconds: i32,
        nx: bool,
        ver: i64,
    ) -> Result<i64> {
        let key = self.get_key(key);
        let value = payload(&value)?;
        let mut argv: Vec<Vec<u8>> = vec![
            b"EXHSET".to_vec(),
            key.into_bytes(),
            field.as_bytes().to_vec(),
            value,
        ];
        if expire_seconds > 0 {
            argv.push(b"EX".to_vec());
            argv.push(expire_seconds.to_string().into_bytes());
        }
        if nx {
            argv.push(b"NX".to_vec());
        }
        if ver > 0 {
            argv.push(b"VER".to_vec());
            argv.push(ver.to_string().into_bytes());
        }
        let refs: Vec<&[u8]> = argv.iter().map(|a| a.as_slice()).collect();
        Ok(int_or(self.redis().execute(&refs)?, 0))
    }

    /// TairHash：读取字段（`EXHGET key field`，对应 C# `ExHGet<T>`）。
    pub fn ex_hget<V: FromRedisPayload>(&self, key: &str, field: &str) -> Result<Option<V>> {
        let key = self.get_key(key);
        let rs = self
            .redis()
            .execute(&[b"EXHGET", key.as_bytes(), field.as_bytes()])?;
        Ok(decode(rs))
    }

    /// TairHash：批量读取字段（`EXHMGET key field...`，对应 C# `ExHMGet<T>`）。
    pub fn ex_hmget<V: FromRedisPayload>(&self, key: &str, fields: &[&str]) -> Result<Vec<Option<V>>> {
        let key = self.get_key(key);
        let mut argv: Vec<Vec<u8>> = Vec::with_capacity(fields.len() + 2);
        argv.push(b"EXHMGET".to_vec());
        argv.push(key.into_bytes());
        for f in fields {
            argv.push(f.as_bytes().to_vec());
        }
        let refs: Vec<&[u8]> = argv.iter().map(|a| a.as_slice()).collect();
        let rs = self.redis().execute(&refs)?;
        Ok(rs
            .into_array()
            .unwrap_or_default()
            .into_iter()
            .map(|v| if v.is_null() { None } else { decode_bytes(&v.as_bytes().unwrap_or_default()) })
            .collect())
    }

    /// TairHash：读取字段及版本号（`EXHGETWITHVER key field`，对应 C# `ExHGetWithVer<T>`）。
    pub fn ex_hget_with_ver<V: FromRedisPayload>(
        &self,
        key: &str,
        field: &str,
    ) -> Result<Option<(Option<V>, i64)>> {
        let key = self.get_key(key);
        let rs = self
            .redis()
            .execute(&[b"EXHGETWITHVER", key.as_bytes(), field.as_bytes()])?;
        let Some(mut items) = rs.into_array() else {
            return Ok(None);
        };
        if items.len() < 2 {
            return Ok(None);
        }
        let ver = items.pop().and_then(|v| v.as_i64()).unwrap_or(0);
        let value = items.pop().unwrap_or(RespValue::Bulk(Vec::new()));
        Ok(Some((decode(value), ver)))
    }

    /// TairHash：字段自增（`EXHINCRBY key field increment [EX expire]`，对应 C# `ExHIncrBy`）。
    pub fn ex_hincr_by(
        &self,
        key: &str,
        field: &str,
        increment: i64,
        expire_seconds: i32,
    ) -> Result<i64> {
        let key = self.get_key(key);
        let increment = increment.to_string();
        let rs = if expire_seconds > 0 {
            self.redis().execute(&[
                b"EXHINCRBY",
                key.as_bytes(),
                field.as_bytes(),
                increment.as_bytes(),
                b"EX",
                expire_seconds.to_string().as_bytes(),
            ])?
        } else {
            self.redis().execute(&[
                b"EXHINCRBY",
                key.as_bytes(),
                field.as_bytes(),
                increment.as_bytes(),
            ])?
        };
        Ok(rs.as_i64().unwrap_or(0))
    }

    /// TairHash：字段剩余过期毫秒数（`EXHPTTL key field`，对应 C# `ExHPExpire`）。
    pub fn ex_hpttl(&self, key: &str, field: &str) -> Result<i64> {
        let key = self.get_key(key);
        Ok(int_or(
            self.redis()
                .execute(&[b"EXHPTTL", key.as_bytes(), field.as_bytes()])?,
            -2,
        ))
    }

    /// TairHash：全部字段名（`EXHKEYS key`，对应 C# `ExHKeys`）。
    pub fn ex_hkeys(&self, key: &str) -> Result<Vec<String>> {
        let key = self.get_key(key);
        let rs = self.redis().execute(&[b"EXHKEYS", key.as_bytes()])?;
        Ok(rs
            .into_array()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| v.as_string())
            .collect())
    }

    /// TairHash：全部字段值（`EXHVALS key`，对应 C# `ExHVals<T>`）。
    pub fn ex_hvals<V: FromRedisPayload>(&self, key: &str) -> Result<Vec<V>> {
        let key = self.get_key(key);
        let rs = self.redis().execute(&[b"EXHVALS", key.as_bytes()])?;
        Ok(rs
            .into_array()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| decode_bytes(&v.as_bytes().unwrap_or_default()))
            .collect())
    }

    /// TairHash：字段数量（`EXHLEN key`，对应 C# `ExHLen`）。
    pub fn ex_hlen(&self, key: &str) -> Result<i64> {
        let key = self.get_key(key);
        Ok(int_or(self.redis().execute(&[b"EXHLEN", key.as_bytes()])?, 0))
    }

    /// TairHash：删除字段（`EXHDEL key field...`，对应 C# `ExHDel`）。
    pub fn ex_hdel(&self, key: &str, fields: &[&str]) -> Result<i64> {
        let key = self.get_key(key);
        let mut argv: Vec<Vec<u8>> = Vec::with_capacity(fields.len() + 2);
        argv.push(b"EXHDEL".to_vec());
        argv.push(key.into_bytes());
        for f in fields {
            argv.push(f.as_bytes().to_vec());
        }
        let refs: Vec<&[u8]> = argv.iter().map(|a| a.as_slice()).collect();
        Ok(int_or(self.redis().execute(&refs)?, 0))
    }
}
