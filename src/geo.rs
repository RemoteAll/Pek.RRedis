//! 地理位置结构（对应 DH.NRedis `RedisGeo`）。
//!
//! 覆盖 `GEOADD` / `GEODIST` / `GEOPOS` / `GEOHASH` / `GEORADIUS(BYMEMBER)` / `GEOSEARCH(STORE)`，
//! 应答解析与 C# `GeoInfo` 一致（名称 + 距离 + 坐标）。

use crate::error::{Error, Result};
use crate::full::FullRedis;
use crate::util::int_or;

/// 地理位置信息（对应 C# `GeoInfo`）。
#[derive(Debug, Clone, PartialEq)]
pub struct GeoMember {
    /// 名称
    pub name: String,
    /// 经度
    pub longitude: f64,
    /// 纬度
    pub latitude: f64,
    /// 距离（带 `WITHDIST` 时返回）
    pub distance: Option<f64>,
}

impl GeoMember {
    /// 创建成员（仅名称与坐标）。
    pub fn new(name: &str, longitude: f64, latitude: f64) -> Self {
        Self {
            name: name.to_string(),
            longitude,
            latitude,
            distance: None,
        }
    }
}

/// 地理位置结构。
pub struct RedisGeo {
    redis: FullRedis,
    key: String,
}

impl RedisGeo {
    /// 由工厂方法创建（[`FullRedis::get_geo`]）。键自动补前缀。
    pub fn new(redis: FullRedis, key: &str) -> Self {
        let key = redis.get_key(key);
        Self { redis, key }
    }

    /// 实际键名（含前缀）。
    pub fn key(&self) -> &str {
        &self.key
    }

    /// 添加位置（`GEOADD`）。
    pub fn add(&self, name: &str, longitude: f64, latitude: f64) -> Result<i64> {
        Ok(int_or(
            self.redis.redis().execute(&[
                b"GEOADD",
                self.key.as_bytes(),
                crate::encoder::format_f64(longitude).as_bytes(),
                crate::encoder::format_f64(latitude).as_bytes(),
                name.as_bytes(),
            ])?,
            0,
        ))
    }

    /// 批量添加位置（`GEOADD`）。
    pub fn add_items(&self, items: &[GeoMember]) -> Result<i64> {
        if items.is_empty() {
            return Ok(0);
        }
        let mut args: Vec<Vec<u8>> = Vec::with_capacity(items.len() * 3 + 2);
        args.push(b"GEOADD".to_vec());
        args.push(self.key.as_bytes().to_vec());
        for item in items {
            args.push(crate::encoder::format_f64(item.longitude).into_bytes());
            args.push(crate::encoder::format_f64(item.latitude).into_bytes());
            args.push(item.name.as_bytes().to_vec());
        }
        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        Ok(int_or(self.redis.redis().execute(&refs)?, 0))
    }

    /// 两点距离（`GEODIST`）。单位 `m` / `km` / `mi` / `ft`，默认米。
    pub fn distance(&self, from: &str, to: &str, unit: Option<&str>) -> Result<Option<f64>> {
        let rs = match unit {
            Some(u) if !u.is_empty() => self.redis.redis().execute(&[
                b"GEODIST",
                self.key.as_bytes(),
                from.as_bytes(),
                to.as_bytes(),
                u.as_bytes(),
            ])?,
            _ => self.redis.redis().execute(&[
                b"GEODIST",
                self.key.as_bytes(),
                from.as_bytes(),
                to.as_bytes(),
            ])?,
        };
        Ok(rs.as_f64())
    }

    /// 获取一批坐标（`GEOPOS`），缺失成员为 `None`。
    pub fn position(&self, members: &[&str]) -> Result<Vec<Option<(f64, f64)>>> {
        if members.is_empty() {
            return Ok(Vec::new());
        }
        let mut args: Vec<&[u8]> = Vec::with_capacity(members.len() + 2);
        args.push(b"GEOPOS");
        args.push(self.key.as_bytes());
        for m in members {
            args.push(m.as_bytes());
        }

        let mut result = Vec::with_capacity(members.len());
        for item in self
            .redis
            .redis()
            .execute(&args)?
            .into_array()
            .unwrap_or_default()
        {
            if item.is_null() {
                result.push(None);
                continue;
            }

            let coords = item.into_array().unwrap_or_default();
            let lon = coords
                .first()
                .and_then(|v| v.as_string())
                .and_then(|s| s.parse().ok());
            let lat = coords
                .get(1)
                .and_then(|v| v.as_string())
                .and_then(|s| s.parse().ok());
            result.push(match (lon, lat) {
                (Some(lon), Some(lat)) => Some((lon, lat)),
                _ => None,
            });
        }
        Ok(result)
    }

    /// 获取一批 GeoHash 一维编码（`GEOHASH`）。
    pub fn geohash(&self, members: &[&str]) -> Result<Vec<Option<String>>> {
        if members.is_empty() {
            return Ok(Vec::new());
        }
        let mut args: Vec<&[u8]> = Vec::with_capacity(members.len() + 2);
        args.push(b"GEOHASH");
        args.push(self.key.as_bytes());
        for m in members {
            args.push(m.as_bytes());
        }

        Ok(self
            .redis
            .redis()
            .execute(&args)?
            .into_array()
            .unwrap_or_default()
            .into_iter()
            .map(|v| v.as_string())
            .collect())
    }

    /// 以坐标为中心按半径查询（`GEORADIUS ... WITHDIST WITHCOORD ASC`）。
    pub fn radius_by_coord(
        &self,
        longitude: f64,
        latitude: f64,
        radius: f64,
        unit: &str,
        count: i32,
    ) -> Result<Vec<GeoMember>> {
        let unit = normalize_unit(unit);
        let mut args: Vec<Vec<u8>> = vec![
            b"GEORADIUS".to_vec(),
            self.key.as_bytes().to_vec(),
            crate::encoder::format_f64(longitude).into_bytes(),
            crate::encoder::format_f64(latitude).into_bytes(),
            crate::encoder::format_f64(radius).into_bytes(),
            unit.as_bytes().to_vec(),
            b"WITHDIST".to_vec(),
            b"WITHCOORD".to_vec(),
            b"ASC".to_vec(),
        ];
        if count > 0 {
            args.push(b"COUNT".to_vec());
            args.push(count.to_string().into_bytes());
        }
        parse_geo_array(self.run(&args)?)
    }

    /// 以成员为中心按半径查询（`GEORADIUSBYMEMBER ... WITHDIST WITHCOORD ASC`）。
    pub fn radius_by_member(
        &self,
        member: &str,
        radius: f64,
        unit: &str,
        count: i32,
    ) -> Result<Vec<GeoMember>> {
        let unit = normalize_unit(unit);
        let mut args: Vec<Vec<u8>> = vec![
            b"GEORADIUSBYMEMBER".to_vec(),
            self.key.as_bytes().to_vec(),
            member.as_bytes().to_vec(),
            crate::encoder::format_f64(radius).into_bytes(),
            unit.as_bytes().to_vec(),
            b"WITHDIST".to_vec(),
            b"WITHCOORD".to_vec(),
            b"ASC".to_vec(),
        ];
        if count > 0 {
            args.push(b"COUNT".to_vec());
            args.push(count.to_string().into_bytes());
        }
        parse_geo_array(self.run(&args)?)
    }

    /// 通用空间搜索（`GEOSEARCH`，Redis 6.2+）。
    ///
    /// `member` 与 `(longitude, latitude)` 二选一。
    /// 参数与 C# `RedisGeo.Search` 一一对应，因此保留较多参数。
    #[allow(clippy::too_many_arguments)]
    pub fn search(
        &self,
        member: Option<&str>,
        longitude: Option<f64>,
        latitude: Option<f64>,
        radius: f64,
        unit: &str,
        count: i32,
        ascending: bool,
    ) -> Result<Vec<GeoMember>> {
        let unit = normalize_unit(unit);
        let mut args: Vec<Vec<u8>> = vec![b"GEOSEARCH".to_vec(), self.key.as_bytes().to_vec()];

        if let Some(member) = member.filter(|m| !m.is_empty()) {
            args.push(b"FROMMEMBER".to_vec());
            args.push(member.as_bytes().to_vec());
        } else if let (Some(lon), Some(lat)) = (longitude, latitude) {
            args.push(b"FROMLONLAT".to_vec());
            args.push(crate::encoder::format_f64(lon).into_bytes());
            args.push(crate::encoder::format_f64(lat).into_bytes());
        } else {
            return Err(Error::Type("GEOSEARCH 需要 member 或经纬度中心点".into()));
        }

        args.push(b"BYRADIUS".to_vec());
        args.push(crate::encoder::format_f64(radius).into_bytes());
        args.push(unit.as_bytes().to_vec());
        args.push(if ascending {
            b"ASC".to_vec()
        } else {
            b"DESC".to_vec()
        });
        args.push(b"WITHDIST".to_vec());
        args.push(b"WITHCOORD".to_vec());
        if count > 0 {
            args.push(b"COUNT".to_vec());
            args.push(count.to_string().into_bytes());
        }

        parse_geo_array(self.run(&args)?)
    }

    /// 搜索并存储结果（`GEOSEARCHSTORE`，Redis 6.2+），返回存储数量。
    /// 参数与 C# `RedisGeo.SearchStore` 一一对应。
    #[allow(clippy::too_many_arguments)]
    pub fn search_store(
        &self,
        destination: &str,
        member: Option<&str>,
        longitude: Option<f64>,
        latitude: Option<f64>,
        radius: f64,
        unit: &str,
        count: i32,
        ascending: bool,
        store_distance: bool,
    ) -> Result<i64> {
        let unit = normalize_unit(unit);
        let dest = self.redis.get_key(destination);
        let mut args: Vec<Vec<u8>> = vec![
            b"GEOSEARCHSTORE".to_vec(),
            dest.into_bytes(),
            self.key.as_bytes().to_vec(),
        ];

        if let Some(member) = member.filter(|m| !m.is_empty()) {
            args.push(b"FROMMEMBER".to_vec());
            args.push(member.as_bytes().to_vec());
        } else if let (Some(lon), Some(lat)) = (longitude, latitude) {
            args.push(b"FROMLONLAT".to_vec());
            args.push(crate::encoder::format_f64(lon).into_bytes());
            args.push(crate::encoder::format_f64(lat).into_bytes());
        } else {
            return Err(Error::Type(
                "GEOSEARCHSTORE 需要 member 或经纬度中心点".into(),
            ));
        }

        args.push(b"BYRADIUS".to_vec());
        args.push(crate::encoder::format_f64(radius).into_bytes());
        args.push(unit.as_bytes().to_vec());
        args.push(if ascending {
            b"ASC".to_vec()
        } else {
            b"DESC".to_vec()
        });
        if count > 0 {
            args.push(b"COUNT".to_vec());
            args.push(count.to_string().into_bytes());
        }
        if store_distance {
            args.push(b"STOREDIST".to_vec());
        }

        Ok(int_or(self.run(&args)?, 0))
    }

    fn run(&self, args: &[Vec<u8>]) -> Result<crate::resp::RespValue> {
        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        self.redis.redis().execute(&refs)
    }
}

fn normalize_unit(unit: &str) -> String {
    if unit.is_empty() {
        "m".into()
    } else {
        unit.to_string()
    }
}

/// 解析 `WITHDIST WITHCOORD` 应答：`[[name, dist, [lon, lat]], ...]`。
fn parse_geo_array(value: crate::resp::RespValue) -> Result<Vec<GeoMember>> {
    let mut list = Vec::new();
    for item in value.into_array().unwrap_or_default() {
        let Some(parts) = item.into_array() else {
            continue;
        };
        if parts.len() < 2 {
            continue;
        }

        let name = parts[0].as_string().unwrap_or_default();
        let distance = parts[1].as_string().and_then(|s| s.parse::<f64>().ok());

        let mut member = GeoMember {
            name,
            longitude: 0.0,
            latitude: 0.0,
            distance,
        };

        if let Some(coords) = parts.get(2).and_then(|v| v.clone().into_array())
            && coords.len() >= 2
        {
            member.longitude = coords[0]
                .as_string()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            member.latitude = coords[1]
                .as_string()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
        }

        list.push(member);
    }
    Ok(list)
}
