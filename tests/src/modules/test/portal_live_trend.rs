use async_trait::async_trait;
use chrono::Local;
use common::model::{CronConfig, Headers, ModuleConfig, Request, Response};
use errors::{ParserError, RequestError, Result};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::sync::Arc;
use common::interface::{ModuleNodeTrait, ModuleTrait, SyncBoxStream};
use common::model::data::Data;
use common::model::login_info::LoginInfo;
use common::model::message::ParserData;
use common::model::request::RequestMethod;

#[derive(Debug)]
pub struct PortalLiveTrend {}

#[async_trait]
impl ModuleTrait for PortalLiveTrend {
    fn should_login(&self) -> bool {
        true
    }

    fn name(&self) -> String {
        "sycm_portal_live_trend".to_string()
    }
    fn version(&self) -> i32 {
        1
    }
    async fn headers(&self) -> Headers {
        Headers::default()
            .add("accept", "*/*")
            .add("accept-language", "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7")
            .add("bx-v", "2.5.36")
            .add("cache-control", " no-cache")
            .add("pragma", "no-cache")
            .add("priority", "u=1, i")
            .add("referer", "https://sycm.taobao.com/portal/home.htm")
            .add("onetrace-card-id", "pc新首页|数据概览")
            .add("sycm-referer", "/portal/home.htm")
    }

    fn default_arc() -> Arc<dyn ModuleTrait>
    where
        Self: Sized,
    {
        Arc::new(Self {})
    }

    async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
        vec![Arc::new(PortalLiveTrendNode::default())]
    }
    // fn cron(&self) -> Option<CronConfig> {
    //     Some(CronConfig::right_now())
    // }
}

#[derive(Serialize, Deserialize, Debug)]
struct Params {
    #[serde(rename = "dateType")]
    date_type: String,
    #[serde(rename = "_")]
    t: u128,
    token: String,
}

impl From<Params> for Vec<(String, String)> {
    fn from(value: Params) -> Self {
        vec![
            ("dateType".to_string(), value.date_type),
            ("_".to_string(), value.t.to_string()),
            ("token".to_string(), value.token),
        ]
    }
}
struct PortalLiveTrendNode {
    url: String,
}

impl Default for PortalLiveTrendNode {
    fn default() -> Self {
        Self {
            url: "https://sycm.taobao.com/portal/live/new/index/trend/v3.json".to_string(),
        }
    }
}

#[async_trait]
impl ModuleNodeTrait for PortalLiveTrendNode {
    async fn generate(
        &self,
        _config: Arc<ModuleConfig>,
        _params: Map<String, Value>,
        login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        let mut requests = Vec::new();
        let login_info = login_info.ok_or(RequestError::NotLogin("not login".into()))?;
        // extract token once
        let token: String = login_info
            .get_extra("token")
            .ok_or(RequestError::InvalidParams(
                "token not found in login_info".into(),
            ))?;

        let params = Params {
            date_type: "today".to_string(),
            t: Local::now().timestamp_millis() as u128,
            token,
        };
        let request = Request::new(self.url.clone(), RequestMethod::Get).with_params(params.into());
        requests.push(request);
        Ok(Box::pin(futures::stream::iter(requests)))
    }

    async fn parser(
        &self,
        response: Response,
        _config: Option<Arc<ModuleConfig>>,
    ) -> Result<ParserData> {
        let shop_id = response
            .get_login_config::<i64>("shopid")
            .ok_or(ParserError::InvalidMetaData("shopid".into()))?;
        let data = String::from_utf8_lossy(&response.content).to_string();
        println!("data: {}", data);
        let content = serde_json::from_slice::<Value>(&response.content)?;
        let yesterday_data = content
            .pointer("/content/data/data/yesterday")
            .ok_or(ParserError::DecodeError(
                "cannot find pointer /content/data/data/yesterday".into(),
            ))?
            .as_object()
            .ok_or(ParserError::DecodeError(
                "cannot decode yesterday data as object".into(),
            ))?;
        let today_data = content
            .pointer("/content/data/data/today")
            .ok_or(ParserError::DecodeError(
                "cannot find pointer /content/data/data/today".into(),
            ))?
            .as_object()
            .ok_or(ParserError::DecodeError(
                "cannot decode today data as object".into(),
            ))?;

        let mut yesterday_df = decode_value(yesterday_data)?;
        let mut today_df = decode_value(today_data)?;
        let date = chrono::Local::now().naive_local().date();
        let yesterday_date = date - chrono::Duration::days(1);
        yesterday_df = yesterday_df
            .lazy()
            .with_columns([
                lit(shop_id).alias("shop_id"),
                lit(yesterday_date).alias("date"),
            ])
            .collect()?;
        today_df = today_df
            .lazy()
            .with_columns([lit(shop_id).alias("shop_id"), lit(date).alias("date")])
            .collect()?;
        let yesterday_data = Data::from(&response)
            .with_df(yesterday_df)
            .with_schema("taobao_sycm")
            .with_table("portal_live_trend");
        let today_data = Data::from(&response)
            .with_df(today_df)
            .with_schema("taobao_sycm")
            .with_table("portal_live_trend");

        Ok(ParserData::default().with_data(vec![yesterday_data, today_data]))
    }
}
fn decode_value(value: &Map<String, Value>) -> Result<DataFrame> {
    let mut column_list = vec![];
    for (key, value) in value {
        let decode_value = if key.eq_ignore_ascii_case("statHour") {
            Column::new(
                PlSmallStr::from_str(key),
                serde_json::from_value::<Vec<String>>(value.clone())?,
            )
        } else {
            Column::new(
                PlSmallStr::from_str(key),
                serde_json::from_value::<Vec<f64>>(value.clone())?,
            )
        };
        column_list.append(&mut vec![decode_value]);
    }
    let df = DataFrame::new(column_list)?;
    Ok(df)
}
