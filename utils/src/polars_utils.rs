// use chrono::{NaiveDate, NaiveDateTime};
// use polars::prelude::*;
// use pyo3_polars::derive::polars_expr;
// use serde::{Deserialize, Serialize};
//
// #[derive(Serialize, Deserialize)]
// pub struct CustomerFormat {
//     format: String,
// }
// #[polars_expr(output_type=Option<NaiveDateTime>)]
// pub fn date_parser_with_format(inputs: &[Series], kwargs: CustomerFormat) -> PolarsResult<Series> {
//     let ca = inputs[0].str()?;
//     let out: DatetimeChunked = ca.apply_into_string_amortized(|value, _output| {
//         // 使用 chrono 进行自定义解析
//         if let Ok(dt) = NaiveDateTime::parse_from_str(value, kwargs.format.as_str()) {
//             Some(dt.timestamp_micros())
//         } else if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
//             Some(date.and_hms_opt(0, 0, 0).unwrap().timestamp_micros())
//         } else {
//             None
//         }
//     });
//     Ok(out.into_series())
// }
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//     #[test]
//     fn test_date_parser_with_format() {
//         let s = Series::new(
//             "date_str".into(),
//             &["2023-10-01 12:34:56", "2023-11-02 23:45:01", "invalid"],
//         );
//         let kwargs = CustomerFormat {
//             format: "%Y-%m-%d %H:%M:%S".to_string(),
//         };
//         let mut df = DataFrame::new(vec![Column::from(s)]).unwrap();
//         let df = df
//             .with_column(
//                 col("date_str").
//             )
//             .unwrap();
//
//     }
// }
