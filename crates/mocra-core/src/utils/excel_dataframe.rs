use crate::utils::type_convert::calamine_value_to_series;
use calamine::{Data, HeaderRow, Range, Reader, Xls, XlsOptions, Xlsx};
use polars::prelude::*;
use std::io::Cursor;

fn resolve_sheet_name(
    sheet_names: &[String],
    sheet_name: Option<&str>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    match sheet_name {
        Some(name) => {
            if sheet_names.iter().any(|sheet| sheet == name) {
                Ok(name.to_string())
            } else {
                Err(format!("Sheet '{name}' not found in the workbook").into())
            }
        }
        None => sheet_names
            .first()
            .cloned()
            .ok_or_else(|| "No sheets found in the workbook".into()),
    }
}

pub fn xlsx_dataframe(
    data: &[u8],
    sheet_name: Option<&str>,
    skip_rows: usize,
    first_row_as_title: bool,
) -> Result<DataFrame, Box<dyn std::error::Error + Send + Sync>> {
    let cursor = Cursor::new(data);

    let mut workbook: Xlsx<_> = calamine::open_workbook_from_rs(cursor)?;
    let sheet_names = workbook.sheet_names();
    let sheet_name = resolve_sheet_name(&sheet_names, sheet_name)?;
    let sheet = workbook
        .with_header_row(HeaderRow::Row(skip_rows as u32))
        .worksheet_range(&sheet_name)?;
    sheet_dataframe(sheet, first_row_as_title)
}

pub fn xls_dataframe(
    data: &[u8],
    sheet_name: Option<&str>,
    skip_rows: usize,
    first_row_as_title: bool,
) -> Result<DataFrame, Box<dyn std::error::Error + Send + Sync>> {
    let cursor = Cursor::new(data);
    let mut options = XlsOptions::default();
    options.force_codepage = Some(1200);
    options.header_row = HeaderRow::Row(skip_rows as u32);
    let mut workbook: calamine::Xls<_> = Xls::new_with_options(cursor, options)?;
    let sheet_names = workbook.sheet_names();
    let sheet_name = resolve_sheet_name(&sheet_names, sheet_name)?;
    let sheet = workbook.worksheet_range(&sheet_name)?;
    sheet_dataframe(sheet, first_row_as_title)
}

fn sheet_dataframe(
    sheet: Range<Data>,
    first_row_as_title: bool,
) -> Result<DataFrame, Box<dyn std::error::Error + Send + Sync>> {
    let mut res = Vec::new();
    for col_index in 0..sheet.width() {
        let column_data: Vec<_> = (0..sheet.height())
            .map(|row_idx| {
                sheet
                    .get((row_idx, col_index)) // Use `get()` instead of `get_value()`.
                    .unwrap_or(&Data::Empty)
                    .clone()
            })
            .collect();
        if !column_data.is_empty() {
            let series = if first_row_as_title {
                let column_name = column_data[0].to_string();
                calamine_value_to_series(&column_name, &column_data[1..])
            } else {
                let column_name = format!("column_{col_index}");
                calamine_value_to_series(&column_name, &column_data)
            };
            res.push(series);
        }
    }
    let df = DataFrame::from_iter(res);
    Ok(df)
}
