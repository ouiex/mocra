use crate::type_convert::calamine_value_to_series;
use calamine::{Data, HeaderRow, Range, Reader, Xls, XlsOptions, Xlsx};
use polars::prelude::*;
use std::io::Cursor;

pub fn xlsx_dataframe(
    data: &[u8],
    sheet_name: &str,
    skip_rows: usize,
    first_row_as_title: bool,
) -> Result<DataFrame, Box<dyn std::error::Error + Send + Sync>> {
    let cursor = Cursor::new(data);

    let mut workbook: Xlsx<_> = calamine::open_workbook_from_rs(cursor)?;
    let sheet_names = workbook.sheet_names();
    if !sheet_names.contains(&sheet_name.to_string()) {
        return Err(format!("Sheet '{sheet_name}' not found in the workbook").into());
    }
    let sheet = workbook.with_header_row(HeaderRow::Row(skip_rows as u32)).worksheet_range(sheet_name)?;
    sheet_dataframe(sheet, first_row_as_title)
}

pub fn xls_dataframe(
    data: &[u8],
    sheet_name: &str,
    skip_rows: usize,
    first_row_as_title: bool,
) -> Result<DataFrame, Box<dyn std::error::Error + Send + Sync>> {
    let cursor = Cursor::new(data);
    let mut options = XlsOptions::default();
    options.force_codepage = Some(1200);
    options.header_row = HeaderRow::Row(skip_rows as u32);
    let mut workbook: calamine::Xls<_> = Xls::new_with_options(cursor, options)?;
    let sheet_names = workbook.sheet_names();
    if !sheet_names.contains(&sheet_name.to_string()) {
        return Err(format!("Sheet '{sheet_name}' not found in the workbook").into());
    }
    let sheet = workbook.worksheet_range(sheet_name)?;
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
                    .get((row_idx, col_index))  // 使用 get() 而不是 get_value()
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
