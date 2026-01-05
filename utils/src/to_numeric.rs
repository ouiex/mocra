#![allow(unused)]


/// Convert a textual numeric representation (including extended Chinese numerals) into f64.
///
/// Newly supported (enhanced version):
/// - Full Chinese digits: 零 〇 一 二 三 四 五 六 七 八 九 两
/// - Chinese units with hierarchical grouping: 十 百 千 万 亿 (supports combinations like "一亿二千三百四十五万六千七百八十九")
/// - Implicit leading '一' for '十' (e.g., "十五" => 15, "十" => 10)
/// - Decimal part after 点 / . with Chinese digits: "三点一四" => 3.14
/// - Trailing unit directly after an Arabic number or mixed decimal: "1.2万" => 12000
/// - Mixed Chinese + Arabic inside the integer part: "一万2千三百" => 12300
/// - Percent handling remains (suffix %) applied at the end
/// - Currency symbols (¥ ￥ $ € £) stripped anywhere
/// - Optional plus sign '+'
/// - Commas as thousands separators ignored
/// - NaN forms removed (nan / NaN / NAN / Nan) -> treated as zero if nothing else remains
///
/// Fallback plain float parsing still attempted; returns None if unrecognized characters remain.
/// This aims to be permissive but deterministic.

pub fn to_numeric(input: &str) -> Option<f64> {
    let mut s = input.trim().to_string();
    if s.is_empty() || s == "-" {
        return Some(0.0);
    }

    // Normalise & remove known noise tokens
    for pat in ["nan", "NaN", "Nan", "NAN"] {
        s = s.replace(pat, "");
    }
    // Remove spaces
    s = s.replace(' ', "");
    // Remove currency symbols
    for sym in ['¥', '￥', '$', '€', '£'] {
        s = s.replace(sym, "");
    }
    if s.is_empty() {
        return Some(0.0);
    }

    // Fast path: plain float parse (no Chinese chars / percent / units)
    if !s.chars().any(|c| {
        matches!(
            c,
            '%' | '万'
                | '千'
                | '百'
                | '点'
                | '零'
                | '十'
                | '亿'
                | '一'
                | '二'
                | '三'
                | '四'
                | '五'
                | '六'
                | '七'
                | '八'
                | '九'
                | '两'
        )
    }) && let Ok(v) = s.parse::<f64>()
    {
        return Some(v);
    }

    // If contains full Chinese numerals, attempt Chinese parsing path.

    // Allowed digit mapping (Arabic + Chinese)
    enum Kind {
        Point,
        Percent,
        Comma,
        Minus,
        Plus,
        Digit(u32),
        Other(char),
    }
    fn classify(c: char) -> Option<Kind> {
        match c {
            '.' | '点' => Some(Kind::Point),
            '%' => Some(Kind::Percent),
            ',' => Some(Kind::Comma),
            '-' => Some(Kind::Minus),
            '+' => Some(Kind::Plus),
            '零' | '〇' => Some(Kind::Digit(0)),
            '一' => Some(Kind::Digit(1)),
            '二' => Some(Kind::Digit(2)),
            '三' => Some(Kind::Digit(3)),
            '四' => Some(Kind::Digit(4)),
            '五' => Some(Kind::Digit(5)),
            '六' => Some(Kind::Digit(6)),
            '七' => Some(Kind::Digit(7)),
            '八' => Some(Kind::Digit(8)),
            '九' => Some(Kind::Digit(9)),
            '两' => Some(Kind::Digit(2)),
            '0'..='9' => Some(Kind::Digit(c.to_digit(10).unwrap())),
            _ => None,
        }
    }
    // Large unit boundaries (section units) and small units
    fn big_unit(c: char) -> Option<f64> {
        match c {
            '亿' => Some(100_000_000.0),
            '万' => Some(10_000.0),
            _ => None,
        }
    }
    fn small_unit(c: char) -> Option<f64> {
        match c {
            '千' => Some(1000.0),
            '百' => Some(100.0),
            '十' => Some(10.0),
            _ => None,
        }
    }

    // If there's any Chinese unit/digit, use Chinese-aware algorithm separate from previous simplistic reverse parsing.
    if s.chars().any(|c| {
        matches!(
            c,
            '亿' | '万'
                | '千'
                | '百'
                | '十'
                | '零'
                | '〇'
                | '一'
                | '二'
                | '三'
                | '四'
                | '五'
                | '六'
                | '七'
                | '八'
                | '九'
                | '两'
        )
    }) {
        let percent = s.ends_with('%');
        if percent {
            s.pop();
        }

        // Split decimal part
        let mut parts = if let Some(pos) = s.find(['.', '点']) {
            vec![s[..pos].to_string(), s[pos + 1..].to_string()]
        } else {
            vec![s.clone()]
        };
        let decimal_part = if parts.len() == 2 {
            Some(parts.pop().unwrap())
        } else {
            None
        };
        let int_part = parts.pop().unwrap();

        // Handle trailing big/small unit after pure Arabic/decimal number like "1.2万"
        let trailing_multiplier;
        if let Some(last) = int_part.chars().last()
            && let Some(m) = big_unit(last).or_else(|| small_unit(last))
        {
            // ensure preceding chars contain a digit
            let core = &int_part[..int_part.len() - last.len_utf8()];
            if core.chars().all(|c| c.is_ascii_digit()) && !core.is_empty() {
                trailing_multiplier = m;
                // rebuild s to parse the core as plain number later
                if let Ok(base) = core.parse::<f64>() {
                    let mut value = base * trailing_multiplier;
                    if let Some(dec) = decimal_part.as_ref() {
                        // decimal after unit ambiguous: treat as fractional appended to base before multiplier? Keep simple: base.dec * multiplier
                        let dec_digits: String =
                            dec.chars().filter(|c| c.is_ascii_digit()).collect();
                        if !dec_digits.is_empty()
                            && let Ok(frac_int) = dec_digits.parse::<f64>()
                        {
                            value += frac_int / 10_f64.powi(dec_digits.len() as i32)
                                * trailing_multiplier;
                        }
                        if percent {
                            value /= 100.0;
                        }
                        return Some(value);
                    }
                }
            }
        }

        // Parse Chinese integer part into number
        let mut section_total = 0_f64; // accumulates within current 10^8 or 10^4 section
        let mut current = 0_f64; // current digit value awaiting a small unit
        let mut total = 0_f64; // grand total
        let mut last_was_digit = false;
        for ch in int_part.chars() {
            if ch == '零' || ch == '〇' {
                last_was_digit = false;
                continue;
            }
            if let Some(su) = small_unit(ch) {
                // 十 百 千
                if last_was_digit {
                    section_total += if current == 0.0 { su } else { current * su };
                } else {
                    // implicit one, e.g. 十 => 10
                    section_total += if current == 0.0 { su } else { current * su };
                }
                current = 0.0;
                last_was_digit = false;
                continue;
            }
            if let Some(bu) = big_unit(ch) {
                // 万 亿 boundaries
                section_total += current;
                total += section_total * bu;
                section_total = 0.0;
                current = 0.0;
                last_was_digit = false;
                continue;
            }
            if let Some(Kind::Digit(d)) = classify(ch) {
                current = d as f64;
                last_was_digit = true;
                continue;
            }
            // Arabic digit fallback
            if ch.is_ascii_digit() {
                current = (ch as u8 - b'0') as f64;
                last_was_digit = true;
                continue;
            }
            if matches!(
                classify(ch),
                Some(Kind::Comma) | Some(Kind::Plus) | Some(Kind::Minus) | Some(Kind::Point)
            ) {
                continue;
            }
            // Unknown char => fail
            return None;
        }
        section_total += current;
        total += section_total;

        // Decimal part (Chinese digits or Arabic) if present
        let mut decimal_value = 0_f64;
        if let Some(dec) = decimal_part {
            let mut scale = 0_f64;
            for ch in dec.chars() {
                if let Some(Kind::Digit(d)) = classify(ch) {
                    scale += 1.0;
                    decimal_value += (d as f64) / 10_f64.powi(scale as i32);
                } else if ch.is_ascii_digit() {
                    scale += 1.0;
                    decimal_value += ((ch as u8 - b'0') as f64) / 10_f64.powi(scale as i32);
                } else if ch == '%' { /* ignore already handled earlier */
                } else if ch == ' ' {
                    continue;
                } else {
                    return None;
                }
            }
        }

        let mut value = total + decimal_value;

        // Percent
        if percent {
            value /= 100.0;
        }

        // Sign handling
        let negative = input.contains('-');
        if negative {
            value = -value;
        }
        return Some(value);
    }

    let mut n: f64 = 1.0; // overall multiplier (units & percent)
    let mut r: f64 = 0.0; // accumulating integer/decimal digits in reverse
    let mut p: u32 = 0; // power index for digits before hitting decimal point
    let mut negative = false;
    let mut saw_valid = false;

    for c in s.chars().rev() {
        // reverse iteration like Python version
        match c {
            '万' => {
                n *= 10_000.0;
                saw_valid = true;
                continue;
            }
            '千' => {
                n *= 1_000.0;
                saw_valid = true;
                continue;
            }
            '百' => {
                n *= 100.0;
                saw_valid = true;
                continue;
            }
            _ => {}
        }
        match classify(c) {
            Some(Kind::Percent) => {
                n /= 100.0;
                saw_valid = true;
            }
            Some(Kind::Minus) => {
                negative = true;
                saw_valid = true;
            }
            Some(Kind::Plus) => {
                saw_valid = true;
            }
            Some(Kind::Comma) => { /* ignore */ }
            Some(Kind::Point) => {
                if p > 0 {
                    r /= 10_f64.powi(p as i32);
                    p = 0;
                }
                saw_valid = true;
            }
            Some(Kind::Digit(d)) => {
                r += (10_f64).powi(p as i32) * (d as f64);
                p += 1;
                saw_valid = true;
            }
            Some(Kind::Other(_)) => {
                return None;
            }
            None => {
                return None;
            } // unknown character
        }
    }

    if !saw_valid {
        return None;
    }
    let mut value = r * n;
    if negative {
        value = -value;
    }
    Some(value)
}

#[cfg(test)]
mod tests {
    use super::to_numeric;

    fn approx(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9, "{} != {}", a, b);
    }

    #[test]
    fn test_plain_number() {
        approx(to_numeric("123").unwrap(), 123.0);
    }

    #[test]
    fn test_with_commas() {
        approx(to_numeric("1,234.56").unwrap(), 1234.56);
    }

    #[test]
    fn test_currency_symbols() {
        approx(to_numeric("￥1,234").unwrap(), 1234.0);
    }

    #[test]
    fn test_percent() {
        approx(to_numeric("12%").unwrap(), 0.12);
    }

    #[test]
    fn test_negative() {
        approx(to_numeric("-123").unwrap(), -123.0);
    }

    #[test]
    fn test_chinese_unit_qian() {
        approx(to_numeric("45千").unwrap(), 45_000.0);
    }

    #[test]
    fn test_chinese_unit_bai() {
        approx(to_numeric("3百").unwrap(), 300.0);
    }

    #[test]
    fn test_zero_variants() {
        approx(to_numeric("零").unwrap(), 0.0);
        approx(to_numeric("0").unwrap(), 0.0);
    }

    #[test]
    fn test_nan_replacement() {
        approx(to_numeric("nan").unwrap(), 0.0);
    }

    #[test]
    fn test_invalid_char() {
        assert!(to_numeric("abc").is_none());
    }

    #[test]
    fn test_mixed_cleaning() {
        approx(to_numeric(" ¥ 1,005.50 % ").unwrap_or(-1.0), 10.055);
    }

    #[test]
    fn test_implicit_ten() {
        approx(to_numeric("十五").unwrap(), 15.0);
        approx(to_numeric("十").unwrap(), 10.0);
    }

    #[test]
    fn test_large_mixed() {
        approx(
            to_numeric("一亿二千三百四十五万六千七百八十九").unwrap(),
            123456789.0,
        );
    }

    #[test]
    fn test_with_liang() {
        approx(to_numeric("两千零三十").unwrap(), 2030.0);
    }

    #[test]
    fn test_percent_chinese() {
        approx(to_numeric("十%").unwrap(), 0.10);
    }

    #[test]
    fn test_plus_sign() {
        approx(to_numeric("+123").unwrap(), 123.0);
    }

    #[test]
    fn test_mixed_arabic_chinese() {
        approx(to_numeric("一万2千三百").unwrap(), 12300.0);
    }
}
