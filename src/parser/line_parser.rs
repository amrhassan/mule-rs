use derive_more::{Display, From, Into};

/// A CSV value
#[derive(Debug, Clone, Hash, PartialEq, Eq, From, Into, Display)]
pub struct Value(pub String);

impl<'a> From<UnquotedValue<'a>> for Value {
    fn from(v: UnquotedValue<'a>) -> Self {
        Value(v.0.to_string())
    }
}

impl<'a> From<QuotedValue<'a>> for Value {
    fn from(v: QuotedValue<'a>) -> Value {
        let quote_l = v.raw.find(v.quote);
        let quote_r = v.raw.rfind(v.quote);
        match (quote_l, quote_r) {
            (Some(ix_l), Some(ix_r)) if ix_l < ix_r => v.raw[ix_l + v.quote.len()..ix_r]
                .replace(v.quote_escape, "")
                .into(),
            _ => v.raw.to_string().into(),
        }
    }
}

#[derive(From)]
pub struct UnquotedValue<'a>(&'a str);

pub struct QuotedValue<'a> {
    raw: &'a str,
    quote: &'a str,
    quote_escape: &'a str,
}

impl<'a> QuotedValue<'a> {
    fn new(raw: &'a str, quote: &'a str, quote_escape: &'a str) -> QuotedValue<'a> {
        QuotedValue {
            raw,
            quote,
            quote_escape,
        }
    }
}

/// A value parser for a single line implemented as an iterator
pub struct LineParser<'a> {
    line: String,
    separator: &'a str,
    text_quote: &'a str,
    text_quote_escape: &'a str,
    next_start: usize,
}

impl<'a> LineParser<'a> {
    pub fn new(
        line: String,
        separator: &'a str,
        text_quote: &'a str,
        text_quote_escape: &'a str,
    ) -> LineParser<'a> {
        LineParser {
            line,
            separator,
            text_quote,
            text_quote_escape,
            next_start: 0,
        }
    }
}

impl<'a> LineParser<'a> {
    fn remaining(&self) -> &str {
        self.start_from(self.next_start)
    }

    fn start_from(&self, ix: usize) -> &str {
        &self.line[ix..]
    }

    fn next_separator_ix(&self) -> Option<usize> {
        self.remaining()
            .find(&self.separator)
            .map(|ix| ix + self.next_start)
    }

    fn next_quote_ix(&self) -> Option<usize> {
        self.remaining()
            .find(&self.text_quote)
            .map(|ix| ix + self.next_start)
    }

    fn subsequent_qoute_ix(&self, first_quote_ix: usize) -> Option<usize> {
        self.start_from(first_quote_ix + self.text_quote.len())
            .find(&self.text_quote)
            .map(|ix| ix + first_quote_ix + self.text_quote.len())
    }

    fn parse_unquoted(&self) -> (UnquotedValue, usize) {
        let end = self.next_separator_ix().unwrap_or(self.line.len());
        let (raw, n) = self.parse_to(end);
        (raw.into(), n)
    }

    fn parse_quoted(&self) -> Result<(QuotedValue, usize), ()> {
        let quote_l = self.next_quote_ix().ok_or(())?;
        let mut quote_r = self.subsequent_qoute_ix(quote_l).ok_or(())?;

        while &self.line[quote_r - self.text_quote_escape.len()..quote_r] == self.text_quote_escape
        {
            quote_r = self.subsequent_qoute_ix(quote_r).ok_or(())?;
        }

        let end = quote_r + self.text_quote.len();
        let (raw, n) = self.parse_to(end);
        let quoted = QuotedValue::new(raw, self.text_quote, self.text_quote_escape);
        Ok((quoted, n))
    }

    fn parse_to(&self, end: usize) -> (&str, usize) {
        let value = &self.line[self.next_start..end];
        (value, end + self.separator.len())
    }
}

impl<'a> Iterator for LineParser<'a> {
    type Item = Value; // New Strings have to be allocated because escape patterns need to be dropped
    fn next(&mut self) -> Option<Self::Item> {
        if self.next_start > self.line.len() || self.line.is_empty() {
            return None;
        }
        let (value, next_start) = if self.next_separator_ix() < self.next_quote_ix() {
            let (raw, n) = self.parse_unquoted();
            (raw.into(), n)
        } else {
            self.parse_quoted()
                .map(|(raw, n)| (raw.into(), n))
                .unwrap_or_else(|_| {
                    let (raw, n) = self.parse_unquoted();
                    (raw.into(), n)
                })
        };

        self.next_start = next_start;
        Some(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;

    #[test]
    fn test_line_values_1() {
        let line = "first, second,,three,4,,,".to_string();
        let values: Vec<String> = LineParser::new(line, ",", "\"", "\\").map_into().collect();

        assert_eq!(
            values,
            vec!["first", " second", "", "three", "4", "", "", ""]
        )
    }

    #[test]
    fn test_line_values_2() {
        let line = "first, second,,three,4,,,five".to_string();
        let values: Vec<String> = LineParser::new(line, ",", "\"", "\\").map_into().collect();

        assert_eq!(
            values,
            vec!["first", " second", "", "three", "4", "", "", "five"]
        )
    }

    #[test]
    fn test_line_values_3() {
        let line = "first,, second,,,,three,,4,,,,,,".to_string();
        let values: Vec<String> = LineParser::new(line, ",,", "\"", "\\").map_into().collect();

        assert_eq!(
            values,
            vec!["first", " second", "", "three", "4", "", "", ""]
        )
    }

    #[test]
    fn test_line_values_4() {
        let line = "first, second,,three,4,\"\",,five".to_string();
        let values: Vec<String> = LineParser::new(line, ",", "\"", "\\").map_into().collect();

        assert_eq!(
            values,
            vec!["first", " second", "", "three", "4", "", "", "five"]
        )
    }

    #[test]
    fn test_line_values_5() {
        let line = "first, \"second point five\",,three,4,\"\",,five".to_string();
        let values: Vec<String> = LineParser::new(line, ",", "\"", "\\").map_into().collect();

        assert_eq!(
            values,
            vec![
                "first",
                "second point five",
                "",
                "three",
                "4",
                "",
                "",
                "five"
            ]
        )
    }

    #[test]
    fn test_line_values_6() {
        let line = "first, \"second \\\" point five\",,three,4,\"\",,five".to_string();
        let values: Vec<String> = LineParser::new(line, ",", "\"", "\\").map_into().collect();

        assert_eq!(
            values,
            vec![
                "first",
                "second \" point five",
                "",
                "three",
                "4",
                "",
                "",
                "five"
            ]
        )
    }

    #[test]
    fn test_line_values_7() {
        let line = "first, \"second \\\" \\\" point five\",,three,4,\"\",,five".to_string();
        let values: Vec<String> = LineParser::new(line, ",", "\"", "\\").map_into().collect();

        assert_eq!(
            values,
            vec![
                "first",
                "second \" \" point five",
                "",
                "three",
                "4",
                "",
                "",
                "five"
            ]
        )
    }
}
