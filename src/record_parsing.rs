use crate::{lexer::Record, value_parsing::RawValue};
use derive_more::From;

#[derive(Clone)]
pub struct RecordParsingOptions {
    pub separator: String,
    pub text_quote: String,
    pub text_quote_escape: String,
}

impl Default for RecordParsingOptions {
    fn default() -> Self {
        RecordParsingOptions {
            separator: ",".to_string(),
            text_quote: "\"".to_string(),
            text_quote_escape: "\\".to_string(),
        }
    }
}

/// An iterator over a record from a CSV file that yields [[RawValue]] instances.
pub struct RecordParser<'a> {
    record: Record,
    options: &'a RecordParsingOptions,
    next_start: usize,
}

impl<'a> RecordParser<'a> {
    pub fn new(record: Record, options: &'a RecordParsingOptions) -> RecordParser<'a> {
        RecordParser {
            record,
            options,
            next_start: 0,
        }
    }
}

impl<'a> RecordParser<'a> {
    fn remaining(&self) -> &str {
        self.start_from(self.next_start)
    }

    fn start_from(&self, ix: usize) -> &str {
        &self.record.as_ref()[ix..]
    }

    fn next_separator_ix(&self) -> Option<usize> {
        self.remaining()
            .find(&self.options.separator)
            .map(|ix| ix + self.next_start)
    }

    fn next_quote_ix(&self) -> Option<usize> {
        self.remaining()
            .find(&self.options.text_quote)
            .map(|ix| ix + self.next_start)
    }

    fn subsequent_qoute_ix(&self, first_quote_ix: usize) -> Option<usize> {
        self.start_from(first_quote_ix + self.options.text_quote.len())
            .find(&self.options.text_quote)
            .map(|ix| ix + first_quote_ix + self.options.text_quote.len())
    }

    fn parse_unquoted(&self) -> (UnquotedRawValue, usize) {
        let end = self
            .next_separator_ix()
            .unwrap_or_else(|| self.record.as_ref().len());
        let (raw, n) = self.parse_to(end);
        (raw.into(), n)
    }

    fn parse_quoted(&self) -> Result<(QuotedRawValue, usize), ()> {
        let quote_l = self.next_quote_ix().ok_or(())?;
        let mut quote_r = self.subsequent_qoute_ix(quote_l).ok_or(())?;

        while self.record.as_ref()[quote_r - self.options.text_quote_escape.len()..quote_r]
            == self.options.text_quote_escape
        {
            quote_r = self.subsequent_qoute_ix(quote_r).ok_or(())?;
        }

        let end = quote_r + self.options.text_quote.len();
        let (raw, n) = self.parse_to(end);
        let quoted = QuotedRawValue::new(raw, &self.options);
        Ok((quoted, n))
    }

    fn parse_to(&self, end: usize) -> (&str, usize) {
        let value = &self.record.as_ref()[self.next_start..end];
        (value, end + self.options.separator.len())
    }
}

impl<'a> Iterator for RecordParser<'a> {
    type Item = RawValue; // New Strings have to be allocated because escape patterns need to be dropped
    fn next(&mut self) -> Option<Self::Item> {
        if self.next_start > self.record.len() || self.record.is_empty() {
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

#[derive(From)]
struct UnquotedRawValue<'a>(&'a str);

impl<'a> From<UnquotedRawValue<'a>> for RawValue {
    fn from(v: UnquotedRawValue<'a>) -> Self {
        RawValue(v.0.to_string())
    }
}

struct QuotedRawValue<'a> {
    raw: &'a str,
    options: &'a RecordParsingOptions,
}

impl<'a> From<QuotedRawValue<'a>> for RawValue {
    fn from(v: QuotedRawValue<'a>) -> RawValue {
        let quote_l = v.raw.find(&v.options.text_quote);
        let quote_r = v.raw.rfind(&v.options.text_quote);
        match (quote_l, quote_r) {
            (Some(ix_l), Some(ix_r)) if ix_l < ix_r => v.raw
                [ix_l + v.options.text_quote.len()..ix_r]
                .replace(&v.options.text_quote_escape, "")
                .into(),
            _ => v.raw.to_string().into(),
        }
    }
}

impl<'a> QuotedRawValue<'a> {
    fn new(raw: &'a str, options: &'a RecordParsingOptions) -> QuotedRawValue<'a> {
        QuotedRawValue { raw, options }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;

    #[test]
    fn test_record_values_1() {
        let record = "first, second,,three,4,,,".into();
        let parsing_options = RecordParsingOptions {
            separator: ",".to_string(),
            text_quote: "\"".to_string(),
            text_quote_escape: "\\".to_string(),
        };
        let values: Vec<String> = RecordParser::new(record, &&parsing_options)
            .map_into()
            .collect();

        assert_eq!(
            values,
            vec!["first", " second", "", "three", "4", "", "", ""]
        )
    }

    #[test]
    fn test_record_values_2() {
        let record = "first, second,,three,4,,,five".into();
        let parsing_options = RecordParsingOptions {
            separator: ",".to_string(),
            text_quote: "\"".to_string(),
            text_quote_escape: "\\".to_string(),
        };
        let values: Vec<String> = RecordParser::new(record, &&parsing_options)
            .map_into()
            .collect();

        assert_eq!(
            values,
            vec!["first", " second", "", "three", "4", "", "", "five"]
        )
    }

    #[test]
    fn test_record_values_3() {
        let record = "first,, second,,,,three,,4,,,,,,".into();
        let parsing_options = RecordParsingOptions {
            separator: ",,".to_string(),
            text_quote: "\"".to_string(),
            text_quote_escape: "\\".to_string(),
        };
        let values: Vec<String> = RecordParser::new(record, &&parsing_options)
            .map_into()
            .collect();

        assert_eq!(
            values,
            vec!["first", " second", "", "three", "4", "", "", ""]
        )
    }

    #[test]
    fn test_record_values_4() {
        let record = "first, second,,three,4,\"\",,five".into();
        let parsing_options = RecordParsingOptions {
            separator: ",".to_string(),
            text_quote: "\"".to_string(),
            text_quote_escape: "\\".to_string(),
        };
        let values: Vec<String> = RecordParser::new(record, &&parsing_options)
            .map_into()
            .collect();

        assert_eq!(
            values,
            vec!["first", " second", "", "three", "4", "", "", "five"]
        )
    }

    #[test]
    fn test_record_values_5() {
        let record = "first, \"second point five\",,three,4,\"\",,five".into();
        let parsing_options = RecordParsingOptions {
            separator: ",".to_string(),
            text_quote: "\"".to_string(),
            text_quote_escape: "\\".to_string(),
        };
        let values: Vec<String> = RecordParser::new(record, &parsing_options)
            .map_into()
            .collect();

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
    fn test_record_values_6() {
        let record = "first, \"second \\\" point five\",,three,4,\"\",,five".into();
        let parsing_options = RecordParsingOptions {
            separator: ",".to_string(),
            text_quote: "\"".to_string(),
            text_quote_escape: "\\".to_string(),
        };
        let values: Vec<String> = RecordParser::new(record, &parsing_options)
            .map_into()
            .collect();

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
    fn test_record_values_7() {
        let record = "first, \"second \\\" \\\" point five\",,three,4,\"\",,five".into();
        let parsing_options = RecordParsingOptions {
            separator: ",".to_string(),
            text_quote: "\"".to_string(),
            text_quote_escape: "\\".to_string(),
        };
        let values: Vec<String> = RecordParser::new(record, &parsing_options)
            .map_into()
            .collect();

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
