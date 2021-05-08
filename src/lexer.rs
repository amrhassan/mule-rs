#[allow(dead_code)]
#[derive(Clone, PartialEq, Eq, Debug)]
struct Token {
    content: String,
    token_type: TokenType,
}

#[allow(dead_code)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TokenType {
    Whitespace,
    Value,
    ValueDelimiter,
    RecordDelimiter,
}

#[allow(dead_code)]
fn parse_whitespace(s: impl AsRef<str>) -> Option<Token> {
    let content: String = s
        .as_ref()
        .chars()
        .take_while(|c| c.is_whitespace())
        .collect();
    if content.is_empty() {
        None
    } else {
        Some(Token {
            content,
            token_type: TokenType::Whitespace,
        })
    }
}

// fn parse_value(s: impl AsRef<str>) -> Option<Token> {
//     let mut chars = s.as_ref().chars().peekable();

// }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_whitespace() {
        let s = " \n\t\r xiao";
        assert_eq!(
            parse_whitespace(s),
            Some(Token {
                content: " \n\t\r ".to_string(),
                token_type: TokenType::Whitespace
            })
        )
    }
}
