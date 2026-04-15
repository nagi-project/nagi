/// Escapes double quotes for SQL double-quoted identifiers.
pub(super) fn escape_identifier(s: &str) -> String {
    s.replace('"', "\"\"")
}

/// Escapes single quotes for SQL string literals.
pub(super) fn escape_literal(s: &str) -> String {
    s.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! escape_identifier_test {
        ($($name:ident: $input:expr => $expected:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    assert_eq!(escape_identifier($input), $expected);
                }
            )*
        };
    }

    escape_identifier_test! {
        identifier_plain: "table_name" => "table_name";
        identifier_empty: "" => "";
        identifier_single_quote: "my\"table" => "my\"\"table";
        identifier_consecutive_quotes: "a\"\"b" => "a\"\"\"\"b";
        identifier_only_quote: "\"" => "\"\"";
    }

    macro_rules! escape_literal_test {
        ($($name:ident: $input:expr => $expected:expr;)*) => {
            $(
                #[test]
                fn $name() {
                    assert_eq!(escape_literal($input), $expected);
                }
            )*
        };
    }

    escape_literal_test! {
        literal_plain: "hello" => "hello";
        literal_empty: "" => "";
        literal_single_quote: "it's" => "it''s";
        literal_consecutive_quotes: "a''b" => "a''''b";
        literal_only_quote: "'" => "''";
    }
}
