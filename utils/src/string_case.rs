pub fn to_camel_case(s: &str) -> String {
    let mut result = String::new();
    let mut capitalize_next = false;

    for (i, c) in s.chars().enumerate() {
        if c == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            result.push(c.to_ascii_uppercase());
            capitalize_next = false;
        } else if i == 0 {
            result.push(c.to_ascii_lowercase());
        } else {
            result.push(c);
        }
    }

    result
}
pub fn to_snake_case(s: &str) -> String {
    let mut result = String::new();

    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i != 0 {
                result.push('_');
            }
            result.push(c.to_ascii_lowercase());
        } else {
            result.push(c);
        }
    }

    result
}
pub fn to_kebab_case(s: &str) -> String {
    let mut result = String::new();

    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i != 0 {
                result.push('-');
            }
            result.push(c.to_ascii_lowercase());
        } else {
            result.push(c);
        }
    }

    result
}
pub fn to_pascal_case(s: &str) -> String {
    let mut result = String::new();
    let mut capitalize_next = true;

    for c in s.chars() {
        if c == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            result.push(c.to_ascii_uppercase());
            capitalize_next = false;
        } else {
            result.push(c);
        }
    }

    result
}
pub fn to_title_case(s: &str) -> String {
    let mut result = String::new();
    let mut capitalize_next = true;

    for c in s.chars() {
        if c.is_whitespace() {
            capitalize_next = true;
            result.push(c);
        } else if capitalize_next {
            result.push(c.to_ascii_uppercase());
            capitalize_next = false;
        } else {
            result.push(c);
        }
    }

    result
}
pub fn to_sentence_case(s: &str) -> String {
    let mut result = String::new();
    let mut capitalize_next = true;

    for c in s.chars() {
        if c == '.' || c == '!' || c == '?' {
            capitalize_next = true;
            result.push(c);
        } else if capitalize_next && !c.is_whitespace() {
            result.push(c.to_ascii_uppercase());
            capitalize_next = false;
        } else {
            result.push(c);
        }
    }

    result
}
pub fn to_constant_case(s: &str) -> String {
    let mut result = String::new();

    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i != 0 {
                result.push('_');
            }
            result.push(c);
        } else if c == '_' {
            result.push('_');
        } else {
            result.push(c.to_ascii_uppercase());
        }
    }

    result
}
pub fn to_dot_case(s: &str) -> String {
    let mut result = String::new();

    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i != 0 {
                result.push('.');
            }
            result.push(c.to_ascii_lowercase());
        } else {
            result.push(c);
        }
    }

    result
}


#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_to_camel_case() {
        assert_eq!(to_camel_case("hello_world"), "helloWorld");
        assert_eq!(to_camel_case("this_is_a_test"), "thisIsATest");
        assert_eq!(to_camel_case("alreadyCamelCase"), "alreadyCamelCase");
        assert_eq!(to_camel_case("_leading_underscore"), "LeadingUnderscore");
        assert_eq!(to_camel_case("trailing_underscore_"), "trailingUnderscore");
        assert_eq!(to_camel_case("multiple__underscores"), "multipleUnderscores");
        assert_eq!(to_camel_case(""), "");
    }
    #[test]
    fn test_to_snake_case() {
        assert_eq!(to_snake_case("helloWorld"), "hello_world");
        assert_eq!(to_snake_case("ThisIsATest"), "this_is_a_test");
        assert_eq!(to_snake_case("already_snake_case"), "already_snake_case");
        assert_eq!(to_snake_case("LeadingUnderscore"), "leading_underscore");
        assert_eq!(to_snake_case("trailingUnderscore"), "trailing_underscore");
        assert_eq!(to_snake_case("multipleUpperCaseLetters"), "multiple_upper_case_letters");
        assert_eq!(to_snake_case(""), "");
    }
    #[test]
    fn test_to_kebab_case() {
        assert_eq!(to_kebab_case("helloWorld"), "hello-world");
        assert_eq!(to_kebab_case("ThisIsATest"), "this-is-a-test");
        assert_eq!(to_kebab_case("already-kebab-case"), "already-kebab-case");
        assert_eq!(to_kebab_case("LeadingUnderscore"), "leading-underscore");
        assert_eq!(to_kebab_case("trailingUnderscore"), "trailing-underscore");
        assert_eq!(to_kebab_case("multipleUpperCaseLetters"), "multiple-upper-case-letters");
        assert_eq!(to_kebab_case(""), "");
    }
    #[test]
    fn test_to_pascal_case() {
        assert_eq!(to_pascal_case("hello_world"), "HelloWorld");
        assert_eq!(to_pascal_case("this_is_a_test"), "ThisIsATest");
        assert_eq!(to_pascal_case("alreadyPascalCase"), "AlreadyPascalCase");
        assert_eq!(to_pascal_case("_leading_underscore"), "LeadingUnderscore");
        assert_eq!(to_pascal_case("trailing_underscore_"), "TrailingUnderscore");
        assert_eq!(to_pascal_case("multiple__underscores"), "MultipleUnderscores");
        assert_eq!(to_pascal_case(""), "");
    }
    #[test]
    fn test_to_title_case() {
        assert_eq!(to_title_case("hello world"), "Hello World");
        assert_eq!(to_title_case("this is a test"), "This Is A Test");
        assert_eq!(to_title_case("already Title Case"), "Already Title Case");
        assert_eq!(to_title_case(" leading space"), " Leading Space");
        assert_eq!(to_title_case("trailing space "), "Trailing Space ");
        assert_eq!(to_title_case("multiple   spaces"), "Multiple   Spaces");
        assert_eq!(to_title_case(""), "");
    }
    #[test]
    fn test_to_sentence_case() {
        assert_eq!(to_sentence_case("helloWorld"), "HelloWorld");
        assert_eq!(to_sentence_case("this is a test. another sentence! and a question?"), "This is a test. Another sentence! And a question?");
        assert_eq!(to_sentence_case("already Sentence case."), "Already Sentence case.");
        assert_eq!(to_sentence_case(" leading space."), " Leading space.");
        assert_eq!(to_sentence_case("trailing space. "), "Trailing space. ");
        assert_eq!(to_sentence_case("multiple sentences. here is another! and one more?"), "Multiple sentences. Here is another! And one more?");
        assert_eq!(to_sentence_case(""), "");
    }
    #[test]
    fn test_to_constant_case() {
        assert_eq!(to_constant_case("helloWorld"), "HELLO_WORLD");
        assert_eq!(to_constant_case("ThisIsATest"), "THIS_IS_A_TEST");
        assert_eq!(to_constant_case("already_constant_case"), "ALREADY_CONSTANT_CASE");
        assert_eq!(to_constant_case("LeadingUnderscore"), "LEADING_UNDERSCORE");
        assert_eq!(to_constant_case("trailingUnderscore"), "TRAILING_UNDERSCORE");
        assert_eq!(to_constant_case("multipleUpperCaseLetters"), "MULTIPLE_UPPER_CASE_LETTERS");
        assert_eq!(to_constant_case(""), "");
    }
    #[test]
    fn test_to_dot_case() {
        assert_eq!(to_dot_case("helloWorld"), "hello.world");
        assert_eq!(to_dot_case("ThisIsATest"), "this.is.a.test");
        assert_eq!(to_dot_case("already.dot.case"), "already.dot.case");
        assert_eq!(to_dot_case("LeadingUnderscore"), "leading.underscore");
        assert_eq!(to_dot_case("trailingUnderscore"), "trailing.underscore");
        assert_eq!(to_dot_case("multipleUpperCaseLetters"), "multiple.upper.case.letters");
        assert_eq!(to_dot_case(""), "");
    }
}
