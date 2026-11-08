use std::collections::HashMap;

/// Expands a Prague topic template the way Prague (.NET) does: `[v:name]` from
/// `vars`, `[e:NAME]` from the environment. A placeholder with no value is an
/// error, since a half-resolved name silently consumes the wrong topic.
pub fn resolve_topic(template: &str, vars: &HashMap<String, String>) -> Result<String, String> {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    loop {
        let Some(i) = rest.find('[') else {
            out.push_str(rest);
            return Ok(out);
        };
        let Some(j) = rest[i..].find(']') else {
            return Err(format!("prague: topic {template:?}: unclosed placeholder"));
        };
        out.push_str(&rest[..i]);
        let ph = &rest[i + 1..i + j];
        let Some((kind, name)) = ph.split_once(':') else {
            return Err(format!(
                "prague: topic {template:?}: placeholder [{ph}] is not [v:name] or [e:NAME]"
            ));
        };
        let val = match kind {
            "v" => vars.get(name).cloned(),
            "e" => std::env::var(name).ok(),
            _ => {
                return Err(format!(
                    "prague: topic {template:?}: unknown placeholder kind {kind:?}"
                ));
            }
        };
        let Some(val) = val else {
            return Err(format!("prague: topic {template:?}: no value for [{ph}]"));
        };
        out.push_str(&val);
        rest = &rest[i + j + 1..];
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_vars_and_rejects_holes() {
        let vars = HashMap::from([("brandId".to_string(), "7".to_string())]);
        assert_eq!(
            resolve_topic("Cache.[v:brandId].Show", &vars).unwrap(),
            "Cache.7.Show"
        );
        assert_eq!(resolve_topic("plain.topic", &vars).unwrap(), "plain.topic");
        assert!(resolve_topic("Cache.[v:siteId].Show", &vars).is_err());
        assert!(resolve_topic("Cache.[v:brandId.Show", &vars).is_err());
        assert!(resolve_topic("Cache.[x:brandId].Show", &vars).is_err());
        assert!(resolve_topic("Cache.[brandId].Show", &vars).is_err());
    }
}
