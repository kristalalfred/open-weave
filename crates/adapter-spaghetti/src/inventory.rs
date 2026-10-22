use std::collections::{BTreeMap, BTreeSet};

use weave_core::{NodeInput, NodeOutput, RESOURCE_ID_MAX_LEN, Track};
use weave_spaghetti::{AppInfo, SystemDevice};

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Inventory {
    pub inputs: Vec<NodeInput>,
    pub outputs: Vec<NodeOutput>,
    pids: BTreeMap<String, Vec<u32>>,
    devices: BTreeMap<String, String>,
}

impl Inventory {
    #[must_use]
    pub fn detect(apps: &[AppInfo], devices: &[SystemDevice], home: &str) -> Self {
        let roots = [
            "/Applications/".to_string(),
            "/System/Applications/".to_string(),
            format!("{}/Applications/", home.trim_end_matches('/')),
        ];
        let mut apps: Vec<&AppInfo> = apps
            .iter()
            .filter(|app| !app.pids.is_empty())
            .filter(|app| {
                app.bundle_path
                    .as_deref()
                    .is_some_and(|path| roots.iter().any(|root| path.starts_with(root.as_str())))
            })
            .collect();
        apps.sort_by(|a, b| (&a.name, &a.bundle_id).cmp(&(&b.name, &b.bundle_id)));

        let mut inventory = Self::default();
        let mut taken = BTreeSet::new();
        for app in apps {
            let base = slug(&app.name)
                .or_else(|| app.bundle_id.as_deref().and_then(slug))
                .unwrap_or_else(|| "app".to_string());
            let id = unique(&base, &mut taken);
            inventory.inputs.push(NodeInput {
                id: id.clone(),
                label: app.name.clone(),
                tracks: vec![Track::Audio],
            });
            inventory.pids.insert(id, app.pids.clone());
        }

        let mut devices: Vec<&SystemDevice> = devices.iter().filter(|d| d.is_output).collect();
        devices.sort_by(|a, b| (&a.name, &a.uid).cmp(&(&b.name, &b.uid)));
        let mut taken = BTreeSet::new();
        for device in devices {
            let base = slug(&device.name)
                .or_else(|| slug(&device.uid))
                .unwrap_or_else(|| "output".to_string());
            let id = unique(&base, &mut taken);
            inventory.outputs.push(NodeOutput {
                id: id.clone(),
                label: device.name.clone(),
            });
            inventory.devices.insert(id, device.uid.clone());
        }
        inventory
    }

    #[must_use]
    pub fn pids(&self, input: &str) -> Option<&[u32]> {
        self.pids.get(input).map(Vec::as_slice)
    }

    #[must_use]
    pub fn device(&self, output: &str) -> Option<&str> {
        self.devices.get(output).map(String::as_str)
    }
}

fn slug(text: &str) -> Option<String> {
    let mut slug = String::new();
    for c in text.chars().flat_map(char::to_lowercase) {
        if c.is_ascii_alphanumeric() {
            slug.push(c);
        } else if !slug.is_empty() && !slug.ends_with('-') {
            slug.push('-');
        }
    }
    slug.truncate(RESOURCE_ID_MAX_LEN - 4);
    let slug = slug.trim_end_matches('-').to_string();
    (!slug.is_empty()).then_some(slug)
}

fn unique(base: &str, taken: &mut BTreeSet<String>) -> String {
    let id = (1..)
        .map(|n| {
            if n == 1 {
                base.to_string()
            } else {
                format!("{base}-{n}")
            }
        })
        .find(|id| !taken.contains(id))
        .expect("an unused suffix");
    taken.insert(id.clone());
    id
}

#[cfg(test)]
mod tests {
    use super::*;

    fn app(name: &str, bundle_path: Option<&str>, pids: &[u32]) -> AppInfo {
        AppInfo {
            name: name.to_string(),
            bundle_id: bundle_path
                .map(|_| format!("com.example.{}", slug(name).unwrap_or_default())),
            bundle_path: bundle_path.map(str::to_string),
            pids: pids.to_vec(),
        }
    }

    fn device(uid: &str, name: &str, is_output: bool) -> SystemDevice {
        SystemDevice {
            uid: uid.to_string(),
            name: name.to_string(),
            channels: 2,
            is_output,
        }
    }

    #[test]
    fn user_apps_become_audio_inputs_and_output_devices_become_outputs() {
        let inventory = Inventory::detect(
            &[
                app("Music", Some("/System/Applications/Music.app"), &[731]),
                app(
                    "Microsoft Teams",
                    Some("/Applications/Microsoft Teams.app"),
                    &[27240, 27249],
                ),
                app("Rex", Some("/Users/alfred/Applications/Rex.app"), &[6828]),
                app(
                    "ControlCenter",
                    Some("/System/Library/CoreServices/ControlCenter.app"),
                    &[738],
                ),
                app("coreaudiod", None, &[400]),
                app("Quit", Some("/Applications/Quit.app"), &[]),
            ],
            &[
                device("BuiltInSpeakerDevice", "MacBook Pro Speakers", true),
                device("BuiltInMicrophoneDevice", "MacBook Pro Microphone", false),
                device("SpaghettiDevice_UID", "Spaghetti", true),
            ],
            "/Users/alfred",
        );
        let inputs: Vec<(&str, &str)> = inventory
            .inputs
            .iter()
            .map(|input| (input.id.as_str(), input.label.as_str()))
            .collect();
        assert_eq!(
            inputs,
            [
                ("microsoft-teams", "Microsoft Teams"),
                ("music", "Music"),
                ("rex", "Rex")
            ]
        );
        assert!(
            inventory
                .inputs
                .iter()
                .all(|input| input.tracks == [Track::Audio])
        );
        assert_eq!(inventory.pids("microsoft-teams"), Some(&[27240, 27249][..]));
        assert_eq!(inventory.pids("controlcenter"), None);

        let outputs: Vec<&str> = inventory.outputs.iter().map(|o| o.id.as_str()).collect();
        assert_eq!(outputs, ["macbook-pro-speakers", "spaghetti"]);
        assert_eq!(
            inventory.device("macbook-pro-speakers"),
            Some("BuiltInSpeakerDevice")
        );
    }

    #[test]
    fn ids_follow_the_identifier_rule_and_stay_unique() {
        let long = "A".repeat(80);
        let inventory = Inventory::detect(
            &[],
            &[
                device("uid-b", "Studio Display", true),
                device("uid-a", "Studio Display", true),
                device("uid-c", "✦✦✦", true),
                device("uid-d", &long, true),
            ],
            "/Users/alfred",
        );
        for output in &inventory.outputs {
            weave_core::validate_resource_id(&output.id).unwrap();
        }
        let ids: Vec<&str> = inventory.outputs.iter().map(|o| o.id.as_str()).collect();
        assert_eq!(ids[1..3], ["studio-display", "studio-display-2"]);
        assert_eq!(inventory.device("studio-display"), Some("uid-a"));
        assert_eq!(inventory.device("uid-c"), Some("uid-c"));
    }
}
