//! Unit tests for common-config crate

use common_config::{ExecutionConfig, ExecutorType, GrismConfig, StorageConfig};
use serde_json;

#[test]
fn test_grism_config_default() {
    let config = GrismConfig::default();

    // Check default execution config
    assert_eq!(config.execution.default_executor, ExecutorType::Local);
    assert_eq!(config.execution.parallelism, None);
    assert_eq!(config.execution.memory_limit, None);

    // Check default storage config
    assert_eq!(config.storage.base_path, None);
    assert!(config.storage.snapshot_isolation);
}

#[test]
fn test_execution_config_default() {
    let config = ExecutionConfig::default();

    assert_eq!(config.default_executor, ExecutorType::Local);
    assert_eq!(config.parallelism, None);
    assert_eq!(config.memory_limit, None);
}

#[test]
fn test_storage_config_default() {
    let config = StorageConfig::default();

    assert_eq!(config.base_path, None);
    assert!(config.snapshot_isolation);
}

#[test]
fn test_executor_type_equality() {
    assert_eq!(ExecutorType::Local, ExecutorType::Local);
    assert_eq!(ExecutorType::Ray, ExecutorType::Ray);
    assert_ne!(ExecutorType::Local, ExecutorType::Ray);
}

#[test]
fn test_executor_type_default() {
    assert_eq!(ExecutorType::default(), ExecutorType::Local);
}

#[test]
fn test_grism_config_serialization() {
    let mut config = GrismConfig::default();
    config.execution.parallelism = Some(4);
    config.execution.memory_limit = Some(1024 * 1024 * 1024); // 1GB
    config.storage.base_path = Some("/data/grism".to_string());
    config.storage.snapshot_isolation = false;

    // Serialize to JSON
    let json = serde_json::to_string(&config).unwrap();

    // Deserialize from JSON
    let deserialized: GrismConfig = serde_json::from_str(&json).unwrap();

    // Verify equality
    assert_eq!(deserialized.execution.default_executor, ExecutorType::Local);
    assert_eq!(deserialized.execution.parallelism, Some(4));
    assert_eq!(
        deserialized.execution.memory_limit,
        Some(1024 * 1024 * 1024)
    );
    assert_eq!(
        deserialized.storage.base_path,
        Some("/data/grism".to_string())
    );
    assert!(!deserialized.storage.snapshot_isolation);
}

#[test]
fn test_execution_config_serialization() {
    let config = ExecutionConfig {
        default_executor: ExecutorType::Ray,
        parallelism: Some(8),
        memory_limit: Some(2 * 1024 * 1024 * 1024), // 2GB
    };

    // Serialize to JSON
    let json = serde_json::to_string(&config).unwrap();
    assert!(json.contains("Ray"));
    assert!(json.contains("8"));
    assert!(json.contains("2147483648"));

    // Deserialize from JSON
    let deserialized: ExecutionConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.default_executor, ExecutorType::Ray);
    assert_eq!(deserialized.parallelism, Some(8));
    assert_eq!(deserialized.memory_limit, Some(2 * 1024 * 1024 * 1024));
}

#[test]
fn test_storage_config_serialization() {
    let config = StorageConfig {
        base_path: Some("/custom/path".to_string()),
        snapshot_isolation: false,
    };

    // Serialize to JSON
    let json = serde_json::to_string(&config).unwrap();
    assert!(json.contains("/custom/path"));
    assert!(json.contains("false"));

    // Deserialize from JSON
    let deserialized: StorageConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.base_path, Some("/custom/path".to_string()));
    assert!(!deserialized.snapshot_isolation);
}

#[test]
fn test_executor_type_serialization() {
    // Test Local
    let local_json = serde_json::to_string(&ExecutorType::Local).unwrap();
    let local: ExecutorType = serde_json::from_str(&local_json).unwrap();
    assert_eq!(local, ExecutorType::Local);

    // Test Ray
    let ray_json = serde_json::to_string(&ExecutorType::Ray).unwrap();
    let ray: ExecutorType = serde_json::from_str(&ray_json).unwrap();
    assert_eq!(ray, ExecutorType::Ray);
}

#[test]
fn test_config_debug_format() {
    let config = GrismConfig::default();
    let debug_str = format!("{:?}", config);
    assert!(debug_str.contains("GrismConfig"));
    assert!(debug_str.contains("ExecutionConfig"));
    assert!(debug_str.contains("StorageConfig"));
}

#[test]
fn test_execution_config_debug_format() {
    let config = ExecutionConfig {
        default_executor: ExecutorType::Ray,
        parallelism: Some(16),
        memory_limit: Some(4096),
    };
    let debug_str = format!("{:?}", config);
    assert!(debug_str.contains("Ray"));
    assert!(debug_str.contains("16"));
    assert!(debug_str.contains("4096"));
}

#[test]
fn test_storage_config_debug_format() {
    let config = StorageConfig {
        base_path: Some("/test".to_string()),
        snapshot_isolation: true,
    };
    let debug_str = format!("{:?}", config);
    assert!(debug_str.contains("/test"));
    assert!(debug_str.contains("true"));
}

#[test]
fn test_grism_config_clone() {
    let mut config = GrismConfig::default();
    config.execution.parallelism = Some(2);
    config.storage.base_path = Some("path".to_string());

    let cloned = config.clone();
    assert_eq!(cloned.execution.parallelism, config.execution.parallelism);
    assert_eq!(cloned.storage.base_path, config.storage.base_path);
}

#[test]
fn test_execution_config_clone() {
    let config = ExecutionConfig {
        default_executor: ExecutorType::Ray,
        parallelism: Some(32),
        memory_limit: Some(8192),
    };

    let cloned = config.clone();
    assert_eq!(cloned.default_executor, config.default_executor);
    assert_eq!(cloned.parallelism, config.parallelism);
    assert_eq!(cloned.memory_limit, config.memory_limit);
}

#[test]
fn test_storage_config_clone() {
    let config = StorageConfig {
        base_path: Some("test_path".to_string()),
        snapshot_isolation: false,
    };

    let cloned = config.clone();
    assert_eq!(cloned.base_path, config.base_path);
    assert_eq!(cloned.snapshot_isolation, config.snapshot_isolation);
}

#[test]
fn test_config_partial_json() {
    // Test partial JSON with missing fields
    let json = r#"{
        "execution": {
            "default_executor": "Ray"
        },
        "storage": {}
    }"#;

    let config: GrismConfig = serde_json::from_str(json).unwrap();
    assert_eq!(config.execution.default_executor, ExecutorType::Ray);
    // Missing fields should use defaults
    assert_eq!(config.execution.parallelism, None);
    assert_eq!(config.execution.memory_limit, None);
    assert_eq!(config.storage.base_path, None);
    assert!(config.storage.snapshot_isolation);
}

#[test]
fn test_config_with_null_values() {
    // Test JSON with explicit null values
    let json = r#"{
        "execution": {
            "default_executor": "Local",
            "parallelism": null,
            "memory_limit": null
        },
        "storage": {
            "base_path": null,
            "snapshot_isolation": false
        }
    }"#;

    let config: GrismConfig = serde_json::from_str(json).unwrap();
    assert_eq!(config.execution.default_executor, ExecutorType::Local);
    assert_eq!(config.execution.parallelism, None);
    assert_eq!(config.execution.memory_limit, None);
    assert_eq!(config.storage.base_path, None);
    assert!(!config.storage.snapshot_isolation);
}

#[test]
fn test_config_toml_serialization() {
    let config = GrismConfig::default();

    // Serialize to TOML
    let toml_str = toml::to_string_pretty(&config).unwrap();
    assert!(toml_str.contains("[execution]"));
    assert!(toml_str.contains("[storage]"));
    assert!(toml_str.contains("default_executor = \"Local\""));
    assert!(toml_str.contains("snapshot_isolation = true"));

    // Deserialize from TOML
    let deserialized: GrismConfig = toml::from_str(&toml_str).unwrap();
    assert_eq!(deserialized.execution.default_executor, ExecutorType::Local);
    assert!(deserialized.storage.snapshot_isolation);
}

#[test]
fn test_config_yaml_serialization() {
    let config = GrismConfig::default();

    // Serialize to YAML
    let yaml_str = serde_yaml::to_string(&config).unwrap();
    assert!(yaml_str.contains("execution:"));
    assert!(yaml_str.contains("storage:"));
    assert!(yaml_str.contains("default_executor: Local"));

    // Deserialize from YAML
    let deserialized: GrismConfig = serde_yaml::from_str(&yaml_str).unwrap();
    assert_eq!(deserialized.execution.default_executor, ExecutorType::Local);
    assert!(deserialized.storage.snapshot_isolation);
}

#[test]
fn test_invalid_executor_type_deserialization() {
    // Test with invalid executor type
    let json = r#"{
        "execution": {
            "default_executor": "InvalidType"
        }
    }"#;

    let result: Result<GrismConfig, _> = serde_json::from_str(json);
    assert!(result.is_err());
}

#[test]
fn test_invalid_memory_limit_deserialization() {
    // Test with invalid memory limit (negative value)
    let json = r#"{
        "execution": {
            "default_executor": "Local",
            "memory_limit": -100
        }
    }"#;

    // Should fail because usize cannot be negative
    let result: Result<GrismConfig, _> = serde_json::from_str(json);
    assert!(result.is_err());
}

#[test]
fn test_config_builder_pattern() {
    // Simulate a builder pattern using struct updates
    let _base_config = GrismConfig::default();

    let custom_config = GrismConfig {
        execution: ExecutionConfig {
            default_executor: ExecutorType::Ray,
            parallelism: Some(12),
            memory_limit: Some(4 * 1024 * 1024 * 1024),
        },
        storage: StorageConfig {
            base_path: Some("/data/grism".to_string()),
            snapshot_isolation: false,
        },
    };

    assert_eq!(custom_config.execution.default_executor, ExecutorType::Ray);
    assert_eq!(custom_config.execution.parallelism, Some(12));
    assert_eq!(
        custom_config.execution.memory_limit,
        Some(4 * 1024 * 1024 * 1024)
    );
    assert_eq!(
        custom_config.storage.base_path,
        Some("/data/grism".to_string())
    );
    assert!(!custom_config.storage.snapshot_isolation);
}

#[test]
fn test_config_merge() {
    let base_config = GrismConfig::default();

    // Create a modified version by merging
    let mut merged_config = base_config.clone();
    merged_config.execution.parallelism = Some(8);
    merged_config.storage.base_path = Some("/new/path".to_string());

    // Original should be unchanged
    assert_eq!(base_config.execution.parallelism, None);
    assert_eq!(base_config.storage.base_path, None);

    // Merged config should have changes
    assert_eq!(merged_config.execution.parallelism, Some(8));
    assert_eq!(
        merged_config.storage.base_path,
        Some("/new/path".to_string())
    );
}
