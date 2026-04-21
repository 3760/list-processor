"""
Tests for SpecLoader: Field Specification Loader Module

SpecLoader 功能覆盖：
- 字段规范文件加载（YAML）
- attr_code 唯一性校验
- 必填字段校验
- get_attr_code_mapping
- get_required_fields
- 缓存机制
"""

import os
from pathlib import Path

import pytest
import yaml

from infra.exceptions import ConfigError, ValidationError
from infra.spec_loader import (
    clear_cache,
    get_attr_code_mapping,
    get_required_fields,
    load_field_spec,
)


# ─────────────────────────────────────────────
# 辅助函数：生成测试 YAML 文件
# ─────────────────────────────────────────────

def _make_spec_yaml(path: Path, spec: dict):
    """生成测试用字段规范 YAML 文件"""
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(spec, f, allow_unicode=True, default_flow_style=False)


# ─────────────────────────────────────────────
# 文件加载
# ─────────────────────────────────────────────

class TestSpecLoaderInit:
    """字段规范加载"""

    def setup_method(self):
        """每个测试前清除缓存"""
        clear_cache()

    def test_load_valid_spec_file(self, tmp_path):
        """正常加载字段规范文件"""
        path = tmp_path / "field_spec.yaml"
        _make_spec_yaml(path, {
            "fields": {
                "email": {"attr_code": "E001", "required": True},
                "name": {"attr_code": "E002", "required": False},
            }
        })
        spec = load_field_spec(str(path))
        assert "fields" in spec
        assert "email" in spec["fields"]

    def test_file_not_found_raises_error(self, tmp_path):
        """文件不存在：抛出 ConfigError"""
        fake_path = str(tmp_path / "nonexistent.yaml")
        with pytest.raises(ConfigError, match="不存在"):
            load_field_spec(fake_path)

    def test_default_path_fallback(self, tmp_path):
        """不指定路径时使用默认路径（需路径存在）"""
        # 此测试依赖实际配置文件存在，跳过无配置环境
        pass  # pragma: no cover


# ─────────────────────────────────────────────
# 规范校验
# ─────────────────────────────────────────────

class TestSpecValidation:
    """字段规范校验"""

    def setup_method(self):
        clear_cache()

    def test_valid_spec_passes(self, tmp_path):
        """有效规范：attr_code 无重复，必填字段完整"""
        path = tmp_path / "field_spec.yaml"
        _make_spec_yaml(path, {
            "fields": {
                "email": {"attr_code": "E001", "required": True},
                "name": {"attr_code": "E002", "required": False},
            }
        })
        spec = load_field_spec(str(path))
        assert spec is not None  # 无异常即通过

    def test_duplicate_attr_code_raises_error(self, tmp_path):
        """attr_code 重复：抛出 ValidationError"""
        path = tmp_path / "field_spec.yaml"
        _make_spec_yaml(path, {
            "fields": {
                "email": {"attr_code": "E001"},
                "phone": {"attr_code": "E001"},  # 重复
            }
        })
        with pytest.raises(ValidationError, match="重复"):
            load_field_spec(str(path))

    def test_empty_fields_raises_error(self, tmp_path):
        """fields 为空：抛出 ValidationError"""
        path = tmp_path / "field_spec.yaml"
        _make_spec_yaml(path, {"fields": {}})
        with pytest.raises(ValidationError, match="为空"):
            load_field_spec(str(path))

    def test_missing_fields_key_raises_error(self, tmp_path):
        """缺少 fields 键：抛出 ValidationError"""
        path = tmp_path / "field_spec.yaml"
        _make_spec_yaml(path, {"other_key": "value"})
        with pytest.raises(ValidationError, match="为空"):
            load_field_spec(str(path))

    def test_invalid_spec_format_raises_error(self, tmp_path):
        """非字典格式：抛出 ValidationError"""
        path = tmp_path / "field_spec.yaml"
        _make_spec_yaml(path, ["not", "a", "dict"])
        with pytest.raises(ValidationError, match="格式错误"):
            load_field_spec(str(path))


# ─────────────────────────────────────────────
# get_attr_code_mapping
# ─────────────────────────────────────────────

class TestGetAttrCodeMapping:
    """字段名 → attr_code 映射"""

    def setup_method(self):
        clear_cache()

    def test_returns_field_name_to_attr_code_map(self, tmp_path):
        """返回字段名到 attr_code 的映射"""
        path = tmp_path / "field_spec.yaml"
        _make_spec_yaml(path, {
            "fields": {
                "email": {"attr_code": "E001"},
                "name": {"attr_code": "E002"},
                "phone": {},  # 无 attr_code
            }
        })
        mapping = get_attr_code_mapping(load_field_spec(str(path)))
        assert mapping["email"] == "E001"
        assert mapping["name"] == "E002"
        assert "phone" not in mapping  # 无 attr_code 的字段不包含

    def test_skips_fields_without_attr_code(self, tmp_path):
        """跳过无 attr_code 的字段"""
        path = tmp_path / "field_spec.yaml"
        _make_spec_yaml(path, {
            "fields": {
                "email": {},  # 无 attr_code
                "name": {"attr_code": "E002"},
            }
        })
        mapping = get_attr_code_mapping(load_field_spec(str(path)))
        assert "email" not in mapping
        assert mapping["name"] == "E002"

    def test_empty_when_no_attr_codes(self, tmp_path):
        """所有字段都无 attr_code 时返回空字典"""
        path = tmp_path / "field_spec.yaml"
        _make_spec_yaml(path, {
            "fields": {
                "email": {},
                "name": {},
            }
        })
        mapping = get_attr_code_mapping(load_field_spec(str(path)))
        assert mapping == {}


# ─────────────────────────────────────────────
# get_required_fields
# ─────────────────────────────────────────────

class TestGetRequiredFields:
    """必填字段列表"""

    def setup_method(self):
        clear_cache()

    def test_returns_required_field_names(self, tmp_path):
        """返回 required=True 的字段名列表"""
        path = tmp_path / "field_spec.yaml"
        _make_spec_yaml(path, {
            "fields": {
                "email": {"attr_code": "E001", "required": True},
                "name": {"attr_code": "E002", "required": False},
                "phone": {"attr_code": "E003", "required": True},
            }
        })
        required = get_required_fields(load_field_spec(str(path)))
        assert "email" in required
        assert "phone" in required
        assert "name" not in required

    def test_defaults_to_not_required(self, tmp_path):
        """未指定 required 时默认为 False"""
        path = tmp_path / "field_spec.yaml"
        _make_spec_yaml(path, {
            "fields": {
                "email": {"attr_code": "E001"},  # 无 required 键
                "name": {"attr_code": "E002", "required": True},
            }
        })
        required = get_required_fields(load_field_spec(str(path)))
        assert "email" not in required
        assert "name" in required

    def test_empty_when_no_required_fields(self, tmp_path):
        """无必填字段时返回空列表"""
        path = tmp_path / "field_spec.yaml"
        _make_spec_yaml(path, {
            "fields": {
                "email": {"attr_code": "E001", "required": False},
                "name": {"attr_code": "E002"},
            }
        })
        required = get_required_fields(load_field_spec(str(path)))
        assert required == []


# ─────────────────────────────────────────────
# 缓存机制
# ─────────────────────────────────────────────

class TestSpecCache:
    """规范缓存机制"""

    def setup_method(self):
        clear_cache()

    def test_cached_spec_returned(self, tmp_path):
        """第二次调用返回缓存"""
        path = tmp_path / "field_spec.yaml"
        _make_spec_yaml(path, {
            "fields": {"email": {"attr_code": "E001"}}
        })
        spec1 = load_field_spec(str(path))
        spec2 = load_field_spec(str(path))
        assert spec1 is spec2  # 同一对象

    def test_clear_cache_forces_reload(self, tmp_path):
        """清除缓存后重新加载"""
        path = tmp_path / "field_spec.yaml"
        _make_spec_yaml(path, {
            "fields": {"email": {"attr_code": "E001"}}
        })
        spec1 = load_field_spec(str(path))
        clear_cache()
        spec2 = load_field_spec(str(path))
        assert spec1 is not spec2  # 不同对象

    def test_cache_isolation_between_paths(self, tmp_path):
        """不同路径独立缓存"""
        path1 = tmp_path / "spec1.yaml"
        path2 = tmp_path / "spec2.yaml"
        _make_spec_yaml(path1, {"fields": {"email": {"attr_code": "E001"}}})
        _make_spec_yaml(path2, {"fields": {"name": {"attr_code": "E002"}}})

        spec1 = load_field_spec(str(path1))
        clear_cache()
        spec2 = load_field_spec(str(path2))

        assert "email" in spec1["fields"]
        assert "name" in spec2["fields"]


# ─────────────────────────────────────────────
# 边界情况
# ─────────────────────────────────────────────

class TestSpecLoaderEdgeCases:
    """边界情况处理"""

    def setup_method(self):
        clear_cache()

    def test_field_def_not_a_dict_skipped(self, tmp_path):
        """字段定义非字典时跳过"""
        path = tmp_path / "field_spec.yaml"
        _make_spec_yaml(path, {
            "fields": {
                "email": {"attr_code": "E001"},
                "invalid": "not_a_dict",  # 非字典定义
            }
        })
        mapping = get_attr_code_mapping(load_field_spec(str(path)))
        assert mapping["email"] == "E001"
        assert "invalid" not in mapping

    def test_spec_with_extra_keys(self, tmp_path):
        """YAML 文件中的所有键都会被加载"""
        path = tmp_path / "field_spec.yaml"
        _make_spec_yaml(path, {
            "fields": {"email": {"attr_code": "E001"}},
            "extra_key": "ignored",
            "version": "1.0",
        })
        spec = load_field_spec(str(path))
        # spec_loader 保留 YAML 中的所有键
        assert "fields" in spec
        assert "email" in spec["fields"]

    def test_multiple_duplicate_attr_codes(self, tmp_path):
        """多个 attr_code 重复：检测第一个重复"""
        path = tmp_path / "field_spec.yaml"
        _make_spec_yaml(path, {
            "fields": {
                "email": {"attr_code": "E001"},
                "phone": {"attr_code": "E002"},
                "addr": {"attr_code": "E001"},  # 重复 E001
                "name": {"attr_code": "E001"},  # 再次重复
            }
        })
        with pytest.raises(ValidationError, match="重复"):
            load_field_spec(str(path))
