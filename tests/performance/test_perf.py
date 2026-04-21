"""
性能基准测试

测试不同数据量级的处理性能：
- 1万行
- 10万行
- 100万行（标记为 slow）

性能目标：
- 一线名单100万行，全流程 ≤ 5分钟（8核/16G机器）

运行方式：
    pytest tests/performance/ -v

运行不包括 slow 测试：
    pytest tests/performance/ -v -m "not slow"
"""

import os
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

import pytest

# 确保项目根目录在 Python 路径中
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


class TestDataLoadingPerformance:
    """数据加载性能测试"""

    @pytest.fixture
    def temp_dir(self):
        """创建临时目录"""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir

    def test_polars_load_performance(self, temp_dir):
        """测试 polars 加载性能（基本测试）"""
        import polars as pl

        # 创建小测试数据
        data = {
            "邮箱": [f"user{i}@example.com" for i in range(1000)],
            "姓名": [f"用户{i}" for i in range(1000)],
        }
        df = pl.DataFrame(data)

        file_path = os.path.join(temp_dir, "perf_test.xlsx")
        df.write_excel(file_path)

        # 测试加载性能
        start = time.time()
        loaded_df = pl.read_excel(file_path)
        elapsed = time.time() - start

        assert len(loaded_df) == 1000
        print(f"\n[1000行加载] 耗时: {elapsed:.3f}秒")

        # 1000行应在1秒内加载完成
        assert elapsed < 1, f"加载时间过长: {elapsed:.3f}秒"


class TestPolarsVectorizedOperations:
    """Polars 向量化操作性能测试"""

    def test_string_operations(self):
        """测试字符串向量化操作性能"""
        import polars as pl

        # 创建测试数据
        n_rows = 10000
        df = pl.DataFrame({
            "email": [f"User{i}@Example.COM" for i in range(n_rows)],
            "name": [f"用户{i}" for i in range(n_rows)],
        })

        # 测试字符串小写转换
        start = time.time()
        result = df.with_columns(
            pl.col("email").str.to_lowercase()
        )
        elapsed = time.time() - start

        print(f"\n[10000行字符串操作] 耗时: {elapsed:.3f}秒")
        assert elapsed < 1

    def test_filter_operations(self):
        """测试过滤操作性能"""
        import polars as pl

        n_rows = 10000
        df = pl.DataFrame({
            "email": [f"user{i}@example.com" for i in range(n_rows)],
            "value": list(range(n_rows)),
        })

        # 测试过滤
        start = time.time()
        result = df.filter(pl.col("value") > 5000)
        elapsed = time.time() - start

        print(f"\n[10000行过滤操作] 耗时: {elapsed:.3f}秒")
        assert len(result) == 4999
        assert elapsed < 1


class TestSetOperations:
    """集合操作性能测试"""

    def test_set_membership(self):
        """测试集合成员检查性能"""
        import polars as pl

        n_rows = 10000
        df = pl.DataFrame({
            "email": [f"user{i}@example.com" for i in range(n_rows)],
        })

        # 创建参考集合
        reference_set = {f"user{i}@example.com" for i in range(5000, 15000)}

        # 测试集合成员检查
        start = time.time()
        result = df.with_columns(
            pl.col("email").is_in(pl.Series(list(reference_set))).alias("in_reference")
        )
        elapsed = time.time() - start

        print(f"\n[10000行集合操作] 耗时: {elapsed:.3f}秒")

        # 验证结果
        in_ref_count = result["in_reference"].sum()
        assert in_ref_count == 5000
        assert elapsed < 1


class TestLargeDataProcessing:
    """大数据处理测试（可选，标记为 slow）"""

    @pytest.fixture
    def temp_dir(self):
        """创建临时目录"""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir

    @pytest.mark.slow
    def test_100k_load_performance(self, temp_dir):
        """测试10万行数据加载性能"""
        import polars as pl

        print("\n[100k 数据生成中...]")
        n_rows = 100000
        data = {
            "邮箱": [f"user{i}@example.com" for i in range(n_rows)],
            "姓名": [f"用户{i}" for i in range(n_rows)],
            "手机": [f"138{i%100000000:08d}" for i in range(n_rows)],
        }
        df = pl.DataFrame(data)

        file_path = os.path.join(temp_dir, "perf_100k.xlsx")
        print("[100k 数据写入中...]")
        df.write_excel(file_path)

        print("[100k 数据加载中...]")
        start = time.time()
        loaded_df = pl.read_excel(file_path)
        elapsed = time.time() - start

        assert len(loaded_df) == 100000
        print(f"\n[100k 加载] 耗时: {elapsed:.2f}秒")

        # 10万行加载应在30秒内完成
        assert elapsed < 30, f"加载时间过长: {elapsed:.2f}秒"

    @pytest.mark.slow
    def test_1m_load_performance(self, temp_dir):
        """测试100万行数据加载性能"""
        import polars as pl

        print("\n[1m 数据生成中...]")
        n_rows = 1000000

        # 使用生成器减少内存峰值
        import random
        def generate_data(n):
            for i in range(n):
                yield {
                    "邮箱": f"user{i}@example.com",
                    "姓名": f"用户{i}",
                    "手机": f"138{random.randint(10000000, 99999999)}",
                }

        df = pl.DataFrame(generate_data(n_rows))

        file_path = os.path.join(temp_dir, "perf_1m.xlsx")
        print("[1m 数据写入中...]")
        df.write_excel(file_path)
        print(f"[1m 数据生成完成: {file_path}]")

        print("[1m 数据加载中...]")
        start = time.time()
        loaded_df = pl.read_excel(file_path)
        elapsed = time.time() - start

        assert len(loaded_df) == 1000000
        print(f"\n[1m 加载] 耗时: {elapsed:.2f}秒 ({elapsed/60:.1f}分钟)")

        # 100万行加载应在2分钟内完成
        assert elapsed < 120, f"加载时间过长: {elapsed:.2f}秒"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
