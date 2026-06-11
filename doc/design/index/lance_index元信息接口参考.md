# Lance 索引元信息接口与数据结构

本文档梳理了 Lance 向上层暴露的所有索引元信息获取接口、输入输出、上下游调用关系及涉及的数据结构。

---

## 一、接口总览

`rust/lance/src/index/api.rs` 中 `DatasetIndexExt` trait 共定义 18 个方法，其中与**获取/查询索引信息**相关的有 8 个：

| #    | 方法                   | 输入                                    | 输出                             | 用途                                 |
| ---- | ---------------------- | --------------------------------------- | -------------------------------- | ------------------------------------ |
| 1    | `load_indices`         | 无                                      | `Arc<Vec<IndexMetadata>>`        | 加载全部索引段元数据（底层基础接口） |
| 2    | `load_index`           | `uuid: &Uuid`                           | `Option<IndexMetadata>`          | 按 UUID 精确查找                     |
| 3    | `load_indices_by_name` | `name: &str`                            | `Vec<IndexMetadata>`             | 按名称查找所有 delta 段              |
| 4    | `load_index_by_name`   | `name: &str`                            | `Option<IndexMetadata>`          | 按名称查找唯一索引                   |
| 5    | `describe_indices`     | `criteria: Option<IndexCriteria>`       | `Vec<Arc<dyn IndexDescription>>` | 聚合描述逻辑索引（推荐上层使用）     |
| 6    | `load_scalar_index`    | `criteria: IndexCriteria`               | `Option<IndexMetadata>`          | 查找最佳匹配的标量索引               |
| 7    | `index_statistics`     | `index_name: &str`                      | `String` (JSON)                  | 获取运行时统计信息                   |
| 8    | `read_index_partition` | `index_name, partition_id, with_vector` | `SendableRecordBatchStream`      | 读取索引分区实际数据                 |

> **注**：trait 中的其余 10 个方法（`create_index_builder`、`create_index`、`drop_index`、`prewarm_index`、`prewarm_index_with_options`、`optimize_indices`、`merge_existing_index_segments`、`commit_existing_index_segments`）属于索引的创建/删除/优化/提交操作，不属于"获取索引信息"范畴。

---

## 二、每个接口详细说明

### 1. `load_indices()` — 加载全部索引

```rust
async fn load_indices(&self) -> Result<Arc<Vec<IndexMetadata>>>;
```

读取当前数据集版本的**所有**索引段元数据。结果带内存缓存（随 manifest 版本变化自动失效）。内部流程：

1. 检查 `index_cache` 是否已有当前版本的缓存
2. 缓存未命中时调用 `read_manifest_indexes()` 从 manifest 中读取
3. `retain_supported_indices()` 过滤掉当前版本不支持的索引类型
4. `infer_missing_vector_details()` 补全旧版向量索引缺失的 `index_details`
5. 若存在 FragmentReuse 索引，通过它对 fragment_bitmap 进行 remap 修正

这是整个体系的**底层入口**，其他所有读取方法均依赖它。

---

### 2. `load_index(uuid)` — 按 UUID 精确查找

```rust
async fn load_index(&self, uuid: &Uuid) -> Result<Option<IndexMetadata>>;
```

最薄的封装层，内部调用 `load_indices()` 后执行 `find(|idx| idx.uuid == *uuid)`。

**上游调用方**：外部按 UUID 定位索引段的场景。

---

### 3. `load_indices_by_name(name)` — 按名称查找（多条）

```rust
async fn load_indices_by_name(&self, name: &str) -> Result<Vec<IndexMetadata>>;
```

内部调用 `load_indices()` 后执行 `filter(|idx| idx.name == name)`。同一名称可能存在多个 delta 段（增量构建产生），因此返回 `Vec`。不存在时返回空 `Vec`。

**上游调用方**：

- `load_index_by_name()` — 加唯一性校验
- `index_statistics()` — 取元数据后分发给统计计算
- `read_index_partition()` — 取元数据后打开逻辑索引读分区
- `commit_existing_index_segments()` — 查已有同名索引判断增删

---

### 4. `load_index_by_name(name)` — 按名称查找（唯一）

```rust
async fn load_index_by_name(&self, name: &str) -> Result<Option<IndexMetadata>>;
```

内部调用 `load_indices_by_name()`，对返回结果做唯一性校验：

- 0 条 → `Ok(None)`
- 1 条 → `Ok(Some(...))`
- ≥2 条 → `Err`（提示用户改用 `load_indices_by_name`）

**上游调用方**：

- `open_frag_reuse_index()` — 打开 FragmentReuse 系统索引
- `open_mem_wal_index()` — 打开 MemWal 系统索引
- 外部按名称查唯一索引的场景

---

### 5. `describe_indices(criteria)` — 聚合描述（推荐接口）

```rust
async fn describe_indices<'a, 'b>(
    &'a self,
    criteria: Option<lance_index::IndexCriteria<'b>>,
) -> Result<Vec<Arc<dyn lance_index::IndexDescription>>>;
```

将零散的 `IndexMetadata` 段**按名称聚合**为逻辑索引，返回高层 `IndexDescription` 对象。

**内部流程**：

1. 调用 `load_indices()` 获取所有段
2. 按 `IndexCriteria` 过滤（若提供）
3. 按 `name` 进行 `chunk_by` 分组
4. 每组的段列表传入 `IndexDescriptionImpl::try_new()` 构建描述对象
5. `try_new()` 内部：
   - 校验所有段 name / fields / type_url 一致性
   - 遍历 fragment 元数据计算 `rows_indexed`
   - 推导 `index_type`（系统索引 > 插件查找 > 旧版推断 > Unknown）

**上游调用方**：

- Python `LanceDataset.describe_indices()` — 主要入口
- Python `LanceDataset.list_indices()` — 已废弃，内部转调本方法
- Python `LanceDataset.has_index` — `len(describe_indices()) > 0`
- 测试代码广泛使用

**注意**：此方法在 Rust 生产代码中仅被 Python 层调用；查询引擎不使用它，而是直接用 `load_indices()` + `load_scalar_index()`。

---

### 6. `load_scalar_index(criteria)` — 标量索引选择

```rust
async fn load_scalar_index<'a, 'b>(
    &'a self,
    criteria: lance_index::IndexCriteria<'b>,
) -> Result<Option<IndexMetadata>>;
```

在全部索引中筛选**最匹配的标量索引**，采用优先级逻辑。

**内部流程**：

1. 调用 `load_indices()` 获取所有段
2. 过滤掉 `fields` 为空的索引（排除系统索引如 frag_reuse）
3. 按 `fields[0]`（第一个字段 ID）分组
4. 对每组调用 `index_matches_criteria()` 匹配条件
5. 同字段多索引时按优先级选择：FTS 索引 > 有 index_details 的 > 多索引场景下的偏好
6. 检查 `fragment_bitmap` 非空（FTS 索引除外——即使为空也必须返回，因为 FTS 查询强制要求索引存在）

**上游调用方**：

- `scanner.rs` — FTS 查询规划、标量索引 pushdown
- `fts.rs` — 全文搜索执行
- `merge_insert.rs` — 查找用于加速 JOIN 的索引
- `inverted.rs` — 倒排索引构建时查找已有索引

这是**查询执行引擎最关键的索引选择入口**。

---

### 7. `index_statistics(name)` — 统计信息

```rust
async fn index_statistics(&self, index_name: &str) -> Result<String>;
```

返回指定索引的运行时统计信息，JSON 字符串格式。

**内部流程**：

1. 调用 `load_indices_by_name(name)` 获取元数据
2. 按索引名分流：
   - `FRAG_REUSE_INDEX_NAME` → `index_statistics_frag_reuse()`（打开索引 → 调用 `statistics()` → 序列化为 JSON）
   - `MEM_WAL_INDEX_NAME` → `index_statistics_mem_wal()`（同上模式）
   - 其他 → `index_statistics_scalar()`：
     - `collect_regular_indices_statistics()` — 统计每段详细信息
     - `gather_fragment_statistics()` — 统计 fragment 级别的覆盖情况
     - 组装 JSON：包含 `index_type`、`name`、`num_indexed_rows`、`num_unindexed_rows`、`num_indexed_fragments`、`num_unindexed_fragments`、`num_indices`、`indices`（每段详情数组）

**上游调用方**：

- Python `LanceDataset.index_statistics()` — 已废弃
- Python `LanceDataset.stats.index_stats()` — 推荐替代
- `migrate_and_recompute_index_statistics()` — 迁移时重算

---

### 8. `read_index_partition(name, partition_id, with_vector)` — 读分区数据

```rust
async fn read_index_partition(
    &self,
    index_name: &str,
    partition_id: usize,
    with_vector: bool,
) -> Result<SendableRecordBatchStream>;
```

按索引名 + 分区 ID 读取某个向量索引分区的实际数据。

**内部流程**：

1. 调用 `load_indices_by_name(name)` 获取元数据
2. 取第一个索引段的 `fields[0]` 获取字段 ID
3. 通过 schema 解析字段名
4. 调用 `self.open_logical_vector_index(column_name, index_name)` 打开逻辑向量索引
5. 调用 `logical_index.as_ivf()?.read_partition(partition_id, with_vector)` 读取分区数据

**注意**：仅适用于向量索引（IVF 族），标量索引无分区概念。

---

### Python 绑定层额外接口

除上述 8 个 Rust 接口的 Python 封装外，Python 侧还有：

#### `list_indices()` — 已废弃

```python
def list_indices(self) -> List[dict]
```

内部转调 `describe_indices()`，展平所有段为一个列表，每个段返回一个包含 `name`、`type`、`uuid`、`fields`、`version`、`fragment_ids`、`base_id` 的扁平 dict。已废弃，建议用 `describe_indices()`。

#### `has_index` (property)

```python
@property
def has_index(self) -> bool
```

直接封装 `len(self.describe_indices()) > 0`。

#### LanceDB Cloud API

- `namespace.list_table_indices(request: ListTableIndicesRequest) -> ListTableIndicesResponse`
- `namespace.describe_table_index_stats(request: DescribeTableIndexStatsRequest) -> DescribeTableIndexStatsResponse`

---

## 三、上下游调用关系

```
                        ┌──────────────────────────────┐
                        │        Python 绑定层           │
                        │  LanceDataset.describe_indices │
                        │  LanceDataset.list_indices     │
                        │  LanceDataset.index_statistics │
                        │  Namespace.list_table_indices  │
                        └──────────────┬───────────────┘
                                       │ PyO3 FFI
                        ┌──────────────┴───────────────┐
                        │      查询执行引擎 (上游)       │
                        │  scanner.rs (ANN / FTS 规划)   │
                        │  fts.rs (全文搜索索引选择)      │
                        │  merge_insert.rs (索引查找)     │
                        └──────────────┬───────────────┘
                                       │
    ╔══════════════════════════════════╧══════════════════════════════╗
    ║              api.rs → Dataset impl (rust/lance/src/index.rs)   ║
    ╠════════════════════════════════════════════════════════════════╣
    ║                                                                ║
    ║  load_index(uuid)        load_indices_by_name(name)            ║
    ║       │                        │     │                         ║
    ║       ▼                        ▼     ▼                         ║
    ║  ┌─────────────────────────────────────────┐                   ║
    ║  │          load_indices()                 │ ← 核心枢纽        ║
    ║  │  ① read_manifest_indexes() 读 manifest  │                   ║
    ║  │  ② retain_supported_indices() 过滤       │                   ║
    ║  │  ③ index_cache 读写缓存                 │                   ║
    ║  │  ④ infer_missing_vector_details()       │                   ║
    ║  │  ⑤ frag_reuse_index remap 位图修正       │                   ║
    ║  └────────┬───────────────┬────────────────┘                   ║
    ║           │               │                                    ║
    ║           ▼               ▼                                    ║
    ║  load_index_by_name  describe_indices                          ║
    ║  (唯一性校验)         (按名称聚合 + IndexCriteria 过滤)         ║
    ║       │               │                                        ║
    ║       ▼               ▼                                        ║
    ║  index_statistics  load_scalar_index                           ║
    ║  (统计信息分发)     (标量索引最佳匹配)                           ║
    ║                         │                                      ║
    ║  read_index_partition ◄─┘                                      ║
    ║  (按分区读实际数据)                                              ║
    ╚════════════════════════════════════════════════════════════════╝
                                       │
                        ┌──────────────┴───────────────┐
                        │    底层存储 (object store)      │
                        │  read_manifest_indexes()       │
                        │  Manifest / IndexMetadata      │
                        └──────────────────────────────┘
```

**调用层级总结**：

| 层级        | 方法                      | 依赖深度                         |
| ----------- | ------------------------- | -------------------------------- |
| L0 (存储)   | `read_manifest_indexes()` | 0（直接读文件）                  |
| L1 (核心)   | `load_indices()`          | 1（调 L0 + 缓存 + 推断）         |
| L2 (薄封装) | `load_index`              | 1（调 L1 + 过滤）                |
| L2 (薄封装) | `load_indices_by_name`    | 1（调 L1 + 过滤）                |
| L2 (薄封装) | `load_index_by_name`      | 2（调 L2 → L1 + 校验）           |
| L2 (聚合)   | `describe_indices`        | 2（调 L1 + 分组聚合）            |
| L2 (选择)   | `load_scalar_index`       | 2（调 L1 + 优先级匹配）          |
| L3 (统计)   | `index_statistics`        | 3（调 L2 → L1 + 分流统计）       |
| L3 (读取)   | `read_index_partition`    | 3（调 L2 → L1 + 打开索引读数据） |

核心规律：**所有方法最终都汇聚到 `load_indices()`**，它是整个索引元信息获取体系的唯一底层入口。

---

## 四、涉及的数据结构

### 4.1 存储层（Manifest 持久化）

#### `IndexMetadata` — 索引段元数据

**位置**：`rust/lance-table/src/format/index.rs:31`

```rust
pub struct IndexMetadata {
    pub uuid: Uuid,                              // 索引段全局唯一 ID
    pub fields: Vec<i32>,                        // 被索引的字段 ID 列表
    pub name: String,                            // 用户定义的索引名称
    pub dataset_version: u64,                    // 最后更新时的数据集版本号
    pub fragment_bitmap: Option<RoaringBitmap>,  // 覆盖的 fragment 集合
    pub index_details: Option<Arc<prost_types::Any>>,  // 索引类型特定详情 (protobuf)
    pub index_version: i32,                      // 索引格式版本号
    pub created_at: Option<DateTime<Utc>>,       // 创建时间戳
    pub base_id: Option<u32>,                    // 外部索引文件的基础路径 ID
    pub files: Option<Vec<IndexFile>>,           // 索引段文件列表及大小
}
```

整个体系的**最底层核心结构**，直接序列化存储在 Manifest 中。一个逻辑索引可能对应多个 `IndexMetadata`（每个 delta 段一条）。

#### `IndexFile` — 段内文件描述

**位置**：`rust/lance-table/src/format/index.rs:22`

```rust
pub struct IndexFile {
    pub path: String,       // 相对路径，如 "index.idx"
    pub size_bytes: u64,    // 文件大小（字节）
}
```

隶属于 `IndexMetadata.files`，使得 `describe_indices()` 可以报告 `total_size_bytes` 而无需额外的 HEAD 请求。

---

### 4.2 API 层（Trait 定义）

#### `IndexSegment` — 物理索引段（API 层）

**位置**：`rust/lance/src/index/api.rs:21`

```rust
pub struct IndexSegment {
    uuid: Uuid,                          // 段唯一 ID
    fragment_bitmap: RoaringBitmap,      // 覆盖的 fragment（非 Option，必填）
    index_details: Arc<prost_types::Any>, // 索引详情（非 Option，必填）
    index_version: i32,                  // 索引版本
}
```

与 `IndexMetadata` 的区别：`fragment_bitmap` 和 `index_details` 是**必填**的。在 `commit_existing_index_segments` 等写入路径中作为中间表示。`IndexMetadata` 实现了 `IntoIndexSegment` trait 可向此转换。

#### `IndexCriteria` — 索引筛选条件

**位置**：`rust/lance-index/src/traits.rs:10`

```rust
pub struct IndexCriteria<'a> {
    pub for_column: Option<&'a str>,          // 限定列名
    pub has_name: Option<&'a str>,            // 限定索引名
    pub must_support_fts: bool,               // 必须支持全文搜索
    pub must_support_exact_equality: bool,    // 必须支持精确等值匹配
}
```

作为 `describe_indices()` 和 `load_scalar_index()` 的**输入参数**。支持 builder 模式链式调用：

```rust
IndexCriteria::default()
    .for_column("text")
    .supports_fts()
```

#### `IndexDescription` (trait) — 逻辑索引描述

**位置**：`rust/lance-index/src/traits.rs:87`

```rust
pub trait IndexDescription: Send + Sync {
    fn name(&self) -> &str;                     // 索引名称（跨段统一）
    fn metadata(&self) -> &[IndexMetadata];      // 所有段的原始元数据
    fn segments(&self) -> &[IndexMetadata];      // metadata() 的别名
    fn type_url(&self) -> &str;                  // protobuf type URL
    fn index_type(&self) -> &str;                // 简短类型标识 (BTREE, IVF_PQ...)
    fn rows_indexed(&self) -> u64;              // 近似索引行数（跨段求和）
    fn field_ids(&self) -> &[u32];              // 被索引的字段 ID
    fn details(&self) -> Result<String>;         // 索引详情的 JSON 字符串
    fn total_size_bytes(&self) -> Option<u64>;  // 所有段文件总大小
}
```

`describe_indices()` 的**返回值类型**，将多个同名 `IndexMetadata` 段聚合为一个逻辑索引的整体描述。

---

### 4.3 内部实现层

#### `IndexDescriptionImpl` — trait 具体实现

**位置**：`rust/lance/src/index.rs:682`（私有结构体）

```rust
struct IndexDescriptionImpl {
    name: String,
    field_ids: Vec<u32>,
    segments: Vec<IndexMetadata>,
    index_type: String,
    details: Option<IndexDetails>,
    rows_indexed: u64,
}
```

实现了 `IndexDescription` trait。由 `describe_indices()` 内部通过 `try_new()` 构造：

- 校验所有段的 name / fields / type_url 一致性
- 遍历 fragment 元数据计算 `rows_indexed`
- 推导 `index_type`：系统索引 > 插件查找 > 旧版推断 > "Unknown"

#### `IndexDetails` — protobuf Any 包装

**位置**：`rust/lance/src/index/scalar.rs:239`

```rust
pub struct IndexDetails(pub Arc<prost_types::Any>);
```

对 `IndexMetadata.index_details` 的轻量包装，提供便捷方法：

- `is_vector()` — 判断是否为向量索引（type_url 以 `VectorIndexDetails` 结尾）
- `supports_fts()` — 判断是否支持全文搜索（type_url 以 `InvertedIndexDetails` 结尾）
- `get_plugin()` — 获取对应的标量索引插件
- `index_version()` — 获取索引版本号

#### `IndexMetadataKey` — 索引缓存键

**位置**：`rust/lance/src/session/index_caches.rs:98`

```rust
pub struct IndexMetadataKey {
    pub version: u64,    // 数据集版本号
}
```

用于 `index_cache` 的键。同一版本的 `load_indices()` 只加载一次，结果被缓存。版本变化时缓存自动失效。对应缓存值为 `Vec<IndexMetadata>`。

#### `IndexFileVersion` — 向量索引文件版本

**位置**：`rust/lance/src/index/vector.rs:239`

```rust
pub enum IndexFileVersion {
    // 用于区分不同版本的向量索引文件格式
}
```

---

### 4.4 Python / Java 绑定层

#### `IndexDescription` (Python)

**位置**：`python/src/indices.rs:673` / `python/python/lance/lance/indices/__init__.pyi:79`

```python
class IndexDescription:
    name: str                         # 索引名称
    type_url: str                     # protobuf type URL
    index_type: str                   # 索引类型 (BTREE, IVF_PQ...)
    fields: list[int]                 # 字段 ID 列表
    field_names: list[str]            # 字段路径列表（含反引号转义）
    num_rows_indexed: int             # 近似索引行数
    details: dict                     # 索引详情的 JSON 字典
    segments: list[IndexSegmentDescription]  # 段描述列表
    total_size_bytes: Optional[int]   # 索引总大小
```

由 `PyIndexDescription::new()` 从 Rust 的 `dyn IndexDescription` 转换而来。

#### `IndexSegmentDescription` (Python)

**位置**：`python/src/indices.rs:619` / `python/python/lance/lance/indices/__init__.pyi:68`

```python
class IndexSegmentDescription:
    uuid: str                                  # 段 UUID
    dataset_version_at_last_update: int        # 最后更新的数据集版本
    fragment_ids: set[int]                     # 覆盖的 fragment ID 集合
    index_version: int                         # 索引版本号
    created_at: Optional[datetime]             # 创建时间
    size_bytes: Optional[int]                  # 段文件总大小
    base_id: Optional[int]                     # 外部基础路径 ID
```

由 `from_metadata()` 直接从 `lance_table::format::IndexMetadata` 转换而来。

#### `IndexConfig` (Python)

**位置**：`python/src/indices.rs` / `python/python/lance/lance/indices/__init__.pyi:20`

```python
class IndexConfig:
    index_type: str     # 索引类型
    config: str         # 配置 JSON 字符串
```

#### `IndexSegment` (Python)

**位置**：`python/python/lance/lance/indices/__init__.pyi:24`

```python
class IndexSegment:
    uuid: str
    fragment_ids: set[int]
    index_version: int
```

与 Rust 的 `IndexSegment` 对应，用于 `commit_existing_index_segments` 等写入路径的外部输入。

#### `IndexDescription` (Java)

**位置**：`java/src/main/java/org/lance/index/IndexDescription.java`

```java
public final class IndexDescription {
    String getName();            // 索引名称
    List<Integer> getFieldIds(); // 字段 ID
    String getTypeUrl();         // protobuf type URL
    String getIndexType();       // 索引类型
    long getRowsIndexed();       // 近似行数
    List<Index> getMetadata();   // 每段元数据
    List<Index> getSegments();   // 同上（别名）
    String getDetailsJson();     // JSON 详情
}
```

---

### 4.5 数据结构对应关系

```
Manifest 存储             Rust API 层              Python 绑定层
────────────────────────────────────────────────────────────────
IndexMetadata  ────────► IndexDescription ──────► IndexDescription
  ├─ uuid                  (trait)                  ├─ name
  ├─ fields                ├─ name()                ├─ type_url
  ├─ name                  ├─ metadata() ───┐       ├─ index_type
  ├─ dataset_version       ├─ segments() ───┤       ├─ fields / field_names
  ├─ fragment_bitmap       ├─ type_url()     │       ├─ num_rows_indexed
  ├─ index_details         ├─ index_type()   │       ├─ details (JSON)
  ├─ index_version         ├─ rows_indexed() │       ├─ segments ──► IndexSegmentDescription
  ├─ created_at            ├─ field_ids()    │       │                 ├─ uuid
  ├─ base_id               ├─ details()      │       │                 ├─ dataset_version_at_last_update
  └─ files ──► IndexFile   └─ total_size_..  │       │                 ├─ fragment_ids
       ├─ path                               │       │                 ├─ index_version
       └─ size_bytes                         │       │                 ├─ created_at
                                             │       │                 ├─ size_bytes
                         IndexDescriptionImpl ◄───────┘                 └─ base_id
                           (具体实现, 持有 Vec<IndexMetadata>)           └─ total_size_bytes

                         IndexCriteria            IndexSegment
                           (筛选输入)               (提交输入, 含 uuid + fragment_ids + index_version)
```

---

## 五、关键文件索引

| 文件                                                       | 内容                                                         |
| ---------------------------------------------------------- | ------------------------------------------------------------ |
| `rust/lance/src/index/api.rs`                              | `DatasetIndexExt` trait 定义（8 个读取接口 + 10 个写入接口） |
| `rust/lance/src/index.rs`                                  | `Dataset` 对 `DatasetIndexExt` 的完整实现                    |
| `rust/lance-table/src/format/index.rs`                     | `IndexMetadata`、`IndexFile` 结构体定义                      |
| `rust/lance-index/src/traits.rs`                           | `IndexDescription` trait、`IndexCriteria` 结构体             |
| `rust/lance-index/src/lib.rs`                              | `IndexType` 枚举、`IndexParams` trait                        |
| `rust/lance/src/index/scalar.rs`                           | `IndexDetails` 包装结构                                      |
| `rust/lance/src/session/index_caches.rs`                   | `IndexMetadataKey` 缓存键                                    |
| `python/src/dataset.rs`                                    | Python `describe_indices()`、`index_statistics()` 绑定       |
| `python/src/indices.rs`                                    | `PyIndexDescription`、`PyIndexSegmentDescription` 定义       |
| `python/python/lance/indices/__init__.pyi`                 | Python 类型桩文件                                            |
| `python/python/lance/dataset.py`                           | Python `LanceDataset` 公开 API                               |
| `python/python/lance/namespace.py`                         | LanceDB Cloud 索引 API                                       |
| `java/src/main/java/org/lance/index/IndexDescription.java` | Java 绑定                                                    |