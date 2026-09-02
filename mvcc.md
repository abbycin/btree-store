# 读写不阻塞（MVCC 并发）改造方案

## 1. 问题、现状或新设计

### 1.1 现状

`BTree` 用一把进程级 `writer_lock: Arc<RwLock<()>>`（`src/lib.rs:2076`）串行化所有操作：

- 读路径 `view`（`src/lib.rs:2378`）与 `buckets_internal`（`src/lib.rs:2537`）持**读锁贯穿整个闭包**，包括全部页 I/O 与迭代。
- 写路径 `exec`/`exec_multi`/`commit`/`new_bucket`/`del_bucket`（`src/lib.rs:2219, 2300, 2414, 2267, 2512`）持**写锁贯穿整个事务**，包括页写入、allocator 更新与 fsync 发布。
- parking_lot `RwLock` 是 task-fair：等待中的写者会挡住新读者。

结果是**双向阻塞**：一个长 `view`（大迭代、GC 扫描）让写者等待，等待中的写者再挡住后续所有读（convoy）；一个大 `exec_multi`（含 fsync）让所有读等待。

### 1.2 问题

btree-store 定位为通用嵌入式 KV。双向阻塞是通用场景的硬伤：

- 读阻塞写：长读直接抬高写延迟；在 mace 元数据场景表现为 GC 扫描延迟 checkpointer 的元数据 commit，进而延迟流控释放。
- 写阻塞读：大 commit（含 fsync）抬高前台读延迟。

mace 当前负载下影响有界（元数据不在热路径、GC 扫描有批次上限），但通用化后不可接受。目标状态：**读者遍历与写者发布互不阻塞**——读者在固定快照上并发遍历，写者并发发布新代，读者遍历不等待写者、写者不等待读者。可精确保证的边界：读者仅在其 refresh（句柄 seq 变化时安装新快照）路径可能等待写者的发布临界区（含 fsync）；与现状"每次 view 都等待整个 commit"相比，最坏情况相同、典型严格更优（§2.6）。

### 1.3 关键约束

- **不改存储格式**：双缓冲 meta slot、节点页、reusable/retired extent 链的布局与语义全部不变；老库直接打开，无需迁移、不 bump `FORMAT_VERSION`。
- 单进程单写者模型不变；多进程并发明确不在范围内。
- 公开 API（`view`/`exec`/`exec_multi` 闭包签名）不变。

### 1.4 代表性失败序列

```
读者 R 在代 g 上遍历（大迭代，持读锁）
写者 W 提交 g+1（等读锁，被 R 阻塞）
新读者 R2 到达（task-fair：排在 W 后面，也被阻塞）
→ W 的提交延迟 → R2 的读延迟；R 的遍历时长直接放大两者
```

## 2. 解决办法

### 2.1 机制总览

用**读者 epoch 追踪 + allocator 延迟页复用**替代 `RwLock`，三个组件：

1. **写者互斥锁**：`writer_lock` 从 `RwLock<()>` 改为 `Mutex<()>`，只串行化写者。
2. **读者 epoch 注册表**（新模块 `src/epoch.rs`）：`view` 进入时 pin 当前 epoch，退出时 unpin；维护 `oldest_active_reader_epoch`。
3. **allocator 条件提升**（`src/store.rs:1658-1667`）：提交时不再无条件把 `retired` 提升为 `reusable`，而是满足读者静默条件才提升，否则把 `retired` 带入下一代继续隔离。

核心不变量：**一个物理页在"仍可能被在途读者引用"期间不得复用**。当前由读锁强制，改造后由 epoch 条件强制。

### 2.2 正确性论证：读锁为什么是承重墙，epoch 如何替代它

**现状的复用窗口**（`src/store.rs:1637-1667`）：构造代 g+1 时，把当前已发布代 g 的 `retired` 集（记为 R_g，即"g-1 可达、g 不可达"的页）无条件提升为 reusable 并可能立即覆写。R_g 被 g-1 及更早代的读者引用（其中部分页在更早代也可达）；`view` 启动时刷新到最新已发布代，所以一个在 g-1 时代启动、g 已发布后仍在遍历的读者，会读到被 g+1 覆写的页 → 损坏。quarantine 规则只保护崩溃回退（上一 meta slot），**不保护在途读者**。读锁正是挡住这个窗口的机制。

**epoch 替代**：读者 pin 其启动时的 epoch；epoch 每次共享快照安装（正常发布或写者采纳磁盘更新代）+1。提升条件为：

```
提升 R_g 安全 ⟸ 无读者 pin < E_g（引用 R_g 的读者——g-1 及更早代——pin 值均 < E_g：refresh 见 g-1 为最新 ⟹ pin 早于 g 发布 ⟹ pin < E_g）
            ⟺ oldest_active_reader_epoch ≥ E_g
            ⟺ oldest_active_reader_epoch ≥ current_epoch   （g+1 构造时 current_epoch = E_g）
```

条件为充分条件：可能额外阻塞"pin 早于 g 发布、refresh 后读 g"的读者（其 pin < E_g 但不引用 R_g），保守安全。注意不能用"无读者 pin ≤ E_{g-1}"表述——generation-only 发布可使 g-1 读者 pin 值落在 (E_{g-1}, E_g) 区间，该表述会漏掉它们；"pin < E_g" 精确覆盖全部危险读者。

推导要点：读者总是 pin 当前 epoch 值；g+1 构造发生在 g 发布之后、g+1 发布之前，此刻 epoch 计数器恰为 E_g（若期间有 generation-only 发布使 epoch 前进，条件更严格，仍保守安全）。条件不满足时 R_g 留在 `next_retired` 随 g+1 发布，后续提交条件满足时统一提升——**延迟只推迟提升、从不提前**，是保守方向，因此对 R_g 与更早的延迟页（R_{g-1} 等）同时安全（E 单调，`oldest ≥ E_{g+1}` 蕴含 `oldest ≥ E_g`）。

**线性化论证（回应"扫描-进入-发布"TOCTOU 质疑）**：安全性不依赖 pin 与写者扫描之间的握手，而依赖 epoch 计数器本身提供的线性化。两个精确事实：

1. **读者在代 Y 上 ⟹ 其 pin 早于 Y+1 的发布**：读者 refresh 总是读到最新已发布代，故 refresh 见 Y 为最新 ⟹ Y+1 未发布 ⟹ refresh 早于 Y+1 发布；pin 先于 refresh（规定），故 pin 更早。
2. **X+1 构造时提升的 R_X 仅 X-1 可达**：R_X 是 X 构造退休的页（X-1 可达、X 不可达），从 X 的 root 不可达（`store.rs:1637-1638` 注释："all allocator pages reachable from g remain quarantined until g+1 commits"）。

合并：引用 R_X 的读者在 X-1 或更早代上（事实 2）⟹ 其 pin 早于 X 的发布（事实 1，Y ≤ X-1 ⟹ Y+1 ≤ X）⟹ 早于 X+1 构造开始（写者由 Mutex 串行化，X+1 构造在 X 发布之后）⟹ 写者扫描必见。**扫描漏掉的新读者只可能是 pin 当前计数器、读当前或更新代的读者，其快照与本次提升的页（严格更旧的代）不相交**。

审核意见的具体交错（读者 pin E_{g-1}、读 g-1、写者发布 g 时复用 R_{g-1}）不成立：R_{g-1} 仅 g-2 可达，从 g-1 的 root 不可达，读者按 g-1 遍历不可能读到被覆写的页。要引用 R_{g-1} 必须在 g-2 快照上，而 g-2 读者 pin 早于 g-1 发布（事实 1）⟹ 早于 g 构造的扫描。结论：任意交错下页被覆写时无在途读者引用；无需 reader-admission 握手。固定 grace period 也无法替代本机制——读者可任意老，只有按读者活动延迟提升（本方案的 epoch 条件）才闭合。

**崩溃安全**：延迟页（如 R_g）在 g+1 的 retired 集里，而 g+1 的 fallback 是 g，g 不引用 R_g（R_g 仅 g-1 可达）——**延迟页不被 fallback 引用**，quarantine 语义保持成立；正常 retired 页（R_{g+1}，被 fallback g 引用）本就是 quarantine 的既定状态，不受影响。双缓冲协议保证 fallback 恒为上一代，g-1 在 g 发布后永不被选中。恢复后无在途读者，retired 页立即满足提升条件。格式与恢复语义均不变。

### 2.3 端到端流程

**读者 `view`**：

```
1. pin 当前 epoch（先于任何页读取）
2. refresh：若共享 seq 变化 → clear_cache + refresh_sb（可能短暂等待写者的 allocator Mutex，见 2.5）
3. 解析 bucket root，在固定快照上遍历（无锁、positional I/O）
4. unpin
```

pin 先于 refresh：refresh 可能把读者推进到更新的代，pin 值偏旧 → 提升条件更保守，安全。

**写者 `exec`**：

```
1. 取 writer Mutex（只与写者互斥）
2. refresh_internal → TxnCore 事务（页分配只从 reusable 取，不触碰 retired）
3. commit_roots_with_alloc：
   a. 计算 promotable = oldest_active_reader_epoch ≥ current_epoch
   b. promotable → 现有提升循环（journal 记录，可回滚）
     否则 → 把 retired 逐 extent 加入 next_retired（随本代发布）
   c. 写 allocator-list 页 → 写 meta slot → sync → shared.update
   d. epoch.fetch_add(1, Release)（发布协议最后一步）
4. 释放 Mutex
```

**伪代码（`commit_roots_with_alloc` 中替换 `src/store.rs:1658-1667`）**：

```rust
// 现有：无条件提升
// for extent in retired.iter() { journal.add(Reusable, &mut reusable, ...); }
let promotable = self.epoch.oldest_active_reader_epoch() >= self.epoch.current();
if promotable {
    for extent in retired.iter() {
        journal.add(ExtentSetKind::Reusable, &mut reusable, extent.page_id, extent.nr_pages);
    }
} else {
    for extent in retired.iter() {
        next_retired.add(extent.page_id, extent.nr_pages);
    }
}
```

去重逻辑（`next_retired.remove(...)`，`src/store.rs:1646-1648`）在提升决策之前执行，语义不变：pending_free 与 reusable/retired 的重叠页只保留一个所有权类；延迟分支随后把整个 `retired` 加回 `next_retired`，净效果为 `retired ∪ (pending_free − reusable)`，正确。

### 2.4 状态与所有权

| 状态 | 所有者 | 变化 |
| --- | --- | --- |
| `writer_lock` | `BTree`（Arc 共享） | `RwLock<()>` → `Mutex<()>` |
| `epoch` 计数器 | `Store` | 新增，每次共享快照安装成功后 +1 |
| 读者注册表 | `Store`（`epoch` 内） | 新增，同 Store 句柄共享；实例间完全隔离 |
| `retired`/`reusable` 集 | `Store`（Mutex） | 不变，仅提升时机条件化 |
| 句柄 `start_seq`/`local_snapshot` | 各 `BTree` 句柄 | 不变 |
| `ReadOnlyTxn._guard` | 事务对象 | `RwLockReadGuard` → epoch guard（内部类型，公开 API 不变） |

### 2.5 同步、排序与失败规则

- **写者互斥**：所有写路径共用 `Mutex`，`commit_internal` 的 `COMMIT_SEQUENCE_CONFLICT` 不变量（`src/lib.rs:2477-2484`）保持成立。理由：检查发生在发布之前，写者持 Mutex 期间共享快照不变——`shared.update` 只发生在 `publish_generation`（写者持锁）与写者路径的 `refresh_sb` 安装（同样持锁）中；**读者 refresh 从不安装磁盘更新代**（`refresh_sb(allow_install=false)`，见 §2.5 读者-写者），只把句柄 `start_seq` 更新为共享快照值，不会造成检查失配。若读者安装失败发布留下的磁盘更新代，会在写者事务中途推进共享快照，触发误报 abort——该路径已被读者不安装规则闭合。
- **读者-写者**：遍历无锁；仅 refresh 路径经 allocator 的 `sb`/`reusable`/`retired` Mutex 串行化（等待时长见 §2.6）。`refresh_sb`（`src/store.rs:2037-2092`）磁盘读取在取锁之前，持锁仅用于安装（微秒级），并在锁内二次检查 `sb.seq > current_sb.seq`——写者先提交则跳过，不会用旧状态覆盖新状态。
- **发布与采纳顺序**：正常发布先完成 meta slot 写入与 sync，再执行 `shared.update`，最后 `epoch.fetch_add(1, Release)`；写者 `refresh_sb(true)` 采纳磁盘更新代时同样执行 `shared.update` 后再推进 epoch。任何共享快照安装都必须遵守这个 `shared.update` → epoch+1 顺序，使计数器与共享快照更新次数保持一一对应。读者 pin 以 Acquire 读 epoch 计数器，与写者的 Release 前进同步：读到新值的读者其后的 `shared_snapshot()` 必见新代；读到旧值的读者按旧代快照遍历，同样安全（pin 值偏旧只会让提升条件更保守）。
- **缓存失效**：`observer.invalidate(page_id)`（`src/store.rs:1790`，`src/cache.rs:226-229`）不变。延迟复用保证页被覆写时无在途读者，失效语义正确；读者持有的 `Arc<Node>` 在缓存清空后仍有效。
- **回滚**：提升循环走 `AllocatorMutationJournal`，失败时 `journal.rollback` 恢复（`src/store.rs:1685-1690`）；延迟分支不产生 journal 条目（未改动 reusable）。`*retired = next_retired` 在成功后统一替换。
- **崩溃/恢复**：格式不变。任一崩溃点（页写入、slot 写入、sync）的恢复路径与现状一致；延迟页随 retired 集持久化，恢复后无读者，立即满足提升条件。
- **no-op 提交**：`commit_internal` 的早退分支（`src/lib.rs:2472-2478`）不变，不发布、不前进 epoch。

### 2.6 性能代价与接受的限制

**热路径（view 进出）**：

- 现状：`writer_lock.read()`（parking_lot 读锁，无竞争 ~20-50ns，竞争时劣化）。
- 改造后：epoch pin/unpin。采用 guard 携带 slot 的注册表（§4）：pin = 按 shard 提示探测空闲槽 + 一次槽上 CAS，unpin = 一次原子写；**无分配**，争用按 64 分片摊薄（每槽独立缓存行，无共享表头）。探测起点 shard 按 (线程, 注册表) 缓存（RocksDB `ThreadLocalPtr` 模式，§4），热路径免去每次线程哈希。**无竞争时**代价 ~30-60ns，与现状读锁同量级；高并发下受槽 CAS 争用摊薄，需 phase c bench 在 1/8/64/256 读者下对比 `RwLock::read()` 验证不超线性劣化；超限走溢出路径（§4，罕见，允许分配）。
- 结论：热路径无实质劣化；现有单线程 bench（insert/prefix）只增加常数。

**提交路径**：

- 写者 `Mutex` 与现状写锁同价。
- 新增 `oldest_active_reader_epoch` 计算：扫描固定数量 slot（O(256) 原子读），每 commit 一次，相对 fsync 可忽略。
- 提升条件不满足时选择**延迟而非等待**：写者不因读者遍历停顿（仅可能短暂等待读者 refresh 的 allocator 安装临界区，微秒级）。

**读者 refresh 路径（唯一新增的阻塞点）**：

- 读者 `refresh_sb`（`src/store.rs:2037-2092`）与写者 commit 通过 allocator 的五个 Mutex 串行化；写者持锁区间覆盖 `write_allocator_state` + `publish_generation`（`src/store.rs:1610-1614` 起，含 `sync_publication` 的 fsync）。
- 量化：读者在句柄 seq 变化时 refresh 一次，最坏等待 = 一个写者 commit 的 allocator 临界区（含 fsync，sync_on_write 下可达毫秒级）。
- 与现状对比：现状下读者 view 在写者持写锁期间**每次**都等待整个 commit（含 fsync）；改造后仅 refresh 路径等待，且仅当 seq 变化。**最坏情况不变，典型情况严格更优**。
- 可选优化（不在本方案内）：把 fsync 移出 allocator 临界区，或 refresh 改用 seqlock 免锁读取——需重排发布协议，风险大于收益，留作后续。

**allocator 写放大（延迟提升的代价）**：

- 每 commit 重写完整 allocator 状态（reusable + retired extent 链）。延迟提升使 retired 集变大 → 链变长 → 每 commit 多写 allocator-list 页。
- 稳态量化：读者 pin 当前 epoch 不阻塞提升；只有"在旧代启动、跨写者提交仍在遍历"的读者触发延迟。view 时长（µs-ms）远短于 commit 间隔时，延迟几乎不发生。
- 最坏情况：长 view（全表扫描）跨多个 commit → retired 集累积 → 文件增长 + allocator 写放大，有界于 view 生命周期，是 MVCC 固有代价；与现状"长 view 阻塞所有写"相比严格更优。

**缓存**：`clear_cache` 频率与现状相同（seq 变化时触发）；并发读者/写者下语义不变（读者持 `Arc<Node>`，失效安全）。

**接受的限制**：

- 长生命周期 `view` 阻塞页复用（空间放大），但不阻塞写——比现状（阻塞写）严格更优。
- **资源边界（明确策略）**：永久持有的 view（调用方缺陷）使 retired 集无限累积——每 commit 重写更长的 allocator 链、reusable 耗尽后新写持续扩展文件，最终触发既有 fatal 边界 `FatalReason::AddressSpaceExhausted`（u32 页号空间，`src/store.rs:1422`）或磁盘耗尽，与"任意 KV 写满存储"同类，属既有故障边界而非新引入的损坏路径。接受的策略：不加硬性配额或写拒绝（会破坏写可用性，比空间增长更糟）；API 文档明确 view 应短生命周期；debug/test 构建暴露活动 pin 计数供诊断。可选的后续护栏（不在本方案内）：活动快照预算与观测指标。
- 多进程并发不在范围内（现状即拒绝）。

### 2.7 约束影响

btree-store 无 live constraint registry（mace 有，本仓库没有）；稳定设计源是 `docs/design.md` 与 `AGENTS.md`。按设计文档陈述分类：

| 分类 | 约束 | 影响与证据 |
| --- | --- | --- |
| preserved | design.md §5.1 "one process-level writer and multiple readers" | 写者串行化保留（Mutex）；读者并发保留（epoch pin 不互斥） |
| changed | design.md §5.1 "Reader locks allow concurrent snapshot reads while preventing a writer from publishing against an inconsistent in-memory state" | 机制替换：读锁 → epoch pin + 延迟提升；不变量（写者不得复用在途读者可能引用的页）保留，由 §2.2 条件强制。design.md §5.1 需在 phase c 更新 |
| changed | design.md §7.3 "promoted to reusable only while constructing a later generation whose fallback metadata no longer references it" | 强化：提升额外要求无在途读者引用相关代；durable quarantine 规则本身不变。design.md §7.3 需在 phase c 更新 |
| preserved | AGENTS.md "Page Reclamation: pages only returned to free list after successful superblock update" | 回收仍在发布成功后；额外受读者静默门控 |
| preserved | AGENTS.md "Generation Publication" / "Durable Quarantine" | 发布协议与 durable extent 持久化不变；延迟页随 retired 集持久化，reopen 恢复 |
| new | 提升规则：`oldest_active_reader_epoch ≥ current_epoch` | 新运行时边界；见证 = phase b 的并发/复用测试；phase c 写入 design.md §5.1/§7.3。若日后引入 constraint registry，以 phase b 测试为 verifier 注册为 active |

无必要破坏的约束；不引入 registry 空转。

## 3. 实现计划与验收

### 3.1 阶段

**phase a：epoch 注册表 + allocator 条件提升（惰性落地，行为不变）**

| 字段 | 内容 |
| --- | --- |
| objective | 机制就位但读者尚未 pin：注册表无读者 → `oldest = current` → 提升条件恒真 → 行为与现状完全一致 |
| changes | 新增 `src/epoch.rs`（`EpochRegistry`/`EpochGuard`，guard 携带 slot 实现，见 §4）；`Store` 增加 `epoch` 字段；`commit_roots_with_alloc` 提升循环条件化（§2.3 伪代码）；`publish_generation` 末尾 `epoch.fetch_add(1, Release)`；epoch 单元测试（含 slot 编码：epoch 0 活动读者存 1、不被 `oldest()` 误判为空闲）；allocator 延迟测试（用测试钩子手动 pin 一个旧 epoch，验证提升被延迟、unpin 后恢复）；**epoch 0 回归测试**：新建 Store（epoch 0）→ pin 读者 → 首次提交 → 验证读者快照页未被复用 |
| focused checks | `cargo test --all-features` 全绿（惰性落地必须零行为变化）；epoch 单测覆盖 pin/unpin/oldest/并发 pin；allocator 延迟测试覆盖"延迟→携带→恢复提升" |
| intentionally incomplete | 读者仍持读锁；写者仍持写锁；无任何并发行为变化 |
| commit | `phase a: add epoch registry and conditional page promotion` |

**phase b：读者切 epoch pin，写者切 Mutex（真正读写不阻塞）**

| 字段 | 内容 |
| --- | --- |
| objective | 读者不再取读锁；写者不再被读者阻塞，读者不再被写者阻塞 |
| changes | `writer_lock` 改 `Mutex<()>`，五个写路径 `write()` → `lock()`；`view`/`buckets_internal` 去掉 `read()`，改为 pin/unpin；`ReadOnlyTxn._guard` 类型改为 epoch guard；**改写依赖读锁互斥语义的现有测试**：`tests/isolation_tests.rs:8 test_view_isolation_blocks_writer` 改为断言写者不被读者阻塞（写者在读者睡眠期间完成、读者仍见固定旧快照——隔离由快照而非互斥保证）；`tests/open_reuse_tests.rs:8 test_reopen_same_path_shares_writer_lock` 与 `:104 test_same_path_open_is_instance_reuse_not_true_reopen` 的阻塞断言改为实例共享的替代证据（跨句柄提交可见性，不再依赖写者被读者阻塞）；`tests/concurrency_tests.rs:16` 注释同步更新；新增 `tests/read_write_nonblocking.rs`：快照一致性（读者跨写者提交仍见固定快照）、页复用竞态（读者 pin 旧代 + 写者提交复用其页 → 读者读旧值、unpin 后复用发生）、**扫描-发布窗口测试**（`commit_roots_with_alloc` 的 oldest 扫描与提升之间加 `#[cfg(test)]` 同步钩子，让新读者在该窗口 pin 并遍历，写者完成发布后读者仍读到一致快照）、长 view 空间有界（扩展 `tests/leak_safety_tests.rs`）；扩展 `tests/concurrency_tests.rs` 并发读写压力；>256 并发 view 压力测试覆盖注册表溢出路径 |
| focused checks | 复用竞态测试确定性通过（这是本阶段唯一新暴露的正确性窗口）；`cargo test --all-features` 全绿；`cargo test --release` 全绿 |
| intentionally incomplete | fuzz 模型未扩展；bench 未加；文档未更新 |
| commit | `phase b: switch readers to epoch pinning and writers to mutex` |

**phase c：验证闭环与文档**

| 字段 | 内容 |
| --- | --- |
| objective | 集成验证 + 设计源同步 |
| changes | 扩展 `fuzz/fuzz_targets/concurrent_snapshot_model.rs`（读者跨写者提交持 view）；`benches/btree_bench.rs` 增加并发读写 bench（写者持续提交下读者延迟，验证读不阻塞写）与 **pin/unpin 扩展性 bench（1/8/64/256 读者，对比 `RwLock::read()`，验证分片空闲表不引入超线性劣化）**；更新 `docs/design.md` §5.1/§7.3 与 `CHANGELOG.md`；按 §3.3 跑完整验收门 |
| focused checks | bench 对比改造前后：并发读延迟在写者持续提交下不劣化；fuzz 模型无 crash |
| intentionally incomplete | 无 |
| commit | `phase c: complete concurrency validation and design docs` |

### 3.2 聚焦验证

| 设计主张（§2） | 聚焦检查 |
| --- | --- |
| 提升条件 `oldest ≥ current` 正确且保守 | phase a allocator 延迟测试：手动 pin 旧 epoch → 提交 → retired 携带；unpin → 提交 → 提升 |
| 读者跨写者提交仍见固定快照 | phase b 快照一致性测试：读者 view 中写者多次提交，读者遍历结果与 view 起点一致 |
| 页复用不破坏在途读者 | phase b 复用竞态测试：确定性构造"读者 pin 旧代 + 写者提交复用其页"，读者读旧值 |
| 扫描-发布窗口无 TOCTOU（§2.2 线性化论证） | phase b 窗口测试：`#[cfg(test)]` 钩子在 oldest 扫描后、发布前停住写者，注入新读者 pin 并遍历，写者完成发布后读者读到一致快照 |
| 延迟页持久化且恢复后可提升 | 扩展 `tests/crash_safety_tests.rs`：延迟场景下崩溃 → reopen → 数据完整、页可复用 |
| 写者不被读者阻塞 | phase c bench：写者持续提交下读者延迟不随读者数劣化 |
| 热路径 pin/unpin 无分配（shard 提示走 RocksDB 模式 TLS 向量，非 pin 值/slot 索引） | phase c bench：并发读写下 view 延迟与改造前同量级（对比 `benches/btree_bench.rs` 新增基准） |
| slot 编码闭合（epoch 0 读者不被误判空闲） | phase a 回归测试：新建 Store → epoch 0 读者 → 首次提交 → 页不被复用 |
| 分片空闲表不引入超线性争用 | phase c bench：1/8/64/256 读者下 pin/unpin 对比 `RwLock::read()` |

### 3.3 最终验收

本仓库无 `extra_check` feature 与 `prod_test.sh`，按 `AGENTS.md` 默认矩阵（替代框架通用门）：

```bash
# Release，连续运行 1 次，每次 180 秒内全绿
timeout 180 cargo test --all-features --release -- --nocapture

# Debug，连续运行 1 次，每次 360 秒内全绿
timeout 360 cargo test --all-features -- --nocapture

# Nightly AddressSanitizer，运行 1 次全绿
RUST_BACKTRACE=1 RUSTFLAGS=-Zsanitizer=address cargo +nightly test -Zbuild-std --target x86_64-unknown-linux-gnu -- --nocapture

# Fuzz regression，运行 1 次完整通过（4 个 target × 600s）
./scripts/fuzz_regression.sh
```

任意非零退出、timeout（124）、panic、sanitizer report 或 fuzz crash 即失败；修复后从对应完整轮次重跑，不拼接失败前后的成功轮次。记录最终 commit id、命令、起止时间、退出码与首个失败产物路径。

## 4. 实现者注意事项

- **不变量**：页在"仍可能被在途读者引用"期间不得复用；提升只延迟、不提前；**延迟页不被 fallback 引用**（正常 retired 页被 fallback 引用是 quarantine 既定语义）；正常发布顺序（slot → sync → shared.update → epoch+1）不可重排，写者采纳也必须保持 shared.update → epoch+1。
- **代码触点**：`src/epoch.rs`（新）、`src/store.rs`（`Store.epoch`、`commit_roots_with_alloc` 提升分支、`publish_generation` epoch 前进）、`src/lib.rs`（`writer_lock` 类型、`view`/`buckets_internal` pin、`ReadOnlyTxn._guard`）。
- **读者注册表实现（按热路径成本排序）**：
  - 推荐：guard 携带 slot 的注册表（完整规格）：
    - 结构：`slots: [AtomicU64; 256]`（**0=空闲，否则存 epoch+1**，见下；每个 slot 是独立状态机，pin 直接在 slot 上 CAS，无共享空闲表头、无 tag、**结构性无 ABA**）+ `overflow: Mutex<Vec<(u64, u64)>>`（(token, epoch) 对，token 取自全局 `AtomicU64` 递增计数）。
    - **slot 编码（闭合 epoch 0 初值缺口）**：slot 存 `epoch + 1`，0 保留为空闲——epoch 计数器从 0 起步时，epoch 0 的活动读者存 1，不会被 `oldest()` 误判为空闲。`oldest()` 对非零 slot 值减 1 还原 epoch。溢出路径存 (token, epoch) 对，无编码问题（Vec 中存在即活动）。
    - pin（普通路径，有空闲槽）：`e = current.load(Acquire)`；`s = thread_shard(hint_id)`（探测起点，见下）；从 shard s 起按序探测**全部** 256 槽（保证 ≤256 并发 view 时任何空闲槽都被找到，无分配承诺精确成立）：`slot.load(Relaxed) == 0` 时 `CAS(0, e + 1, AcqRel)`，成功即独占该槽；guard 持 idx。无分配。
    - pin（探测全部槽均非 0，即 >256 并发 view）：`tok = counter.fetch_add(1)`；`overflow.lock().push((tok, e))`；guard 持 tok。**分配与可能短暂阻塞仅发生在此路径**。
    - unpin：普通路径 `slots[idx].store(0, Relaxed)`（槽归 pinner 独占，新 pin 只能在读到 0 后 CAS 它，无需归还操作）；溢出路径 `overflow.lock().retain(|(t, _)| *t != tok)`（O(溢出数)，通常 0）。
    - `oldest()`：扫描 256 slot（无锁原子读，非零值减 1 取最小）+ 短取 `overflow.lock()` 扫描；空则 current。每 commit 一次。
    - 失败行为：溢出路径在内存耗尽时经 Vec push panic，panic=abort 下进程终止（既有故障边界），无静默失败；"无阻塞"承诺仅在 ≤256 并发下成立。
    - 泄漏诊断：`#[cfg(debug_assertions)]` 与测试钩子暴露活动 pin 计数与 oldest 值；API 文档注明长生命周期 view 的后果（§2.6 资源边界）。
    - 测试：epoch 0 活动读者 → 首次提交 → 页不被复用的回归测试（闭合 slot 编码缺口）；>256 并发 view 压力测试覆盖溢出路径 pin/unpin/oldest 正确性。
  - **探测起点 shard 缓存（RocksDB `ThreadLocalPtr` 模式）**：`thread_shard()` 每次 pin 做线程哈希（~9ns）是热路径可测成本。按 RocksDB 的 per-thread-per-instance 模式缓存：全局分配器给每个 `EpochRegistry` 一个唯一 hint id（`AtomicU32` 递增 + 空闲 id 回收），每线程持一个 TLS `Vec<u64>` 按 hint id 索引存 shard。快速路径 = TLS 向量一次索引（~1-2ns）；首次 pin 计算线程哈希并写入。**为什么这里可以用 TLS**：shard 是纯探测起点提示——不是 pin 值（无跨实例覆盖风险）、不是 slot 索引（无资源所有权、无泄漏），线程退出无需清理，回收的 id 只会读到陈旧但无害的提示；实例隔离由 hint id 保证（注册表 A 的提示不会覆盖 B 的）。
  - **为什么不用 TLS 存 pin 值或 slot 索引**：多实例共享 TLS 变量是真实风险——若 TLS 存 pin 值，实例 A 的 pin 会被实例 B 覆盖（不安全）；若只存 slot 索引，需要全局索引空间（每个实例的数组按全局线程索引，线程退出遗留索引泄漏、实例间隐式耦合）。guard 携带索引天然规避：slot 随 guard 生命周期独占，unpin 即归还，无泄漏、无共享、实例间完全隔离（每个 `Store` 独立注册表）。
  - 备选：分片 `Mutex<BTreeMap<epoch, count>>`：无 TLS、计数精确，但 pin/unpin 有分片锁争用 + BTreeMap 插入分配（~100-300ns），热路径可测出，仅作原型用。
  - 不采用：counter-window（`counters[e % WINDOW]` 原子计数）：无锁但 slot 内多代混叠导致 min 陈旧，最老读者退出后提升可能被永久延迟（文件无界增长），除非每次 unpin 全量重扫（O(WINDOW)），得不偿失。
  - 不采用：crossbeam-epoch 依赖：内部使用 TLS 且 epoch 是进程全局的，与 per-Store 语义不匹配（实例间会互相阻塞页复用），与"无 TLS"约束冲突。
- **快照可见性同步依赖**：读者 `shared_snapshot()` 与写者 `shared.update` 经 `SharedMeta.state: RwLock`（`src/store.rs:522-539`）排序——读者取读锁、写者发布时取写锁；epoch 计数器的 Acquire/Release 是附加排序。实现时在代码注释中明确这条依赖，不依赖简写推理。
- **测试钩子**：epoch 注册表提供 `#[cfg(test)]` 直接 pin/unpin 的入口，供 allocator 延迟测试注入"假读者"。
- **观察性**：不新增固定基数指标；`#[cfg(test)]` 下可暴露 `oldest_active_reader_epoch` 与延迟提升次数供断言。
- **性能预算**：≤256 并发 view 时 pin/unpin = 槽探测（Relaxed 读 + 一次槽上 CAS，典型 1-2 次）+ TLS 向量一次索引（shard 提示，RocksDB 模式），无分配；>256 并发走溢出路径（允许分配与短暂阻塞，罕见）。提交时 oldest 计算 ≤ 一次 O(256) 原子读扫描 + 溢出表短扫描；不得在常见热路径引入分配或 fsync。高并发下的实际争用由 phase c 的 1/8/64/256 读者 bench 验证。
