// 状态管理与 Reducer —— 对应 deer-flow agents/thread_state.py(整文件)。
//
// 源码行号映射:
//   - SandboxState      : thread_state.py:6-7
//   - ThreadDataState   : thread_state.py:10-13
//   - ViewedImageData   : thread_state.py:16-18
//   - merge_sandbox     : thread_state.py:21-39
//   - merge_artifacts   : thread_state.py:45-52
//   - merge_viewed_images: thread_state.py:55-69
//   - merge_todos       : thread_state.py:72-82
//   - PromotedTools     : thread_state.py:85-87
//   - merge_promoted    : thread_state.py:90-108
//
// 核心设计(LangGraph 为什么需要自定义 reducer):
//
//	LangGraph 默认「覆盖」写 state,但同一字段可能在同一 graph step 被多个节点/中间件
//	并发写入,「覆盖」或「简单合并」无法表达正确语义,所以要为每个字段显式写合并规则:
//	  - merge_sandbox      幂等写入 + fail-closed(不同 sandbox_id 说明生命周期 bug,报错而非静默选一个)。
//	  - merge_artifacts    追加 + 去重保序(同一文件可能被多次生成)。
//	  - merge_viewed_images 关键区分「未更新(None)」与「显式清空(空 dict)」。
//	  - merge_todos        最后非 nil 胜出(空 list 也是「显式更新」,覆盖旧值)。
//	  - merge_promoted     按 catalog_hash 分区(hash 变则整体替换,防目录漂移)。
//
// Go 与 Python 的关键差异:
//   - Go 没有 LangGraph 的 reducer 机制,把「并发合并规则显式化」为纯函数即可。
//   - Python 的 None vs {} 区分,Go 用「nil 指针 / nil map」表达:
//   - 字段级 None(如 sandbox/promoted 整个字段未写)→ nil *T 指针;
//   - 字典级 None(未更新)→ nil map;空字典(显式清空)→ 非 nil 的 len==0 map。
//   - NotRequired[str | None] 的可空字段(sandbox_id / 三个路径)用 *string 表达,
//     nil = None/未设置。
package harness

import "fmt"

// SandboxState 对应 thread_state.py:6 的 SandboxState(sandbox_id: NotRequired[str | None])。
// SandboxID 用 *string 表达可空:nil = None(沙盒未初始化/未设置)。
type SandboxState struct {
	SandboxID *string
}

// ThreadDataState 对应 thread_state.py:10 的 ThreadDataState
// (workspace_path / uploads_path / outputs_path 三个 NotRequired[str | None] 路径)。
// 三个字段均为 *string,nil = None/未设置。
type ThreadDataState struct {
	WorkspacePath *string
	UploadsPath   *string
	OutputsPath   *string
}

// ViewedImageData 对应 thread_state.py:16 的 ViewedImageData(base64: str, mime_type: str)。
// image_path -> ViewedImageData。mime_type 是生产细节:view_image_middleware 据此
// 决定如何把 base64 回灌给模型,不能砍掉。
type ViewedImageData struct {
	Base64   string
	MimeType string
}

// PromotedTools 对应 thread_state.py:85 的 PromotedTools(catalog_hash: str, names: list[str])。
// 两个字段都非空(catalog_hash 是目录版本戳,names 是提升的工具名)。
type PromotedTools struct {
	CatalogHash string
	Names       []string
}

// StrPtr 便捷构造 *string,表达「非 None」的字符串(便于调用方与测试构造 SandboxState)。
func StrPtr(s string) *string { return &s }

// MergeSandbox 幂等合并沙盒状态(对应 merge_sandbox:21)。
//
// 语义:
//   - new 为 nil(节点没碰 sandbox)→ 保留 existing。
//   - existing 为 nil(首次写入)→ 返回 new。
//   - 两者都存在且 sandbox_id 相等(含同为 None)→ 幂等,返回 existing。
//   - 两者 sandbox_id 不同 → fail-closed:报错而不是静默选一个。
//
// 多个沙盒工具在同一步懒初始化、写同一个 sandbox_id 时这是幂等的;出现两个不同 id
// 说明生命周期/隔离出了 bug。
func MergeSandbox(existing, new *SandboxState) (*SandboxState, error) {
	if new == nil {
		return existing, nil
	}
	if existing == nil {
		return new, nil
	}
	if stringPtrEqual(existing.SandboxID, new.SandboxID) {
		return existing, nil
	}
	return nil, fmt.Errorf("Conflicting sandbox state updates: %s != %s",
		stringPtrRepr(existing.SandboxID), stringPtrRepr(new.SandboxID))
}

// stringPtrEqual 比较两个 *string:同为 nil 判等(对应 Python 里 None == None)。
func stringPtrEqual(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// stringPtrRepr 复现 Python 的 {id!r}:None -> "None",字符串 -> 'x'(单引号)。
func stringPtrRepr(p *string) string {
	if p == nil {
		return "None"
	}
	return fmt.Sprintf("'%s'", *p)
}

// MergeArtifacts 合并去重(保序)。对应 merge_artifacts:45。
//
//   - existing 为 nil → 返回 new or [](Python 首次写入不 dedup,原样返回 new)。
//   - new 为 nil → 返回 existing。
//   - 两者都有 → existing + new 拼接后按 dict.fromkeys 语义保序去重。
//
// 注意:Python 里 dedup 只在「两者都非 None」的合并分支发生;首次写入(existing=None)
// 返回 new 原样,不 dedup —— 忠实复现这个细节。
func MergeArtifacts(existing, new []string) []string {
	if existing == nil {
		if new == nil {
			return []string{}
		}
		return new
	}
	if new == nil {
		return existing
	}
	return dedupeStrings(append(append([]string{}, existing...), new...))
}

// MergeViewedImages 合并图片字典。对应 merge_viewed_images:55。
//
// 关键语义(nil vs 空 map,对应 Python 的 None vs {}):
//   - existing 为 nil(未更新)→ 返回 new(可能为空)。
//   - new 为 nil(节点没碰)→ 返回 existing。
//   - new 为非 nil 的 len==0(显式清空)→ 返回空 map(清空已有图片)。
//     这是「显式清空」信号:中间件处理完图片后用空 dict 清空 viewed_images。
//   - 两者都有 → 合并,new 的同名 key 覆盖 existing。
func MergeViewedImages(existing, new map[string]ViewedImageData) map[string]ViewedImageData {
	if existing == nil {
		if new == nil {
			return map[string]ViewedImageData{}
		}
		return new
	}
	if new == nil {
		return existing
	}
	if len(new) == 0 {
		return map[string]ViewedImageData{} // 显式清空
	}
	out := make(map[string]ViewedImageData, len(existing)+len(new))
	for k, v := range existing {
		out[k] = v
	}
	for k, v := range new {
		out[k] = v
	}
	return out
}

// MergeTodos 合并 todos 列表 —— 最后非 nil 胜出。对应 merge_todos:72。
//
// Python 的 merge_todos 是 list | None(元素类型无关),所以用 Go 泛型复现类型无关语义:
//   - new 为 nil(节点没碰 todos)→ 保留 existing。
//   - new 非 nil(哪怕空 slice)→ 显式更新,胜出。
//
// 注意:Go 里 nil slice 与空 slice 可区分(对应 Python 的 None vs []):
// MergeTodos[T](nil, []T{}) 返回非 nil 的空 slice(显式清空)。
func MergeTodos[T any](existing, new []T) []T {
	if new == nil {
		return existing
	}
	return new
}

// MergePromoted 按 catalog_hash 分区合并提升记录。对应 merge_promoted:90。
//
//   - new 为 nil 或 names 为空(节点没碰 promotion)→ 保留 existing。
//     (Python 用 `if not new:` 判空:None 或空 dict 都算「没碰」。)
//   - existing 为 nil 或 catalog_hash 变化 → 整体替换,丢弃旧 names。
//     防目录漂移:持久化的裸名字可能指向换了目录后的另一个工具。
//   - catalog_hash 相同 → names 并集 + 去重保序。
func MergePromoted(existing, new *PromotedTools) *PromotedTools {
	if new == nil || len(new.Names) == 0 {
		return existing
	}
	if existing == nil || existing.CatalogHash != new.CatalogHash {
		return &PromotedTools{CatalogHash: new.CatalogHash, Names: dedupeStrings(new.Names)}
	}
	return &PromotedTools{
		CatalogHash: existing.CatalogHash,
		Names:       dedupeStrings(append(append([]string{}, existing.Names...), new.Names...)),
	}
}

// dedupeStrings 保序去重(对应 Python dict.fromkeys 的保序去重)。
func dedupeStrings(in []string) []string {
	if in == nil {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}
