package harness

import (
	"reflect"
	"testing"
)

func TestMergeSandbox(t *testing.T) {
	idA := "local:t1"
	idB := "docker:t1"

	// new 为 nil → 保留 existing。
	if got, err := MergeSandbox(&SandboxState{SandboxID: &idA}, nil); err != nil || got == nil || *got.SandboxID != idA {
		t.Fatalf("new nil: got %+v err %v", got, err)
	}
	// existing 为 nil → 返回 new。
	if got, err := MergeSandbox(nil, &SandboxState{SandboxID: &idA}); err != nil || got == nil || *got.SandboxID != idA {
		t.Fatalf("existing nil: got %+v err %v", got, err)
	}
	// 同 id 幂等:返回 existing。
	existing := &SandboxState{SandboxID: &idA}
	got, err := MergeSandbox(existing, &SandboxState{SandboxID: &idA})
	if err != nil || got != existing {
		t.Fatalf("same id: got %+v (want existing pointer) err %v", got, err)
	}
	// 两者 sandbox_id 都为 nil → 幂等。
	got, err = MergeSandbox(&SandboxState{}, &SandboxState{})
	if err != nil || got == nil || got.SandboxID != nil {
		t.Fatalf("both nil id: got %+v err %v", got, err)
	}
	// 不同 id → fail-closed 报错。
	if _, err := MergeSandbox(&SandboxState{SandboxID: &idA}, &SandboxState{SandboxID: &idB}); err == nil {
		t.Fatal("conflicting ids should error")
	}
}

func TestMergeArtifacts(t *testing.T) {
	// existing nil → new 原样返回(Python `new or []`,首次写入不 dedup)。
	if got := MergeArtifacts(nil, []string{"b", "a", "b"}); !reflect.DeepEqual(got, []string{"b", "a", "b"}) {
		t.Fatalf("existing nil: got %v", got)
	}
	// new nil → existing 原样。
	if got := MergeArtifacts([]string{"a", "b"}, nil); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("new nil: got %v", got)
	}
	// 合并去重保序。
	if got := MergeArtifacts([]string{"a", "b"}, []string{"b", "c"}); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Fatalf("merge: got %v", got)
	}
	// existing nil 且 new nil → 空。
	if got := MergeArtifacts(nil, nil); got == nil || len(got) != 0 {
		t.Fatalf("both nil: got %v", got)
	}
}

func TestMergeViewedImages(t *testing.T) {
	imgA := ViewedImageData{Base64: "aaa", MimeType: "image/png"}
	imgB := ViewedImageData{Base64: "bbb", MimeType: "image/jpeg"}

	// existing nil → new。
	if got := MergeViewedImages(nil, map[string]ViewedImageData{"a": imgA}); !reflect.DeepEqual(got, map[string]ViewedImageData{"a": imgA}) {
		t.Fatalf("existing nil: got %v", got)
	}
	// new nil → existing。
	existing := map[string]ViewedImageData{"a": imgA}
	if got := MergeViewedImages(existing, nil); !reflect.DeepEqual(got, existing) {
		t.Fatalf("new nil: got %v", got)
	}
	// new 空 map → 显式清空。
	if got := MergeViewedImages(existing, map[string]ViewedImageData{}); len(got) != 0 {
		t.Fatalf("empty new should clear: got %v", got)
	}
	// 合并,new 覆盖同名 key。
	merged := MergeViewedImages(map[string]ViewedImageData{"a": imgA}, map[string]ViewedImageData{"b": imgB, "a": imgB})
	if !reflect.DeepEqual(merged["a"], imgB) || !reflect.DeepEqual(merged["b"], imgB) {
		t.Fatalf("merge override: got %v", merged)
	}
	// existing nil 且 new nil → 空 map(非 nil)。
	if got := MergeViewedImages(nil, nil); got == nil || len(got) != 0 {
		t.Fatalf("both nil: got %v", got)
	}
}

func TestMergeTodos(t *testing.T) {
	// new nil → existing。
	if got := MergeTodos([]string{"a"}, nil); !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatalf("new nil: got %v", got)
	}
	// new 非 nil(空)→ 显式清空,胜出。
	if got := MergeTodos([]string{"a"}, []string{}); got == nil || len(got) != 0 {
		t.Fatalf("empty new should win: got %v", got)
	}
	// new 非 nil → new。
	if got := MergeTodos([]string{"a"}, []string{"b"}); !reflect.DeepEqual(got, []string{"b"}) {
		t.Fatalf("new wins: got %v", got)
	}
	// 泛型对任意元素类型有效。
	if got := MergeTodos([]int{1}, []int{2, 3}); !reflect.DeepEqual(got, []int{2, 3}) {
		t.Fatalf("generic: got %v", got)
	}
}

func TestMergePromoted(t *testing.T) {
	// new nil → existing。
	existing := &PromotedTools{CatalogHash: "h1", Names: []string{"a"}}
	if got := MergePromoted(existing, nil); got != existing {
		t.Fatalf("new nil: got %v", got)
	}
	// new names 空 → existing。
	if got := MergePromoted(existing, &PromotedTools{CatalogHash: "h1", Names: nil}); got != existing {
		t.Fatalf("empty new: got %v", got)
	}
	// existing nil → new(去重)。
	got := MergePromoted(nil, &PromotedTools{CatalogHash: "h1", Names: []string{"a", "a"}})
	if !reflect.DeepEqual(got, &PromotedTools{CatalogHash: "h1", Names: []string{"a"}}) {
		t.Fatalf("existing nil: got %+v", got)
	}
	// hash 变 → 整体替换,丢弃旧 names。
	got = MergePromoted(existing, &PromotedTools{CatalogHash: "h2", Names: []string{"x"}})
	if !reflect.DeepEqual(got, &PromotedTools{CatalogHash: "h2", Names: []string{"x"}}) {
		t.Fatalf("hash change: got %+v", got)
	}
	// 同 hash → 并集去重保序。
	got = MergePromoted(existing, &PromotedTools{CatalogHash: "h1", Names: []string{"b", "a"}})
	if !reflect.DeepEqual(got, &PromotedTools{CatalogHash: "h1", Names: []string{"a", "b"}}) {
		t.Fatalf("same hash union: got %+v", got)
	}
}
