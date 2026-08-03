package videolease

import "testing"

func TestAdvisoryLockNameIsStableAndNamespaced(t *testing.T) {
	t.Parallel()

	first := advisoryLockName("4c106c1b-0859-4b16-a053-210f52264ead")
	second := advisoryLockName("4c106c1b-0859-4b16-a053-210f52264aee")
	if first == second {
		t.Fatal("different videos produced the same advisory lock name")
	}
	if first != advisoryLockNamespace+"4c106c1b-0859-4b16-a053-210f52264ead" {
		t.Fatalf("unexpected advisory lock name %q", first)
	}
}
