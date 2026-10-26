package mastersync

import "testing"

func TestValidate(t *testing.T) {
	if _, err := Validate(nil); err == nil {
		t.Error("empty list must fail")
	}
	if _, err := Validate([]Customer{{ID: "a"}, {ID: ""}}); err == nil {
		t.Error("missing id must fail")
	}
	if _, err := Validate([]Customer{{ID: "a"}, {ID: "a"}}); err == nil {
		t.Error("duplicate id must fail")
	}
	if got, err := Validate([]Customer{{ID: "a"}, {ID: "b"}}); err != nil || len(got) != 2 {
		t.Errorf("valid list: %v, %v", got, err)
	}
}

func TestHashKeyMatchesXray(t *testing.T) {
	// printf demo-key | sha256sum — the same hash x-ray's seed.sh and store use.
	const want = "c48a01f49fd0f2cc404bc3cbbc80e91457a3d41bb429a695243de4c61794155c"
	if got := HashKey("demo-key"); got != want {
		t.Fatalf("HashKey = %s, want %s", got, want)
	}
}
