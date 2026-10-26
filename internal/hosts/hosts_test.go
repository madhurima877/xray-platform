package hosts

import "testing"

func TestNormalize(t *testing.T) {
	cases := map[string]string{
		"https://WWW.Evil.Example/login?a=1": "evil.example",
		"evil.example":                       "evil.example",
		"www.evil.example/path":              "evil.example",
		"http://evil.example.:8080/":         "evil.example",
		"  phish.example  ":                  "phish.example",
		"":                                   "",
		"http://":                            "",
		"not a host":                         "",
		"::":                                 "",
		"localhost":                          "",
		"http://1.2.3.4/x":                   "1.2.3.4",
		"bad..example":                       "",
	}
	for in, want := range cases {
		if got := Normalize(in); got != want {
			t.Errorf("Normalize(%q) = %q, want %q", in, got, want)
		}
	}
}
