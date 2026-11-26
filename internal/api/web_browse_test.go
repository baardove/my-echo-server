package api

import "testing"

func TestArtistFieldMatchesKey(t *testing.T) {
	cases := []struct {
		raw  string
		key  string
		want bool
	}{
		{"Calvin Harris", "calvin harris", true},
		{"Calvin Harris & Dua Lipa", "calvin harris", true},
		{"Calvin Harris & Dua Lipa", "dua lipa", true},
		{"Calvin Harris feat. Rihanna", "rihanna", true},
		{"Rihanna (ft. Calvin Harris)", "calvin harris", true},
		{"Calvin Harris", "alvin", false},
		{"Various Artists", "calvin harris", false},
		{"", "calvin harris", false},
	}
	for _, tc := range cases {
		if got := artistFieldMatchesKey(tc.raw, tc.key); got != tc.want {
			t.Errorf("artistFieldMatchesKey(%q, %q) = %v, want %v", tc.raw, tc.key, got, tc.want)
		}
	}
}
