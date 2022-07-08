package settings

// some settings which should not be changed after deployed (will corrput data or make
// the app incompatible )
// {
//   "xxx": 32,
// }
//
var Static = getStaticSettings()

type static struct {
	// test
	TestInt  uint64 `json:"test_int"`
	TestBool bool   `json:"test_bool"`
	TestStr  string `json:"test_str,omitempty"`
}

func getStaticSettings() static {
	s := defaultStaticSettings()
	overwriteSettingsWithFile(&s, "static-settings.json")
	return s
}

func defaultStaticSettings() static {
	return static{}
}
