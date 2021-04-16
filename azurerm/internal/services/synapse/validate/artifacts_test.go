package validate

import "testing"

func TestValidatePipelineAndTriggerName(t *testing.T) {
	validNames := []string{
		"validname",
		"valid02name",
		"validName1",
	}
	for _, v := range validNames {
		_, errors := PipelineAndTriggerName()(v, "valid")
		if len(errors) != 0 {
			t.Fatalf("%q should be an invalid Synapse Pipeline or Trigger Name: %q", v, errors)
		}
	}

	invalidNames := []string{
		"invalid.",
		":@£",
		">invalid",
		"invalid&name",
	}
	for _, v := range invalidNames {
		_, errors := PipelineAndTriggerName()(v, "invalid")
		if len(errors) == 0 {
			t.Fatalf("%q should be an invalid Synapse Pipeline or Trigger Name", v)
		}
	}
}
