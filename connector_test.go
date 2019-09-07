package connector

import "testing"

const (
	success = "\u2713"
	failed  = "\u2717"
)

func TestConnectionOption(t *testing.T) {
	connectionOption := ConnectionOption{}
	t.Log("Test Connection Option; simple integer")
	{
		expectedTimeout := 15
		connectionOption.Add("timeout.value", expectedTimeout)

		timeout := 0
		err := connectionOption.Get("timeout.value", &timeout)
		if err != nil {
			t.Fatalf("%s expected error nil, got %s", failed, err.Error())
		}

		if timeout != expectedTimeout {
			t.Fatalf("%s expected timeout == %d", failed, expectedTimeout)
		}

		t.Logf("%s expected timeout == %d", success, timeout)
	}

	t.Log("Test Connection Option; simple struct")
	{
		expectedData := struct {
			Name string
			Rate float64
		}{
			"Test Name", 0.9,
		}

		outputData := struct {
			Name string
			Rate float64
		}{}

		connectionOption.Add("option.data", expectedData)
		err := connectionOption.Get("option.data", &outputData)
		if err != nil {
			t.Fatalf("%s expected error nil, got %s", failed, err.Error())
		}

		if outputData != expectedData {
			t.Fatalf("%s expected data == %v, got %v", failed, expectedData, outputData)
		}

		t.Logf("%s expected data == %v", success, expectedData)
	}
}
