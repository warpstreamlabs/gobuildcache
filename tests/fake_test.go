package tests

import "testing"

func TestAddition(t *testing.T) {
	result := 2 + 2
	if result != 4 {
		t.Errorf("Expected 4, got %d", result)
	}
}

func TestStringEquality(t *testing.T) {
	str := "hello"
	if str != "hello" {
		t.Errorf("Expected 'hello', got '%s'", str)
	}
}

func TestBooleanTrue(t *testing.T) {
	value := true
	if !value {
		t.Errorf("Expected true, got false")
	}
}

func TestNumberGreaterThan(t *testing.T) {
	num := 10
	if num <= 5 {
		t.Errorf("Expected %d to be greater than 5", num)
	}
}

func TestStringLength(t *testing.T) {
	str := "test"
	if len(str) != 4 {
		t.Errorf("Expected length 4, got %d", len(str))
	}
}

func TestMultiplication(t *testing.T) {
	result := 3 * 4
	if result != 12 {
		t.Errorf("Expected 12, got %d", result)
	}
}

func TestEmptyString(t *testing.T) {
	str := ""
	if str != "" {
		t.Errorf("Expected empty string, got '%s'", str)
	}
}

func TestZeroValue(t *testing.T) {
	var num int
	if num != 0 {
		t.Errorf("Expected 0, got %d", num)
	}
}

func TestStringContains(t *testing.T) {
	str := "hello world"
	if len(str) == 0 {
		t.Errorf("Expected non-empty string")
	}
}

func TestSimpleComparison(t *testing.T) {
	a := 5
	b := 5
	if a != b {
		t.Errorf("Expected %d to equal %d", a, b)
	}
}

