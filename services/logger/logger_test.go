package logger

import (
	"bytes"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
)

func TestInit(t *testing.T) {
	// Test initialization with json format
	err := Init("info", "json", "stdout")
	if err != nil {
		t.Errorf("Init() error = %v", err)
	}

	log := Get()
	if log == nil {
		t.Error("Get() returned nil")
	}

	if log.GetLevel() != logrus.InfoLevel {
		t.Errorf("Expected log level info, got %v", log.GetLevel())
	}

	// Test initialization with text format
	err = Init("debug", "text", "stdout")
	if err != nil {
		t.Errorf("Init() error = %v", err)
	}

	log = Get()
	if log.GetLevel() != logrus.DebugLevel {
		t.Errorf("Expected log level debug, got %v", log.GetLevel())
	}
}

func TestInit_InvalidLevel(t *testing.T) {
	err := Init("invalid", "json", "stdout")
	if err == nil {
		t.Error("Expected error for invalid log level, got nil")
	}
}

func TestGet(t *testing.T) {
	// Test Get without initialization (fallback)
	log = nil
	logger := Get()
	if logger == nil {
		t.Error("Get() returned nil without initialization")
	}
}

func TestWithFields(t *testing.T) {
	Init("info", "text", "stdout")

	entry := WithFields(logrus.Fields{
		"key":    "value",
		"number": 42,
	})

	if entry == nil {
		t.Error("WithFields() returned nil")
	}
}

func TestInfo(t *testing.T) {
	// Capture output
	var buf bytes.Buffer
	log = logrus.New()
	log.SetOutput(&buf)
	log.SetLevel(logrus.InfoLevel)

	Info("test info message")

	output := buf.String()
	if !strings.Contains(output, "test info message") {
		t.Errorf("Expected output to contain 'test info message', got %s", output)
	}
}

func TestInfof(t *testing.T) {
	var buf bytes.Buffer
	log = logrus.New()
	log.SetOutput(&buf)
	log.SetLevel(logrus.InfoLevel)

	Infof("test %s message", "formatted")

	output := buf.String()
	if !strings.Contains(output, "test formatted message") {
		t.Errorf("Expected output to contain 'test formatted message', got %s", output)
	}
}

func TestError(t *testing.T) {
	var buf bytes.Buffer
	log = logrus.New()
	log.SetOutput(&buf)
	log.SetLevel(logrus.ErrorLevel)

	Error("test error message")

	output := buf.String()
	if !strings.Contains(output, "test error message") {
		t.Errorf("Expected output to contain 'test error message', got %s", output)
	}
}

func TestErrorf(t *testing.T) {
	var buf bytes.Buffer
	log = logrus.New()
	log.SetOutput(&buf)
	log.SetLevel(logrus.ErrorLevel)

	Errorf("test %s message", "formatted")

	output := buf.String()
	if !strings.Contains(output, "test formatted message") {
		t.Errorf("Expected output to contain 'test formatted message', got %s", output)
	}
}

func TestWarn(t *testing.T) {
	var buf bytes.Buffer
	log = logrus.New()
	log.SetOutput(&buf)
	log.SetLevel(logrus.WarnLevel)

	Warn("test warning message")

	output := buf.String()
	if !strings.Contains(output, "test warning message") {
		t.Errorf("Expected output to contain 'test warning message', got %s", output)
	}
}

func TestWarnf(t *testing.T) {
	var buf bytes.Buffer
	log = logrus.New()
	log.SetOutput(&buf)
	log.SetLevel(logrus.WarnLevel)

	Warnf("test %s message", "formatted")

	output := buf.String()
	if !strings.Contains(output, "test formatted message") {
		t.Errorf("Expected output to contain 'test formatted message', got %s", output)
	}
}

func TestDebug(t *testing.T) {
	var buf bytes.Buffer
	log = logrus.New()
	log.SetOutput(&buf)
	log.SetLevel(logrus.DebugLevel)

	Debug("test debug message")

	output := buf.String()
	if !strings.Contains(output, "test debug message") {
		t.Errorf("Expected output to contain 'test debug message', got %s", output)
	}
}

func TestDebugf(t *testing.T) {
	var buf bytes.Buffer
	log = logrus.New()
	log.SetOutput(&buf)
	log.SetLevel(logrus.DebugLevel)

	Debugf("test %s message", "formatted")

	output := buf.String()
	if !strings.Contains(output, "test formatted message") {
		t.Errorf("Expected output to contain 'test formatted message', got %s", output)
	}
}
