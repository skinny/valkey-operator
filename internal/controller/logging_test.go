/*
Copyright 2025 Valkey Contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"errors"
	"sync"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// fakeSink captures the level at which messages are logged so we can
// assert the helper's classification. logr.Logger forwards Error / Info
// to the sink with distinct methods; we just record which one was hit.
type fakeSink struct {
	mu      sync.Mutex
	infoMsg string
	infoLvl int
	errMsg  string
	errErr  error
}

func (f *fakeSink) Init(logr.RuntimeInfo)          {}
func (f *fakeSink) Enabled(int) bool               { return true }
func (f *fakeSink) WithName(string) logr.LogSink   { return f }
func (f *fakeSink) WithValues(...any) logr.LogSink { return f }
func (f *fakeSink) Info(level int, msg string, _ ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.infoMsg = msg
	f.infoLvl = level
}
func (f *fakeSink) Error(err error, msg string, _ ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errMsg = msg
	f.errErr = err
}

func TestLogSwallowedError(t *testing.T) {
	gr := schema.GroupResource{Resource: "pods"}

	t.Run("nil is a no-op", func(t *testing.T) {
		f := &fakeSink{}
		logSwallowedError(logr.New(f), nil, "x")
		assert.Empty(t, f.infoMsg)
		assert.Empty(t, f.errMsg)
	})

	t.Run("forbidden -> Error", func(t *testing.T) {
		f := &fakeSink{}
		err := apierrors.NewForbidden(gr, "p", errors.New("no delete"))
		logSwallowedError(logr.New(f), err, "rollout step failed")
		assert.Contains(t, f.errMsg, "permission denied")
		assert.Equal(t, err, f.errErr)
		assert.Empty(t, f.infoMsg)
	})

	t.Run("unauthorized -> Error", func(t *testing.T) {
		f := &fakeSink{}
		err := apierrors.NewUnauthorized("missing token")
		logSwallowedError(logr.New(f), err, "op")
		assert.Contains(t, f.errMsg, "permission denied")
	})

	t.Run("not found -> Info at V(0)", func(t *testing.T) {
		f := &fakeSink{}
		err := apierrors.NewNotFound(gr, "p")
		logSwallowedError(logr.New(f), err, "op")
		assert.Contains(t, f.infoMsg, "resource not found")
		assert.Equal(t, 0, f.infoLvl)
		assert.Empty(t, f.errMsg)
	})

	t.Run("generic error -> Info at V(1) (transient assumption)", func(t *testing.T) {
		f := &fakeSink{}
		err := errors.New("connection refused")
		logSwallowedError(logr.New(f), err, "op")
		assert.Equal(t, "op", f.infoMsg)
		assert.Equal(t, 1, f.infoLvl)
		assert.Empty(t, f.errMsg)
	})

	t.Run("server-timeout stays at V(1) (genuinely transient)", func(t *testing.T) {
		f := &fakeSink{}
		err := apierrors.NewServerTimeout(gr, "create", 1)
		logSwallowedError(logr.New(f), err, "op")
		assert.Equal(t, 1, f.infoLvl)
		assert.Empty(t, f.errMsg)
	})

	t.Run("status-error of unknown reason stays at V(1)", func(t *testing.T) {
		f := &fakeSink{}
		err := &apierrors.StatusError{ErrStatus: metav1.Status{
			Status: metav1.StatusFailure, Code: 500, Reason: metav1.StatusReasonInternalError,
		}}
		logSwallowedError(logr.New(f), err, "op")
		assert.Equal(t, 1, f.infoLvl)
		assert.Empty(t, f.errMsg)
	})
}
