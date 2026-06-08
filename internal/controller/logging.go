/*
Copyright 2025 Valkey Contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// logSwallowedError logs err at a level appropriate to its kind.
// Use it at sites where the reconciler intentionally catches an error
// from a helper call (so the next reconcile can retry) instead of
// returning it. Without this, the natural reflex - log.V(1).Info(...)
// - hides RBAC and other permanent failures below default verbosity,
// which is how we shipped two RBAC-shaped bugs in one branch without
// noticing them in the operator logs.
//
// Classification:
//
//   - Forbidden / Unauthorized: log.Error. These are permanent
//     misconfigurations (missing RBAC verbs, wrong ServiceAccount,
//     ACL gap) that no number of reconciles will fix. Always visible.
//
//   - NotFound: Info (V(0)). Could be normal during initial bring-up
//     (resource not yet created) or a real bug (resource was deleted
//     out from under us). Visible at default verbosity so the user
//     can see WHICH resource is missing.
//
//   - Anything else: V(1) Info. Assumed transient (network blip,
//     optimistic concurrency, sentinel mid-restart). Suppressed at
//     default verbosity to keep the log readable.
//
// Callers do not need to add "err" to keysAndValues themselves; this
// helper appends it.
func logSwallowedError(log logr.Logger, err error, msg string, keysAndValues ...any) {
	if err == nil {
		return
	}
	switch {
	case apierrors.IsForbidden(err), apierrors.IsUnauthorized(err):
		// Permanent misconfiguration. ALWAYS visible. The RBAC pattern
		// we just fixed (executor's r.Delete(pod) lacking pods/delete
		// verb) silently failed at V(1) for an entire iteration cycle
		// before anyone noticed - this is the price tag for hiding
		// it.
		log.Error(err, msg+": permission denied (RBAC/ACL?)", keysAndValues...)
	case apierrors.IsNotFound(err):
		// Could be initial-bring-up timing or a real teardown bug.
		// Loud enough to be seen but not flagged as Error.
		kv := append(keysAndValues, "err", err)
		log.Info(msg+": resource not found", kv...)
	default:
		// Assumed transient.
		kv := append(keysAndValues, "err", err)
		log.V(1).Info(msg, kv...)
	}
}
