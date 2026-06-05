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
	"context"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

// reconcileACL provisions the per-Valkey ACL Secret (containing
// `users.acl`) plus the per-Valkey system passwords Secret. It also
// projects the `_sentinel` user's credentials into a dedicated
// `<name>-sentinel-auth` Secret that selecting ValkeySentinels read.
func (r *ValkeyReconciler) reconcileACL(ctx context.Context, valkey *valkeyiov1alpha1.Valkey) error {
	pwSecret, err := r.upsertValkeySystemPasswordSecret(ctx, valkey)
	if err != nil {
		return err
	}
	if err := r.upsertValkeyACLSecret(ctx, valkey, pwSecret); err != nil {
		return err
	}
	return r.projectSentinelAuthSecret(ctx, valkey, pwSecret)
}

// upsertValkeySystemPasswordSecret ensures a Secret with one generated
// password per system user. Uses the existing `getSystemPasswordSecretName`
// helper for compatibility with the wider tooling.
func (r *ValkeyReconciler) upsertValkeySystemPasswordSecret(ctx context.Context, valkey *valkeyiov1alpha1.Valkey) (*corev1.Secret, error) {
	users := append([]string{}, systemUsers...)
	if valkey.Spec.Replicas > 0 {
		users = append(users, sentinelUser)
	}
	sort.Strings(users)

	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name:      getSystemPasswordSecretName(valkey.Name),
		Namespace: valkey.Namespace,
	}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}
		secret.Labels = valkeyLabels(valkey)
		secret.Type = corev1.SecretTypeOpaque
		for _, user := range users {
			if _, ok := secret.Data[user]; !ok {
				pw, err := generatePassword(passwordLength)
				if err != nil {
					return err
				}
				secret.Data[user] = pw
			}
		}
		return controllerutil.SetControllerReference(valkey, secret, r.Scheme)
	})
	return secret, err
}

// upsertValkeyACLSecret renders the ACL string for every system user
// (and any user-defined Spec.Users) and stores it as `users.acl`.
func (r *ValkeyReconciler) upsertValkeyACLSecret(ctx context.Context, valkey *valkeyiov1alpha1.Valkey, pwSecret *corev1.Secret) error {
	var b strings.Builder
	for _, u := range systemUsers {
		fmt.Fprintf(&b, "user %s on >%s %s\n", u, string(pwSecret.Data[u]), systemUsersAcls[u])
	}
	if valkey.Spec.Replicas > 0 {
		fmt.Fprintf(&b, "user %s on >%s %s\n", sentinelUser, string(pwSecret.Data[sentinelUser]), sentinelUserACL)
	}

	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name:      getInternalSecretName(valkey.Name),
		Namespace: valkey.Namespace,
	}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		secret.Labels = valkeyLabels(valkey)
		secret.Type = corev1.SecretTypeOpaque
		secret.Data = map[string][]byte{aclFilename: []byte(b.String())}
		return controllerutil.SetControllerReference(valkey, secret, r.Scheme)
	})
	return err
}

// projectSentinelAuthSecret writes a small `<name>-sentinel-auth` Secret
// containing only the `_sentinel` user credentials. Selecting
// ValkeySentinels read this Secret to bake `sentinel auth-user/auth-pass`
// directly into their rendered sentinel.conf (the password is substituted
// at pod start from the per-sentinel aggregated auth Secret).
// Skipped when the Valkey is standalone (no sentinel monitoring possible).
func (r *ValkeyReconciler) projectSentinelAuthSecret(ctx context.Context, valkey *valkeyiov1alpha1.Valkey, pwSecret *corev1.Secret) error {
	if valkey.Spec.Replicas == 0 {
		// Remove if it exists, since standalone has no replication to monitor.
		old := &corev1.Secret{}
		if err := r.Get(ctx, client.ObjectKey{
			Name: valkeySentinelAuthSecretName(valkey), Namespace: valkey.Namespace,
		}, old); err == nil {
			_ = r.Delete(ctx, old)
		}
		return nil
	}
	pw, ok := pwSecret.Data[sentinelUser]
	if !ok {
		return fmt.Errorf("system password secret missing %s entry", sentinelUser)
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name:      valkeySentinelAuthSecretName(valkey),
		Namespace: valkey.Namespace,
	}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		secret.Labels = valkeyLabels(valkey)
		secret.Type = corev1.SecretTypeOpaque
		secret.Data = map[string][]byte{
			"username": []byte(sentinelUser),
			"password": pw,
		}
		return controllerutil.SetControllerReference(valkey, secret, r.Scheme)
	})
	return err
}
