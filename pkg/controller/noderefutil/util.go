/*
Copyright 2018 The Kubernetes Authors.

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

package noderefutil

import (
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/pkg/apis/cluster/v1alpha1"
)

func IsMachineAvailable(m *clusterv1.Machine, minReadySeconds int32, now metav1.Time) bool {
	if m == nil {
		return false
	}
	return readyForAtleast(m.Status.Conditions, minReadySeconds, now)
}

// IsNodeAvailable returns true if the node is ready and minReadySeconds have elapsed or is 0. False otherwise.
func IsNodeAvailable(node *corev1.Node, minReadySeconds int32, now metav1.Time) bool {
	if node == nil {
		return false
	}
	return readyForAtleast(node.Status.Conditions, minReadySeconds, now)
}

func readyForAtleast(cs []corev1.NodeCondition, minReadySeconds int32, now metav1.Time) bool {
	rc := getReadyCondition(cs)
	if !isTrue(rc) {
		return false
	}
	if minReadySeconds == 0 {
		return true
	}
	x := time.Duration(minReadySeconds) * time.Second
	return !rc.LastTransitionTime.IsZero() && rc.LastTransitionTime.Add(x).Before(now.Time)
}

func getReadyCondition(cs []corev1.NodeCondition) *corev1.NodeCondition {
	for i := range cs {
		if cs[i].Type == corev1.NodeReady {
			return &cs[i]
		}
	}
	return nil
}

// GetReadyCondition extracts the ready condition from the given status and returns that.
// Returns nil and -1 if the condition is not present, and the index of the located condition.
func GetReadyCondition(status *corev1.NodeStatus) *corev1.NodeCondition {
	if status == nil {
		return nil
	}
	return getReadyCondition(status.Conditions)
}

func IsMachineReady(m *clusterv1.Machine) bool {
	if m == nil {
		return false
	}
	return isTrue(getReadyCondition(m.Status.Conditions))
}

func isTrue(rc *corev1.NodeCondition) bool {
	return rc != nil && rc.Status == corev1.ConditionTrue
}

// IsNodeReady returns true if a node is ready; false otherwise.
func IsNodeReady(node *corev1.Node) bool {
	if node == nil {
		return false
	}
	return isTrue(getReadyCondition(node.Status.Conditions))
}
