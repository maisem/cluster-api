/*
Copyright 2017 The Kubernetes Authors.

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

package node

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog"
	clusterv1 "sigs.k8s.io/cluster-api/pkg/apis/cluster/v1alpha1"
)

func (c *Reconciler) link(ctx context.Context, machine *clusterv1.Machine, node *corev1.Node) error {
	t := metav1.Now()
	machine.Status.LastUpdated = &t
	machine.Status.NodeRef = objectRef(node)
	machine.Status.Conditions = node.Status.Conditions
	if err := c.Client.Status().Update(ctx, machine); err != nil {
		klog.Errorf("Error updating machine %s to link node %s: %v\n", machine.ObjectMeta.Name, node.ObjectMeta.Name, err)
		return err
	}
	klog.Infof("Successfully linked machine %s to node %s\n", machine.ObjectMeta.Name, node.ObjectMeta.Name)
	return nil
}

func (c *Reconciler) unlink(ctx context.Context, machine *clusterv1.Machine, node *corev1.Node) error {
	// This machine was linked to a different node, don't unlink them
	if machine.Status.NodeRef.Name != node.ObjectMeta.Name {
		klog.Warningf("Node (%v) is tring to unlink machine (%v) which is linked with node (%v).",
			node.ObjectMeta.Name, machine.ObjectMeta.Name, machine.Status.NodeRef.Name)
		return nil
	}

	t := metav1.Now()
	machine.Status.LastUpdated = &t
	machine.Status.NodeRef = nil
	if err := c.Client.Status().Update(ctx, machine); err != nil {
		klog.Errorf("Error updating machine %s to unlink node %s: %v\n", machine.ObjectMeta.Name, node.ObjectMeta.Name, err)
		return err
	}
	klog.Infof("Successfully unlinked node %s from machine %s\n", node.ObjectMeta.Name, machine.ObjectMeta.Name)
	return nil
}

func objectRef(node *corev1.Node) *corev1.ObjectReference {
	return &corev1.ObjectReference{
		Kind: node.Kind,
		Name: node.ObjectMeta.Name,
		UID:  node.ObjectMeta.UID,
	}
}
