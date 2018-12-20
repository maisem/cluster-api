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

package node

import (
	"context"
	"sync"
	"time"

	"github.com/openshift/kubernetes-drain"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog"
	clusterv1 "sigs.k8s.io/cluster-api/pkg/apis/cluster/v1alpha1"
	"sigs.k8s.io/cluster-api/pkg/util"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	finalizer = "machines.k8s.io/k8s-node"
)

// Add creates a new Node Controller and adds it to the Manager with default RBAC. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newNodeReconciler(mgr))
}

// newNodeReconciler returns a new reconcile.NodeReconciler
func newNodeReconciler(mgr manager.Manager) *NodeReconciler {
	return &NodeReconciler{
		Client:  mgr.GetClient(),
		kClient: kubernetes.NewForConfigOrDie(mgr.GetConfig()),
		scheme:  mgr.GetScheme(),
		links:   make(map[string]nodeMachineMapping),
	}
}

type nodeMachineMapping struct {
	providerID  string
	nodeName    types.NamespacedName
	machineName types.NamespacedName
}

// add adds a new Controller to mgr with r as the reconcile.NodeReconciler
func add(mgr manager.Manager, r *NodeReconciler) error {
	// Create a new controller
	nc, err := controller.New("node-controller", mgr, controller.Options{Reconciler: reconcile.Func(r.ReconcileNode)})
	if err != nil {
		return err
	}
	// Watch for changes to Node
	if err := nc.Watch(&source.Kind{Type: &corev1.Node{}}, &handler.EnqueueRequestForObject{}); err != nil {
		return err
	}

	mc, err := controller.New("node-controller-machine", mgr, controller.Options{Reconciler: reconcile.Func(r.ReconcileMachine)})
	if err != nil {
		return err
	}
	// Watch for changes to Machines.
	if err := mc.Watch(&source.Kind{Type: &clusterv1.Machine{}}, &handler.EnqueueRequestForObject{}); err != nil {
		return err
	}
	return nil
}

// ReconcileNode reconciles a Node object
type NodeReconciler struct {
	client.Client
	kClient *kubernetes.Clientset
	scheme  *runtime.Scheme
	m       sync.RWMutex
	links   map[string]nodeMachineMapping
}

func (r *NodeReconciler) ReconcileMachine(request reconcile.Request) (reconcile.Result, error) {
	// Fetch the Machine instance
	m := &clusterv1.Machine{}
	ctx := context.Background()
	err := r.Get(ctx, request.NamespacedName, m)
	if err != nil {
		if errors.IsNotFound(err) {
			// Object not found, return.  Created objects are automatically garbage collected.
			// For additional cleanup logic use finalizers.
			klog.Errorf("Unable to retrieve Node %v from store: %v", request, err)
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}
	if m.Status.ProviderID == nil {
		return reconcile.Result{}, nil
	}
	id := *m.Status.ProviderID
	klog.Infof("Reconciling machine %q", id)

	if !m.DeletionTimestamp.IsZero() {
		klog.Infof("Machine %q deleted, removing link", id)
		if !util.Contains(m.Finalizers, finalizer) {
			return reconcile.Result{}, nil
		}
		if m.Status.NodeRef != nil {
			// Drain the node.
			n, err := r.kClient.CoreV1().Nodes().Get(m.Status.NodeRef.Name, metav1.GetOptions{})
			if err != nil {
				if !errors.IsNotFound(err) {
					return reconcile.Result{}, err
				}
			} else if err := drain.Drain(r.kClient, []*corev1.Node{n}, &drain.DrainOptions{
				DeleteLocalData:    true,
				GracePeriodSeconds: 3600,
				IgnoreDaemonsets:   true,
				Timeout:            time.Hour,
			}); err != nil {
				klog.Infof("Node %q drain failed: %v", id, err)
			}
		}
		klog.Infof("Machine %q removing link", id)
		r.m.Lock()
		delete(r.links, id)
		r.m.Unlock()
		m.ObjectMeta.Finalizers = util.Filter(m.ObjectMeta.Finalizers, finalizer)
		if err := r.Client.Update(ctx, m); err != nil {
			klog.Errorf("Error removing finalizer from machine object %v; %v", m.Name, err)
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}
	if !util.Contains(m.ObjectMeta.Finalizers, finalizer) {
		// If object hasn't been deleted and doesn't have a finalizer, add one
		// Add a finalizer to newly created objects.
		m.Finalizers = append(m.Finalizers, "machines.k8s.io/k8s-node")
		if err = r.Update(ctx, m); err != nil {
			klog.Infof("failed to add finalizer to machine object %v due to error %v.", m.Name, err)
			return reconcile.Result{}, err
		}
	}

	// See if we have already seen the node.
	r.m.RLock()
	x, ok := r.links[id]
	r.m.RUnlock()
	if !ok {
		klog.Infof("First observation of machine %q", id)
		// This means we haven't seen a node associated with this provider id yet.
		r.m.Lock()
		r.links[id] = nodeMachineMapping{
			providerID:  id,
			machineName: request.NamespacedName,
		}
		r.m.Unlock()
		return reconcile.Result{}, nil
	}
	if x.nodeName.Name == "" {
		klog.Infof("Node %q hasn't been observed yet", id)
		// This means we haven't seen a node associated with this provider id yet.
		return reconcile.Result{}, nil
	}
	if x.machineName.Name != "" {
		klog.Infof("Machine and node already linked: %q", id)
		// This means that we have already linked the machine and node.
		return reconcile.Result{}, nil
	}
	node := &corev1.Node{}
	if err := r.Get(ctx, x.nodeName, node); err != nil {
		if errors.IsNotFound(err) {
			// Don't do anything, let the node reconciliation loop deal with this.
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}
	klog.Infof("Linking %q", id)
	if err := r.link(ctx, m, node); err != nil {
		klog.Infof("Failed to link %q: %v", id, err)
		return reconcile.Result{}, err
	}
	x.machineName = request.NamespacedName
	r.m.Lock()
	r.links[id] = x
	r.m.Unlock()
	return reconcile.Result{}, nil
}

// Reconcile reads that state of the cluster for a Node object and makes changes based on the state read
// and what is in the Node.Spec
// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster.k8s.io,resources=machines,verbs=get;list;watch;create;update;patch;delete
func (r *NodeReconciler) ReconcileNode(request reconcile.Request) (reconcile.Result, error) {
	// Fetch the node instance
	node := &corev1.Node{}
	ctx := context.Background()
	err := r.Get(ctx, request.NamespacedName, node)
	if err != nil {
		if errors.IsNotFound(err) {
			// Object not found, return.  Created objects are automatically garbage collected.
			// For additional cleanup logic use finalizers.
			klog.Errorf("Unable to retrieve Node %v from store: %v", request, err)
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}
	id := node.Spec.ProviderID
	klog.Infof("Reconciling node with providerID: %q", id)
	// See if we have already seen the machine.
	r.m.RLock()
	x, ok := r.links[id]
	r.m.RUnlock()
	if !ok {
		klog.Infof("First observation of node %q", id)
		// This means we haven't seen a machine associated with this provider id yet.
		r.m.Lock()
		r.links[id] = nodeMachineMapping{
			providerID: id,
			nodeName:   request.NamespacedName,
		}
		r.m.Unlock()
		return reconcile.Result{}, nil
	}
	if x.machineName.Name == "" {
		klog.Infof("Machine %q hasn't been observed yet", id)
		// This means we haven't seen a machine associated with this provider id yet.
		return reconcile.Result{}, nil
	}
	machine := &clusterv1.Machine{}
	if err := r.Get(ctx, x.machineName, machine); err != nil {
		if errors.IsNotFound(err) {
			// Don't do anything, let the machine reconciliation loop deal with this.
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}
	// This means that we have already linked the machine and node.
	if !node.DeletionTimestamp.IsZero() {
		klog.Infof("Node %q is being deleted", id)
		// We should remove the link if the node goes away.
		return reconcile.Result{}, r.unlink(ctx, machine, node)
	}
	klog.Infof("Linking %q", id)
	if err := r.link(ctx, machine, node); err != nil {
		klog.Infof("Failed to link %q: %v", id, err)
		return reconcile.Result{}, err
	}
	x.nodeName = request.NamespacedName
	r.m.Lock()
	r.links[id] = x
	r.m.Unlock()
	return reconcile.Result{}, nil
}
