package view

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPodStatus(t *testing.T) {
	waiting := func(reason string) corev1.ContainerStatus {
		return corev1.ContainerStatus{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason}}}
	}
	now := metav1.Now()
	cases := []struct {
		name string
		pod  corev1.Pod
		want string
	}{
		{"running", corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{
			{Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}, "Running"},
		{"crashloop", corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{waiting("CrashLoopBackOff")}}}, "CrashLoopBackOff"},
		{"oomkilled", corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{
			{State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled", ExitCode: 137}}}}}}, "OOMKilled"},
		{"init waiting", corev1.Pod{Spec: corev1.PodSpec{InitContainers: []corev1.Container{{Name: "i"}}},
			Status: corev1.PodStatus{Phase: corev1.PodPending, InitContainerStatuses: []corev1.ContainerStatus{waiting("ImagePullBackOff")}}}, "Init:ImagePullBackOff"},
		{"terminating", corev1.Pod{ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &now},
			Status: corev1.PodStatus{Phase: corev1.PodRunning}}, "Terminating"},
		{"evicted", corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodFailed, Reason: "Evicted"}}, "Evicted"},
	}
	for _, c := range cases {
		if got := PodStatus(&c.pod); got != c.want {
			t.Errorf("%s: PodStatus = %q, want %q", c.name, got, c.want)
		}
	}
}
