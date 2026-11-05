<!--
SPDX-FileCopyrightText: 2024  StorPool <support@storpool.com>
SPDX-License-Identifier: Apache-2.0
-->

# StorPool CSI

## Overview

The StorPool CSI driver enables CSI-compliant COs e.g. Kubernetes, to dynamically provision
and attach StorPool volumes. The driver implements version 1.11.0 of the CSI specification and
registers itself under the name `csi.storpool.com`.

Currently, the driver only supports the StorPool native protocol, hence it's required that all
Kubernetes hosts have the StorPool native client service (`storpool_block`) configured and
running.

## Features

- Dynamic provisioning and deletion of volumes. Each `StorageClass` maps to a StorPool
  template, so multiple storage tiers can be exposed from a single StorPool cluster.
- Filesystem volumes (`volumeMode: Filesystem`, formatted with `ext4` by default) and raw block
  volumes (`volumeMode: Block`).
- Access modes depend on the volume mode: filesystem volumes support `ReadWriteOnce` only,
  while raw block volumes also support `ReadWriteMany` and `ReadOnlyMany`, i.e. the same block
  volume can be attached to multiple nodes at the same time (as required by e.g. KubeVirt live
  migration).
- Read-only publishing of volumes (`readOnly: true` on the pod volume).
- Online volume expansion. The filesystem is grown in place on the node without unmounting;
  this is implemented for `ext4` only.
- Volume cloning from an existing PVC (`dataSource` of kind `PersistentVolumeClaim`).
- Volume modification through a `VolumeAttributesClass`: StorPool volume tags and other volume
  parameters (e.g. the template) can be changed on an existing volume.
- Creating a volume from an existing StorPool snapshot via a pre-provisioned
  `VolumeSnapshotContent`. Creating and deleting snapshots through Kubernetes is not supported.
- StorPool multi-cluster aware: the node ID encodes the StorPool cluster a node belongs to, and
  attach/detach operations are issued against that cluster.
- Every volume is created with a `csi_name` tag holding the name of the Kubernetes PV, so
  StorPool volumes can be matched to Kubernetes objects from the StorPool CLI.
- Reports StorPool's per-client attached volume limit to the Kubernetes scheduler.

## Requirements

- A working StorPool cluster.
- The StorPool native client service (`storpool_block`) configured and running on every
  Kubernetes node that will use StorPool volumes.
- `/etc/storpool.conf` present on every node. The node plugin reads the node's own section of
  the file (selected by the `SP_NODE_NAME` environment variable, which the manifests set to the
  Kubernetes node name) to determine the node's `SP_CLUSTER_ID` and `SP_OURID`.
- Access to the StorPool API from the controller plugin. The shipped manifests mount
  `/etc/storpool.conf` from the host, so the controller plugin must be scheduled on a node with
  StorPool configured. Alternatively, the `SP_API_ENDPOINT` and `SP_API_TOKEN` environment
  variables can be set (see [Configuration](#configuration)).

## Deployment

The StorPool CSI driver can be deployed using the precompiled manifests inside the `manifests/`
directory. The deployment procedure assumes that StorPool is already installed and configured.
The procedure looks like this:

1. Apply the `CSIDriver`, `ControllerPlugin` and `NodePlugin` manifests:

```shell
kubectl apply -f manifests/*
```
Please note, the manifests will create all respective resource inside the `kube-system` namespace.
Feel free to edit them if you want to create the resources elsewhere.

2. Create a `StorageClass` resource. Mapping a `StorageClass` to a StorPool template allows the 
Kubernetes operator to utilize multiple storage media if such is present in the StorPool cluster.
More information on StorPool templates can be found
[here](https://kb.storpool.com/user_guides/user_guide.html#cli-templates).
Below you can find an example `StorageClass`:
```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: storpool-nvme
provisioner: csi.storpool.com
allowVolumeExpansion: true
parameters:
  template: nvme
volumeBindingMode: WaitForFirstConsumer
```

The driver recognises the following `StorageClass` settings:

| Setting                                | Required | Description                                                                                                       |
|----------------------------------------|----------|-------------------------------------------------------------------------------------------------------------------|
| `parameters.template`                  | yes      | Name of the StorPool template to create volumes from.                                                             |
| `parameters.csi.storage.k8s.io/fstype` | no       | Filesystem used for `Filesystem` volumes. Defaults to `ext4`. Only `ext4` supports online expansion.              |
| `mountOptions`                         | no       | Extra mount options for `Filesystem` volumes. The driver always adds `discard` and `ro`/`rw` on its own.          |
| `allowVolumeExpansion`                 | no       | Set to `true` to allow PVCs of this class to be expanded.                                                         |

3. Finally, one can create a PVC to test if the CSI is configured properly. Please note that
filesystem volumes support only the `ReadWriteOnce` access mode; see
[Raw block volumes](#raw-block-volumes) for the access modes available to block volumes.
```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: test-storpool-pvc
spec:
  storageClassName: storpool-nvme
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
```

## Usage

### Raw block volumes

Set `volumeMode: Block` on the PVC to expose the StorPool volume to the pod as a block device
instead of a mounted filesystem. Block volumes support the `ReadWriteOnce`, `ReadOnlyMany` and
`ReadWriteMany` access modes. A `ReadWriteMany` block volume is attached to every node that
publishes it; coordinating concurrent access is left to the consuming application (for example
KubeVirt during a live migration).

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: test-storpool-block-pvc
spec:
  storageClassName: storpool-nvme
  volumeMode: Block
  accessModes:
    - ReadWriteMany
  resources:
    requests:
      storage: 10Gi
---
apiVersion: v1
kind: Pod
metadata:
  name: test-storpool-block-pod
spec:
  containers:
    - name: app
      image: busybox
      command: ["sleep", "infinity"]
      volumeDevices:
        - name: data
          devicePath: /dev/xvda
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: test-storpool-block-pvc
```

### Expanding volumes

If the `StorageClass` has `allowVolumeExpansion: true`, a volume can be grown by increasing
`spec.resources.requests.storage` of its PVC. Expansion happens online: the StorPool volume is
resized by the controller plugin and the filesystem is then grown in place by the node plugin
while the volume stays mounted. Node-side filesystem expansion is implemented for `ext4` only;
block volumes need no node-side step.

### Cloning volumes

A new PVC can be populated from an existing one by setting `dataSource`. The new volume is
created in StorPool as a clone of the source volume, so it is created instantly and shares the
unchanged data blocks with its source. The clone must use the same `StorageClass` and
`volumeMode` as the source and must be at least as large.

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: test-storpool-pvc-clone
spec:
  storageClassName: storpool-nvme
  accessModes:
    - ReadWriteOnce
  dataSource:
    kind: PersistentVolumeClaim
    name: test-storpool-pvc
  resources:
    requests:
      storage: 10Gi
```

### Restoring from a StorPool snapshot

The driver does not create or delete snapshots, so `VolumeSnapshot` objects cannot be
provisioned dynamically. An existing StorPool snapshot can still be used as the source of a new
volume: pre-provision a `VolumeSnapshotContent` whose `snapshotHandle` is the global ID of the
StorPool snapshot (together with the corresponding `VolumeSnapshot`), then reference that
`VolumeSnapshot` as the PVC's `dataSource`. The new StorPool volume is created with the snapshot
as its parent. This requires the Kubernetes volume snapshot CRDs and snapshot controller, which
are not part of the shipped manifests.

### Modifying volumes

A `VolumeAttributesClass` can be used to change attributes of an existing volume by setting
`spec.volumeAttributesClassName` on its PVC. The driver interprets the class parameters as
follows:

- `tag.<key>` sets the StorPool volume tag `<key>` to the given value.
- Any other key is passed as-is to the StorPool `VolumeUpdate` API call, e.g. `template` moves
  the volume to another StorPool template.

```yaml
apiVersion: storage.k8s.io/v1
kind: VolumeAttributesClass
metadata:
  name: storpool-hdd-archive
driverName: csi.storpool.com
parameters:
  template: hdd
  tag.tier: archive
```

Volume modification requires Kubernetes 1.29 or newer with the `VolumeAttributesClass` feature
gate enabled (it is enabled by default since Kubernetes 1.34, where the API is GA; older
releases expose it as `v1beta1`/`v1alpha1`), and the `csi-resizer` sidecar at version 1.10.0 or
newer started with `--feature-gates=VolumeAttributesClass=true` and granted RBAC access to
`volumeattributesclasses`. Please note that the manifests in this repository ship
`csi-resizer` v1.9.0 and therefore do not enable volume modification yet.

