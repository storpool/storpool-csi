<!--
SPDX-FileCopyrightText: 2024  StorPool <support@storpool.com>
SPDX-License-Identifier: Apache-2.0
-->

# StorPool CSI

## Overview

The StorPool CSI driver enables CSI-compliant COs e.g. Kubernetes, to dynamically provision
and attach StorPool volumes. Currently, the driver only supports the StorPool native protocol,
hence it's required that all Kubernetes hosts have the StorPool native client service 
(`storpool_block`) configured and running.

## Deployment

The StorPool CSI driver can be deployed using the precompiled manifests inside the `manifests/`
directory. The deployment procedure assumes that StorPool is already installed and configured.
The procedure looks like this:

1. Apply the `ControllerPlugin` and `NodePlugin` manifests:

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

3. Finally, one can create a PVC to test if the CSI is configured properly. Please note that the
StorPool CSI supports only the `ReadWriteOnce` access mode.
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
