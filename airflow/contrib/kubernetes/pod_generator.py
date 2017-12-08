# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import copy
import os
import six

from airflow.contrib.kubernetes.pod import Pod, Resources
from airflow.contrib.kubernetes.secret import Secret


class PodGenerator:
    """Contains Kubernetes Airflow Worker configuration logic"""

    def __init__(self, kube_config=None):
        self.kube_config = kube_config
        self.env_vars = {}
        self.volumes = []
        self.volume_mounts = []
        self.secrets = []

    def set_environment(self, env):
        """

        Args:
            env (str):

        Returns:

        """
        self.env_vars = env

    def _get_environment(self):
        return self.env_vars

    def add_volume(self, name):
        """

        Args:
            name (str):

        Returns:

        """
        self.volumes.append({'name': name})

    def add_volume_with_configmap(self, name, config_map):
        self.volumes.append(
            {
                'name': name,
                'configMap': config_map
            }
        )

    def add_mount(self,
                  name,
                  mount_path,
                  sub_path,
                  read_only):
        """

        Args:
            name (str):
            mount_path (str):
            sub_path (str):
            read_only:

        Returns:

        """

        self.volume_mounts.append({
            'name': name,
            'mountPath': mount_path,
            'subPath': sub_path,
            'readOnly': read_only
        })

    def _get_volumes_and_mounts(self):
        return self.volumes, self.volume_mounts


    def make_pod(self, namespace, pod_id, cmds,
                 arguments, labels, kube_executor_config=None):
        volumes, volume_mounts = self._get_volumes_and_mounts()
        resources = Resources(
            request_memory=kube_executor_config.request_memory,
            request_cpu=kube_executor_config.request_cpu,
            limit_memory=kube_executor_config.limit_memory,
            limit_cpu=kube_executor_config.limit_cpu
        )

        return Pod(
            namespace=namespace,
            name=pod_id,
            image=kube_executor_config.image or self.kube_config.kube_image,
            cmds=cmds,
            args=[arguments],
            labels=labels,
            envs=self._get_environment,
            secrets=self._get_secrets(),
            service_account_name=self.kube_config.worker_service_account_name,
            volumes=volumes,
            volume_mounts=volume_mounts,
            resources=resources
        )


'''
This class is a necessary building block to the kubernetes executor, which will be PR'd
shortly
'''


class WorkerGenerator(PodGenerator):
    def __init__(self, kube_config):
        PodGenerator.__init__(self, kube_config)
        self.volumes, self.volume_mounts = self._init_volumes_and_mounts()

    def _init_volumes_and_mounts(self):
        dags_volume_name = "airflow-dags"
        dags_path = os.path.join(self.kube_config.dags_folder,
                                 self.kube_config.git_subpath)
        volumes = [{
            'name': dags_volume_name
        }]
        volume_mounts = [{
            'name': dags_volume_name,
            'mountPath': dags_path,
            'readOnly': True
        }]

        # A PV with the DAGs should be mounted
        volumes[0]['persistentVolumeClaim'] = {"claimName": self.kube_config.dags_volume_claim}
        return volumes, volume_mounts

    def _init_labels(self, dag_id, task_id, execution_date):
        return {
            "airflow-slave": "",
            "dag_id": dag_id,
            "task_id": task_id,
            "execution_date": execution_date
        },

    def make_worker_pod(self,
                        namespace,
                        pod_id,
                        dag_id,
                        task_id,
                        execution_date,
                        airflow_command,
                        kube_executor_config):
        cmds=["bash", "-cx", "--"]
        labels = self._init_labels(dag_id, task_id, execution_date)
        PodGenerator.make_pod(self,
                              namespace=namespace,
                              pod_id=pod_id,
                              cmds=cmds,
                              arguments=airflow_command,
                              labels=labels,
                              kube_executor_config=kube_executor_config)
