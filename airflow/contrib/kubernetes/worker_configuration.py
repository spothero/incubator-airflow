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

import six

from airflow.contrib.kubernetes.secret import Secret


class WorkerConfiguration:
    """Contains Kubernetes Airflow Worker configuration logic"""
    @classmethod
    def get_init_containers(cls, kube_config, volume_mounts):
        """When using git to retrieve the DAGs, use the GitSync Init Container"""
        # If we're using volume claims to mount the dags, no init container is needed
        if kube_config.dags_volume_claim:
            return []

        # Otherwise, define a git-sync init container
        init_environment = [{
                'name': 'GIT_SYNC_REPO',
                'value': kube_config.git_repo
            }, {
                'name': 'GIT_SYNC_BRANCH',
                'value': kube_config.git_branch
            }, {
                'name': 'GIT_SYNC_ROOT',
                'value': '/tmp'
            }, {
                'name': 'GIT_SYNC_DEST',
                'value': 'dags'
            }, {
                'name': 'GIT_SYNC_ONE_TIME',
                'value': 'true'
            }]
        if kube_config.git_user:
            init_environment.append({
                'name': 'GIT_SYNC_USERNAME',
                'value': kube_config.git_user
            })
        if kube_config.git_password:
            init_environment.append({
                'name': 'GIT_SYNC_PASSWORD',
                'value': kube_config.git_password
            })

        volume_mounts[0]['readOnly'] = False
        return [{
            'name': kube_config.git_sync_init_container_name,
            'image': kube_config.git_sync_container,
            'securityContext': {'runAsUser': 0},
            'env': init_environment,
            'volumeMounts': volume_mounts
        }]

    @classmethod
    def get_volumes_and_mounts(cls, kube_config):
        """Determine volumes and volume mounts for Airflow workers"""
        config_volume_name = "airflow-config"
        dags_volume_name = "airflow-dags"
        dags_path = '{}/{}'.format(kube_config.dags_folder, kube_config.git_subpath.lstrip('/'))
        config_path = '{}/airflow.cfg'.format(kube_config.airflow_home)
        volumes = [{
            'name': dags_volume_name
        }, {
            'name': config_volume_name,
            'configMap': {
                'name': kube_config.airflow_configmap
            }
        }]
        volume_mounts = [{
            'name': dags_volume_name,
            'mountPath': dags_path,
            'readOnly': True
        }, {
            'name': config_volume_name,
            'mountPath': config_path,
            'subPath': 'airflow.cfg',
            'readOnly': True
        }]

        # A PV with the DAGs should be mounted
        if kube_config.dags_volume_claim:
            volumes[0]['persistentVolumeClaim'] = {"claimName": kube_config.dags_volume_claim}

            if kube_config.dags_volume_subpath:
                volume_mounts[0]["subPath"] = kube_config.dags_volume_subpath
            return volumes, volume_mounts

        # Create a Shared Volume for the Git-Sync module to populate
        volumes[0]["emptyDir"] = {}
        return volumes, volume_mounts

    @classmethod
    def get_environment(cls, kube_config):
        """Defines any necessary environment variables for the pod executor"""
        return {
            'AIRFLOW__CORE__AIRFLOW_HOME': kube_config.airflow_home,
            'AIRFLOW__CORE__DAGS_FOLDER': '/tmp/dags'
        }

    @classmethod
    def get_secrets(cls, kube_config):
        """Defines any necessary secrets for the pod executor"""
        worker_secrets = []
        for k8s_secret_obj, key_env_pair in six.iteritems(kube_config.kube_secrets):
            k8s_secret_key, env_var_name = key_env_pair.split(':')
            worker_secrets.append(Secret('env', env_var_name, k8s_secret_obj, k8s_secret_key))
        return worker_secrets

    @classmethod
    def get_image_pull_secrets(cls, kube_config):
        """Extracts any image pull secrets for fetching container(s)"""
        if not kube_config.image_pull_secrets:
            return []
        return kube_config.image_pull_secrets.split(',')
