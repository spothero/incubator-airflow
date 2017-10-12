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
                'value': kube_config.dags_folder
            }, {
                'name': 'GIT_SYNC_DEST',
                'value': ''
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
        env = [{
            'name': 'AIRFLOW__CORE__AIRFLOW_HOME',
            'value': kube_config.airflow_home
        }]
        if kube_config.kube_secrets_object_name:
            env.extend([
                {
                    'name': env_var,
                    'valueFrom': {
                        'secretKeyRef': {
                            'name': kube_config.kube_secrets_object_name,
                            'key': secret_key
                        }
                    }
                }
                for secret_key, env_var in six.iteritems(kube_config.kube_secrets)
            ])
        return env
