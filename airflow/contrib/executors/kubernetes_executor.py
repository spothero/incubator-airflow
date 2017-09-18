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
import logging
import time
import os
import multiprocessing
import six
from queue import Queue
from datetime import datetime
from dateutil import parser
from uuid import uuid4
from kubernetes import watch, client
from kubernetes.client.rest import ApiException
from airflow import settings
from airflow.contrib.kubernetes.pod_launcher import PodLauncher
from airflow.executors.base_executor import BaseExecutor
from airflow.models import TaskInstance
from airflow.utils.state import State
from airflow import configuration
from airflow.exceptions import AirflowConfigException
from airflow.contrib.kubernetes.pod import Pod


class KubeConfig:
    core_section = "core"
    kubernetes_section = "kubernetes"

    @staticmethod
    def safe_get(section, option, default):
        try:
            return configuration.get(section, option)
        except AirflowConfigException:
            return default

    @staticmethod
    def safe_getboolean(section, option, default):
        try:
            return configuration.getboolean(section, option)
        except AirflowConfigException:
            return default

    def __init__(self):
        self.core_configuration = configuration.as_dict(display_sensitive=True)['core']
        self.dags_folder = configuration.get(self.core_section, 'dags_folder')
        self.parallelism = configuration.getint(self.core_section, 'PARALLELISM')
        self.kube_image = configuration.get(self.kubernetes_section, 'container_image')
        self.delete_worker_pods = self.safe_getboolean(self.kubernetes_section, 'delete_worker_pods', True)

        # The Kubernetes Namespace in which pods will be created by the executor. Note that if your
        # cluster has RBAC enabled, your scheduler will need service account permissions to
        # create, watch, get, and delete pods in this namespace.
        self.kube_namespace = self.safe_get(self.kubernetes_section, 'namespace', 'default')

        # NOTE: `git_repo` and `git_branch` must be specified together as a pair
        # The http URL of the git repository to clone from
        self.git_repo = self.safe_get(self.kubernetes_section, 'git_repo', None)
        # The branch of the repository to be checked out
        self.git_branch = self.safe_get(self.kubernetes_section, 'git_branch', None)
        # Optionally, the directory in the git repository containing the dags
        self.git_subpath = self.safe_get(self.kubernetes_section, 'git_subpath', None)

        # Optionally a user may supply a `git_user` and `git_password` for private repositories
        self.git_user = self.safe_get(self.kubernetes_section, 'git_user', None)
        self.git_password = self.safe_get(self.kubernetes_section, 'git_password', None)

        # NOTE: The user may optionally use a volume claim to mount a PV containing DAGs directly
        self.dags_volume_claim = self.safe_get(self.kubernetes_section, 'dags_volume_claim', None)

        # This prop may optionally be set for either PV or Git Volumes and is used to located DAGs
        # on a SubPath in the volume
        self.dags_volume_subpath = self.safe_get(self.kubernetes_section, 'dags_volume_subpath', None)

        self._validate()

    def _validate(self):
        if not self.dags_volume_claim and (not self.git_repo or not self.git_branch):
            raise AirflowConfigException(
                "In kubernetes mode you must set the following configs in the `kubernetes` section: "
                "`dags_volume_claim` or "
                "`git_repo and git_branch`"
            )


class PodMaker:
    def __init__(self, kube_config):
        self.logger = logging.getLogger(__name__)
        self.kube_config = kube_config

    def _get_init_containers(self, volume_mounts):
        """When using git to retrieve the DAGs, use the GitSync Init Container"""
        if self.kube_config.dags_volume_claim:
            return []

        init_environment = [{
                'name': 'GIT_SYNC_REPO',
                'value': self.kube_config.git_repo
            }, {
                'name': 'GIT_SYNC_BRANCH',
                'value': self.kube_config.git_branch
            }, {
                'name': 'GIT_SYNC_ROOT',
                'value': self.kube_config.dags_folder
            }, {
                'name': 'GIT_SYNC_DEST',
                'value': ''
            }, {
                'name': 'GIT_SYNC_ONE_TIME',
                'value': 'true'
            }]
        if self.kube_config.git_user:
            init_environment.append({
                'name': 'GIT_SYNC_USERNAME',
                'value': self.kube_config.git_user
            })
        if self.kube_config.git_password:
            init_environment.append({
                'name': 'GIT_SYNC_PASSWORD',
                'value': self.kube_config.git_password
            })

        volume_mounts[0]['readOnly'] = False
        return [{
            'name': 'git-sync-clone',
            'image': 'gcr.io/google-containers/git-sync-amd64:v2.0.5',
            'securityContext': {'runAsUser': 0},
            'env': init_environment,
            'volumeMounts': volume_mounts
        }]

    def _get_volumes_and_mounts(self):
        volume_name = "airflow-dags"
        volumes = [{'name': 'airflow-dags'}]
        volume_mounts = [{
                "name": volume_name, "mountPath": self.kube_config.dags_folder,
                "readOnly": True
            }]

        # A PV with the DAGs should be mounted
        if self.kube_config.dags_volume_claim:
            volumes[0]['persistentVolumeClaim'] = {"claimName": self.kube_config.dags_volume_claim}

            if self.kube_config.dags_volume_subpath:
                volume_mounts[0]["subPath"] = self.kube_config.dags_volume_subpath
            return volumes, volume_mounts
        # Use Kubernetes git-sync sidecar to retrieve the latest DAGs
        else:
            # Create a Shared Volume for the Git-Sync module to populate
            volumes = [{"name": volume_name, "emptyDir": {}}]
        return volumes, volume_mounts

    def make_environment(self):
        """Creates the Pod Executor's environment by extracting the Scheduler's core configuration
        to be shared with the newly created Pod. The core configuration contains critical settings
        such as SQLALchemy connection strings, S3 Logging Buckets, etc.
        """
        environment = {}
        configuration_dict = configuration.as_dict(display_sensitive=True)
        configuration_dict['core']['executor'] = 'LocalExecutor'
        if self.kube_config.git_subpath:
            configuration_dict['core']['dags_folder'] = '{}/{}'.format(
                self.kube_config.dags_folder, self.kube_config.git_subpath.lstrip('/'))
        for section, section_dict in six.iteritems(configuration_dict):
            section_upper = section.upper()
            for key, value in six.iteritems(section_dict):
                environment['AIRFLOW__{}__{}'.format(section_upper, key.upper())] = value
        return environment

    def make_pod(self, namespace, pod_id, dag_id, task_id, execution_date, airflow_command):
        volumes, volume_mounts = self._get_volumes_and_mounts()
        return Pod(
            namespace=namespace,
            name=pod_id,
            image=self.kube_config.kube_image,
            cmds=["bash", "-cx", "--"],
            args=[airflow_command],
            labels={
                "airflow-slave": "",
                "dag_id": dag_id,
                "task_id": task_id,
                "execution_date": execution_date
            },
            envs=self.make_environment(),
            init_containers=self._get_init_containers(copy.deepcopy(volume_mounts)),
            volumes=volumes,
            volume_mounts=volume_mounts
        )


class KubernetesJobWatcher(multiprocessing.Process, object):
    def __init__(self, namespace, watcher_queue):
        self.logger = logging.getLogger(__name__)
        multiprocessing.Process.__init__(self)
        self.namespace = namespace
        self.watcher_queue = watcher_queue
        self._api = client.CoreV1Api()
        self._watch = watch.Watch()

    def run(self):
        while True:
            try:
                self._run()
            except Exception:
                self.logger.exception("Unknown error in KubernetesJobWatcher. Failing")
                raise
            else:
                self.logger.warn("Watcher process died gracefully: generator stopped returning values")

    def _run(self):
        self.logger.info("Event: and now my watch begins")
        for event in self._watch.stream(self._api.list_namespaced_pod, self.namespace,
                                        label_selector='airflow-slave'):
            task = event['object']
            self.logger.info(
                "Event: {} had an event of type {}".format(task.metadata.name,
                                                           event['type']))
            self.process_status(task.metadata.name, task.status.phase, task.metadata.labels)

    def process_status(self, pod_id, status, labels):
        if status == 'Pending':
            self.logger.info("Event: {} Pending".format(pod_id))
        elif status == 'Failed':
            self.logger.info("Event: {} Failed".format(pod_id))
            self.watcher_queue.put((pod_id, State.FAILED, labels))
        elif status == 'Succeeded':
            self.logger.info("Event: {} Succeeded".format(pod_id))
            self.watcher_queue.put((pod_id, None, labels))
        elif status == 'Running':
            self.logger.info("Event: {} is Running".format(pod_id))
        else:
            self.logger.info("Event: Invalid state: {} on pod: {} with labels: {}".format(status, pod_id, labels))


class AirflowKubernetesScheduler(object):
    def __init__(self, kube_config, task_queue, result_queue):
        self.logger = logging.getLogger(__name__)
        self.logger.info("creating kubernetes executor")
        self.kube_config = kube_config
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.namespace = self.kube_config.kube_namespace
        self.logger.info("k8s: using namespace {}".format(self.namespace))
        self.launcher = PodLauncher()
        self.pod_maker = PodMaker(kube_config=self.kube_config)
        self.watcher_queue = multiprocessing.Queue()
        self.api = client.CoreV1Api()
        self.kube_watcher = self._make_kube_watcher()

    def _make_kube_watcher(self):
        watcher = KubernetesJobWatcher(self.namespace, self.watcher_queue)
        watcher.start()
        return watcher

    def _health_check_kube_watcher(self):
        if self.kube_watcher.is_alive():
            pass
        else:
            self.logger.error("Error while health checking kube watcher process. Process died for unknown reasons")
            self.kube_watcher = self._make_kube_watcher()

    def run_next(self, next_job):
        """

        The run_next command will check the task_queue for any un-run jobs.
        It will then create a unique job-id, launch that job in the cluster,
        and store relevent info in the current_jobs map so we can track the job's
        status

        :return:

        """
        self.logger.info('k8s: job is {}'.format(str(next_job)))
        key, command = next_job
        dag_id, task_id, execution_date = key
        self.logger.info("k8s: running for command {}".format(command))
        self.logger.info("k8s: launching image {}".format(self.kube_config.kube_image))
        try:
            pod = self.pod_maker.make_pod(
                namespace=self.namespace, pod_id=self._create_pod_id(dag_id, task_id),
                dag_id=dag_id, task_id=task_id, execution_date=self._datetime_to_label_safe_datestring(execution_date),
                airflow_command=command
            )
            # the watcher will monitor pods, so we do not block.
            self.launcher.run_pod_async(pod)
        except Exception:
            self.logger.exception('An error occurred!')
        self.logger.info("k8s: Job created!")

    def delete_pod(self, pod_id):
        if self.kube_config.delete_worker_pods:
            try:
                self.api.delete_namespaced_pod(pod_id, self.namespace, body=client.V1DeleteOptions())
            except ApiException as e:
                if e.status != 404:
                    raise

    def sync(self):
        """
        The sync function checks the status of all currently running kubernetes jobs.
        If a job is completed, it's status is placed in the result queue to
        be sent back to the scheduler.
        :return:
        """
        self._health_check_kube_watcher()
        while not self.watcher_queue.empty():
            self.process_watcher_task()

    def process_watcher_task(self):
        pod_id, state, labels = self.watcher_queue.get()
        logging.info("Attempting to finish pod; pod_id: {}; state: {}; labels: {}".format(pod_id, state, labels))
        key = self._labels_to_key(labels)
        if key:
            self.logger.info("finishing job {}".format(key))
            self.result_queue.put((key, state, pod_id))

    @staticmethod
    def _strip_unsafe_kubernetes_special_chars(string):
        """
        Kubernetes only supports lowercase alphanumeric characters and "-" and "." in the pod name
        However, there are special rules about how "-" and "." can be used so let's only keep alphanumeric chars
        see here for detail: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/
        :param string:
        :return:
        """
        return ''.join(ch.lower() for ind, ch in enumerate(string) if ch.isalnum())

    @staticmethod
    def _make_safe_pod_id(safe_dag_id, safe_task_id, safe_uuid):
        """
        Kubernetes pod names must be <= 253 chars and must pass the following regex for validation
        "^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$"
        :param safe_dag_id: a dag_id with only alphanumeric characters
        :param safe_task_id: a task_id with only alphanumeric characters
        :param random_uuid: a uuid
        :return:
        """
        MAX_POD_ID_LEN = 253

        safe_key = safe_dag_id + safe_task_id

        safe_pod_id = safe_key[:MAX_POD_ID_LEN-len(safe_uuid)-1] + "-" + safe_uuid

        return safe_pod_id

    @staticmethod
    def _create_pod_id(dag_id, task_id):
        safe_dag_id = AirflowKubernetesScheduler._strip_unsafe_kubernetes_special_chars(dag_id)
        safe_task_id = AirflowKubernetesScheduler._strip_unsafe_kubernetes_special_chars(task_id)
        safe_uuid = AirflowKubernetesScheduler._strip_unsafe_kubernetes_special_chars(uuid4().hex)
        return AirflowKubernetesScheduler._make_safe_pod_id(safe_dag_id, safe_task_id, safe_uuid)

    @staticmethod
    def _label_safe_datestring_to_datetime(string):
        """
        Kubernetes doesn't like ":" in labels, since ISO datetime format uses ":" but not "_" let's replace ":" with "_"
        :param string: string
        :return: datetime.datetime object
        """
        return parser.parse(string.replace("_", ":"))

    @staticmethod
    def _datetime_to_label_safe_datestring(datetime_obj):
        """
        Kubernetes doesn't like ":" in labels, since ISO datetime format uses ":" but not "_" let's replace ":" with "_"
        :param datetime_obj: datetime.datetime object
        :return: ISO-like string representing the datetime
        """
        return datetime_obj.isoformat().replace(":", "_")

    def _labels_to_key(self, labels):
        try:
            return labels["dag_id"], labels["task_id"], self._label_safe_datestring_to_datetime(labels["execution_date"])
        except Exception as e:
            self.logger.warn("Error while converting labels to key; labels: {}; exception: {}".format(
                labels, e
            ))
            return None


class KubernetesExecutor(BaseExecutor):
    def __init__(self):
        self.kube_config = KubeConfig()
        self.task_queue = None
        self._session = None
        self.result_queue = None
        self.kub_client = None
        super(KubernetesExecutor, self).__init__(parallelism=self.kube_config.parallelism)

    def clear_queued(self):
        """
        If the airflow scheduler restarts with pending "Queued" tasks those queued tasks will never be scheduled
        Thus, on starting up the scheduler let's set all the queued tasks state to None
        There is a possibility two tasks will be launched like this:
        one task is launched before scheduler crashes, then the scheduler restarts and its state is set to None,
        then another task is launched, then both tasks start up
        however only one of the tasks should be allowed to actually run due to guards in `TI.run` method
        :return: None
        """
        self._session.query(TaskInstance).filter(TaskInstance.state == State.QUEUED).update({
            TaskInstance.state: State.NONE
        })
        self._session.commit()

    def start(self):
        self.logger.info('k8s: starting kubernetes executor')
        self._session = settings.Session()
        self.task_queue = Queue()
        self.result_queue = Queue()
        self.kub_client = AirflowKubernetesScheduler(self.kube_config, self.task_queue, self.result_queue)
        self.clear_queued()

    def execute_async(self, key, command, queue=None):
        self.logger.info("k8s: adding task {} with command {}".format(key, command))
        self.task_queue.put((key, command))

    def sync(self):
        self.kub_client.sync()
        while not self.result_queue.empty():
            results = self.result_queue.get()
            self.logger.info("reporting {}".format(results))
            self._change_state(*results)

        if not self.task_queue.empty():
            key, command = self.task_queue.get()
            self.kub_client.run_next((key, command))

    def _change_state(self, key, state, pod_id):
        self.logger.info("k8s: setting state of {} to {}".format(key, state))
        if state != State.RUNNING:
            self.kub_client.delete_pod(pod_id)
            try:
                self.running.pop(key)
            except KeyError:
                pass
        self.event_buffer[key] = state
        (dag_id, task_id, ex_time) = key
        item = self._session.query(TaskInstance).filter_by(
            dag_id=dag_id,
            task_id=task_id,
            execution_date=ex_time
        ).one()

        if item.state == State.RUNNING or item.state == State.QUEUED:
            item.state = state
            self._session.add(item)
            self._session.commit()

    def end(self):
        self.logger.info('ending kube executor')
        self.task_queue.join()

    def terminate(self):
        pass
