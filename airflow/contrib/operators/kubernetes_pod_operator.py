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

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.kubernetes import kube_client, pod_generator, pod_launcher
from airflow.utils.state import State


template_fields = ('templates_dict',)
template_ext = tuple()
ui_color = '#ffefeb'


class KubernetesPodOperator(BaseOperator):
    def execute(self, context):
        raise AirflowException("trying to except")
        try:

            client = kube_client.get_kube_client(in_cluster=False)
            gen = pod_generator.PodGenerator()

            pod = gen.make_pod(namespace=self.namespace,
                               pod_id=self.name,
                               cmds=self.cmds,
                               arguments=self.arguments,
                               labels=self.labels,
                               )

            launcher = pod_launcher.PodLauncher(client)
            final_state = launcher.run_pod(pod)
            if final_state != State.SUCCESS:
                raise AirflowException("Pod returned a failure")

        except Exception as ex:
            raise AirflowException("Pod Launching failed")

    @apply_defaults
    def __init__(self,
                 namespace,
                 cmds,
                 arguments,
                 labels,
                 name,
                 task_id,
                 *args,
                 **kwargs):
        super(KubernetesPodOperator, self).__init__(task_id, *args, **kwargs)
        self.namespace = namespace,
        self.cmds = cmds
        self.arguments = arguments
        self.labels = labels
        self.name = name
