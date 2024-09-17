import logging
from abc import ABC
import boto3

class Ec2_instance(ABC):

    def __init__(self, autoscalinggroup_name, region='us-east-1'):
        self.ec2_conn = boto3.resource('ec2', region_name=region)
        self.autoscalinggroup_name = autoscalinggroup_name
        self.region = region
        self.instances = []

    def get_ipaddress(self, instanceid):
        try:
            logging.debug("Getting Ip Address")
            instances = self.ec2_conn.instances.filter(InstanceIds=[instanceid])
            for instance in instances:
                return instance.private_ip_address
        except Exception as e:
            logging.error("Exception raised while getting ip address - {0} ".format(e))
            return None

    def get_node_info(self, nodenum):
        try:
            logging.debug("Getting IP Address of node {0}".format(nodenum))
            if nodenum > len(self.instances):
                raise Exception('Node number is out of range.')
            return {'id': self.instances[nodenum - 1].id, 'ip': self.instances[nodenum - 1].private_ip_address}
        except Exception as e:
            logging.error("Exception raised while getting information on {0} node - {1}".format(nodenum, e))
            return None

    def get_all_nodes(self):
        try:
            logging.debug("Get all AutoScaling group nodes")
            running_state_filter = {'Name': 'tag:region', 'Values': [self.region]}
            asg_filter = {'Name': 'tag:aws:autoscaling:groupName', 'Values': [self.autoscalinggroup_name]}
            instances = self.ec2_conn.instances.filter(Filters=[asg_filter, running_state_filter])
            sorted_instances = sorted(instances, key=lambda instance: (instance.launch_time, instance.id))
            self.instances = sorted_instances
            return sorted_instances
        except Exception as e:
            logging.error(e)
            return None

    def get_all_nodes_ip(self):
        try:
            logging.debug("Getting Node ID and IP Address of all nodes ")
            instances_info = []
            for instance in self.instances:
                instances_info.append(instance.private_ip_address)
            return instances_info
        except Exception as e:
            logging.error(e)

        return

    def resolve_host_to_ip_address(self) -> [list]:
        self.get_all_nodes()
        nodeips = self.get_all_nodes_ip()
        return nodeips
