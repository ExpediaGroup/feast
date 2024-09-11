package utils

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"log"
	"sort"
	"strings"
)

type EC2Instance struct {
	EC2Conn              *ec2.EC2
	AutoScalingGroupName string
	Instances            []*ec2.Instance
	region               string
}

func NewEC2Instance(autoScalingGroupName string, region string) *EC2Instance {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
	}))

	svc := ec2.New(sess)

	return &EC2Instance{
		EC2Conn:              svc,
		AutoScalingGroupName: autoScalingGroupName,
		region:               region,
	}
}

func (e *EC2Instance) GetIPAddress(instanceID string) (*string, error) {
	input := &ec2.DescribeInstancesInput{
		InstanceIds: []*string{
			aws.String(instanceID),
		},
	}

	result, err := e.EC2Conn.DescribeInstances(input)
	if err != nil {
		log.Println("Error getting IP address:", err)
		return nil, err
	}

	for _, reservation := range result.Reservations {
		for _, instance := range reservation.Instances {
			return instance.PrivateIpAddress, nil
		}
	}

	return nil, nil
}

func (e *EC2Instance) GetNodeInfo(nodeNum int) (*ec2.Instance, error) {
	if nodeNum > len(e.Instances) {
		return nil, fmt.Errorf("node number is out of range")
	}

	return e.Instances[nodeNum-1], nil
}

func (e *EC2Instance) GetAllNodes(instanceName string) ([]*ec2.Instance, error) {
	input := &ec2.DescribeInstancesInput{
		Filters: []*ec2.Filter{
			{
				Name:   aws.String("tag:region"),
				Values: []*string{aws.String(e.region)},
			},
			{
				Name:   aws.String("tag:aws:autoscaling:groupName"),
				Values: []*string{aws.String(e.AutoScalingGroupName)},
			},
		},
	}

	result, err := e.EC2Conn.DescribeInstances(input)
	if err != nil {
		log.Println("Error getting all nodes:", err)
		return nil, err
	}

	var instances []*ec2.Instance
	for _, reservation := range result.Reservations {
		for _, instance := range reservation.Instances {
			instances = append(instances, instance)
		}
	}

	sort.Slice(instances, func(i, j int) bool {
		return strings.Compare(*instances[i].InstanceId, *instances[j].InstanceId) < 0
	})

	e.Instances = instances

	return instances, nil
}

func (e *EC2Instance) GetAllNodesIP() ([]string, error) {
	instancesInfo := make([]string, 0)
	for _, instance := range e.Instances {
		instancesInfo = append(instancesInfo, *instance.PrivateIpAddress)
	}

	return instancesInfo, nil
}

func (e *EC2Instance) ResolveHostNameToIp() ([]string, error) {

	e.GetAllNodes(e.AutoScalingGroupName)
	nodeIps, err := e.GetAllNodesIP()
	if err != nil {
		return nil, fmt.Errorf("Unable to get Node Ips")
	}
	return nodeIps, nil
}
