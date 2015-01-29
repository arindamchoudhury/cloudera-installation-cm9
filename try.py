import ConfigParser
import socket
import sys
from time import sleep
from cm_api.api_client import ApiResource
from cm_api.endpoints.services import ApiService
from cm_api.endpoints.services import ApiServiceSetupInfo

config = ConfigParser.ConfigParser()
config.read("clouderaconfig.ini")

cm_api_host = config.get("CM", "cm.api.host")
cm_api_version = config.get("CM", "cm.api.version")
cm_api_user = config.get("CM", "admin.api.name")
cm_api_pass = config.get("CM", "admin.api.password")

cm_management_host = config.get("MANAGEMENT", "mgmt.host")

host_username = config.get("HOST", "host.username")
host_password = config.get("HOST", "host.password")

cluster_name = config.get("CDH", "cluster.name")
cluster_hosts = config.get("CDH", "cluster.hosts").split(',')
cdh_version = config.get("CDH", "cdh.version")
parcel_version = config.get("CDH", "parcel.version")

management_service_name = config.get("MANAGEMENT", "service.name")
smon_role_name = config.get("MANAGEMENT", "servicemonitor.name")
hmon_role_name = config.get("MANAGEMENT", "hostmonitor.name")
eserv_role_name = config.get("MANAGEMENT", "eventserver.name")
apub_role_name = config.get("MANAGEMENT", "alertpublisher.name")
amon_role_name = config.get("MANAGEMENT", "activitymonitor.name")

yarn_servicename = config.get("YARN", "yarn.servicename")
yarn_resourcemanager_servicename = config.get("YARN", "yarn.resourcemanager.servicename")
yarn_jobhistory_servicename = config.get("YARN", "yarn.jobhistory.servicename")
yarn_nodemanager_servicename = config.get("YARN", "yarn.nodemanager.servicename")
yarn_resourcemanager = config.get("YARN", "yarn.resourcemanager")
yarn_jobhistory = config.get("YARN", "yarn.jobhistory")
yarn_nodemanager = config.get("YARN", "yarn.nodemanager").split(',')

hdfs_servicename = config.get("HDFS", "hdfs.servicename")
hdfs_namenode_servicename = config.get("HDFS", "hdfs.namenode.servicename")
hdfs_secondarynamenode_servicename = config.get("HDFS", "hdfs.secondarynamenode.servicename")
hdfs_balancer_servicename = config.get("HDFS", "hdfs.balancer.servicename")
hdfs_datanode_servicename = config.get("HDFS", "hdfs.datanode.servicename")
hdfs_namenode = config.get("HDFS", "hdfs.namenode")
hdfs_secondarynamenode = config.get("HDFS", "hdfs.secondarynamenode")
hdfs_balancer = config.get("HDFS", "hdfs.balancer")
hdfs_datanode = config.get("HDFS", "hdfs.datanode").split(',')

zookeeper_servicename = config.get("ZOOKEEPER", "zookeeper.servicename")
zookeeper_server_servicename = config.get("ZOOKEEPER", "zookeeper.server.servicename")
zookeeper_server = config.get("ZOOKEEPER", "zookeeper.server").split(',')

hive_servicename = config.get("HIVE", "hive.servicename")
hive_gateway_servicename = config.get("HIVE", "hive.gateway.servicename")         
hive_hivemetastore_servicename = config.get("HIVE", "hive.hivemetastore.servicename")
hive_hiveserver2_servicename = config.get("HIVE", "hive.hiveserver2.servicename")
hive_gateway = config.get("HIVE", "hive.gateway")
hive_metastore = config.get("HIVE", "hive.metastore")
hive_hiveserver2 = config.get("HIVE", "hive.hiveserver2")

api = ApiResource(cm_api_host, version=cm_api_version, username=cm_api_user, password=cm_api_pass)

manager = api.get_cloudera_manager()

cmd = manager.host_install(host_username,cluster_hosts, password=host_password)
print "checking if host_install finished"    
while cmd.active == True:
    sleep(5)
    print " ..."
    cmd = cmd.fetch()
    
if cmd.success != True:
    print "host_install failed: " + cmd.resultMessage
    exit(0)
print "host_install successful: " + cmd.resultMessage

cluster = api.create_cluster(cluster_name, cdh_version)

all_hosts = api.get_all_hosts()

hostrefs = []
yarn_nodemanager_hostrefs = []
hdfs_datanode_hostrefs = []
zookeeper_server_hostrefs=[]
hive_gateway_hostrefs = []

for host in all_hosts:
    if host.hostname == cm_management_host:
        cm_management_host_hostref = host.hostId
    if host.hostname == yarn_resourcemanager:
        yarn_resourcemanager_hostref = host.hostId
    if host.hostname == yarn_jobhistory:
        yarn_jobhistory_hostref = host.hostId
    if host.hostname in yarn_nodemanager:
        yarn_nodemanager_hostrefs.append(host.hostId)
    if host.hostname == hdfs_namenode:
        hdfs_namenode_hostref = host.hostId
    if host.hostname == hdfs_secondarynamenode:
        hdfs_secondarynamenode_hostref = host.hostId
    if host.hostname == hdfs_balancer:
        hdfs_balancer_hostref = host.hostId
    if host.hostname in hdfs_datanode:
        hdfs_datanode_hostrefs.append(host.hostId)
    if host.hostname in zookeeper_server:
        zookeeper_server_hostrefs.append(host.hostId)
    if host.hostname in hive_gateway:
       hive_gateway_hostrefs.append(host.hostId)
    if host.hostname == hive_metastore:
        hive_metastore_hostref = host.hostId
    if host.hostname == hive_hiveserver2:
        hive_hiveserver2_hostref = host.hostId
    hostrefs.append(host.hostId)

cluster.add_hosts(hostrefs)


parcel = cluster.get_parcel('CDH', parcel_version)

parcel.start_download()

while True:
    parcel = cluster.get_parcel('CDH', parcel_version)
    if parcel.stage == 'DOWNLOADED':
        break
    if parcel.state.errors:
        raise Exception(str(parcel.state.errors))
    print "progress: %s / %s" % (parcel.state.progress, parcel.state.totalProgress)
    sleep(5)

print "downloaded CDH parcel version %s on cluster %s" % (parcel_version, cluster_name)

parcel.start_distribution()

while True:
  parcel = cluster.get_parcel('CDH', parcel_version)
  if parcel.stage == 'DISTRIBUTED':
    break
  if parcel.state.errors:
    raise Exception(str(parcel.state.errors))
  print "progress: %s / %s" % (parcel.state.progress, parcel.state.totalProgress)
  sleep(5)

print "distributed CDH parcel version %s on cluster %s" % (parcel_version, cluster_name)

parcel.activate()

mgmt = manager.create_mgmt_service(ApiServiceSetupInfo(name = management_service_name, type = "MGMT"))

mgmt.create_role(smon_role_name + "-1", "SERVICEMONITOR",cm_management_host_hostref)
mgmt.create_role(hmon_role_name + "-1", "HOSTMONITOR", cm_management_host_hostref)
mgmt.create_role(eserv_role_name + "-1", "EVENTSERVER", cm_management_host_hostref)
mgmt.create_role(apub_role_name + "-1", "ALERTPUBLISHER", cm_management_host_hostref)

mgmt.start().wait()

print "Deployed and started Cloudera Management Services"

print "Inspecting hosts. This might take a few minutes."

cmd = manager.inspect_hosts()

while cmd.active == True:
    cmd = cmd.fetch()
    sleep(5)
    
if cmd.success != True:
    print " failed: inspect_hosts" + cmd.resultMessage
    exit(0)
    
print "inspect_hostsl successful: " + cmd.resultMessage


print "Deploying ZooKeeper"

zookeeper_service = cluster.create_service(zookeeper_servicename, "ZOOKEEPER")

i = 1
for hostref in zookeeper_server_hostrefs:
    zookeeper_service.create_role(zookeeper_server_servicename + "-" + str(i),"SERVER",hostref)
    i = i+1

print "configuraring ZooKeeper"

zookeeper_config = {
    'zookeeper_datadir_autocreate': 'true'
    }

zookeeper_server_config = {
    'dataLogDir':'/var/lib/zookeeper',
    'dataDir':'/var/lib/zookeeper',
    'zookeeper_server_java_heapsize':'52428800'
    }

zookeeper_server_groups = []
for group in zookeeper_service.get_all_role_config_groups():
    if group.roleType == 'SERVER':
        zookeeper_server_groups.append(group)

zookeeper_service.update_config(zookeeper_config)
for group in zookeeper_server_groups:
    group.update_config(zookeeper_server_config)
    
print "Initiating First Run command after ZooKeeper"

cmd = cluster.first_run()

while cmd.active == True:
    sleep(5)
    print " ..."
    cmd = cmd.fetch()
    
if cmd.success != True:
    print "First run failed after ZooKeeper: " + cmd.resultMessage
    exit(0)
print "First run successful after ZooKeeper: " + cmd.resultMessage

print "Restarting cluster"
cluster.stop().wait()
cluster.start().wait()

print "Deploying HDFS"

hdfs_service = cluster.create_service(hdfs_servicename, "HDFS")

hdfs_service.create_role(hdfs_namenode_servicename + "-1","NAMENODE",hdfs_namenode_hostref)
hdfs_service.create_role(hdfs_secondarynamenode_servicename + "-1","SECONDARYNAMENODE",hdfs_secondarynamenode_hostref)
hdfs_service.create_role(hdfs_balancer_servicename + "-1","BALANCER",hdfs_balancer_hostref)

i = 1
for hostref in hdfs_datanode_hostrefs:
    hdfs_service.create_role(hdfs_datanode_servicename + "-" + str(i),"DATANODE",hostref)
    i = i+1

print "configuraring HDFS"

namenode_groups = []
secondarynamenode_groups = []
datanode_groups = []
hdfs_gateway_groups = []

for group in hdfs_service.get_all_role_config_groups():
    if group.roleType == 'NAMENODE':
        namenode_groups.append(group)
    if group.roleType == 'SECONDARYNAMENODE':
        secondarynamenode_groups.append(group)
    if group.roleType == 'DATANODE':
        datanode_groups.append(group)
    if group.roleType == 'GATEWAY':
        hdfs_gateway_groups.append(group)

namenode_config = {
    'dfs_name_dir_list':'/dfs/nn'
    }
datanode_config = {
    'dfs_data_dir_list':'/dfs/dn',
    'dfs_datanode_failed_volumes_tolerated': 0
    }
secondarynamenode_config = {
    'fs_checkpoint_dir_list': '/dfs/snn'
    }
hdfs_gateway_config = {
    'dfs_client_use_trash':'true'
    }

for group in namenode_groups:
    group.update_config(namenode_config)
for group in secondarynamenode_groups:
    group.update_config(secondarynamenode_config)
for group in datanode_groups:
    group.update_config(datanode_config)
for group in hdfs_gateway_groups:
    group.update_config(hdfs_gateway_config)

print "Deploying YARN"

yarn_service = cluster.create_service(yarn_servicename, "YARN")

yarn_service.create_role(yarn_resourcemanager_servicename + "-1","RESOURCEMANAGER",yarn_resourcemanager_hostref)
yarn_service.create_role(yarn_jobhistory_servicename + "-1","JOBHISTORY",yarn_jobhistory_hostref)

i = 1
for hostref in yarn_nodemanager_hostrefs:
    yarn_service.create_role(yarn_nodemanager_servicename + "-" + str(i),"NODEMANAGER",hostref)
    i = i+1
    
print "configuraring YARN"

nodemanager_groups = []
yarn_gateway_groups = []

for group in yarn_service.get_all_role_config_groups():
    if group.roleType == 'NODEMANAGER':
        nodemanager_groups.append(group)
    if group.roleType == 'GATEWAY':
        yarn_gateway_groups.append(group)

yarn_config = {
    'hdfs_service': hdfs_servicename
    }
nodemanager_config = {
    'yarn_nodemanager_local_dirs':'/yarn/nm'
    }
yarn_gateway_config = {
    'mapred_submit_replication':len(hdfs_datanode_hostrefs)
    }

yarn_service.update_config(yarn_config)
for group in nodemanager_groups:
    group.update_config(nodemanager_config)
for group in yarn_gateway_groups:
    group.update_config(yarn_gateway_config)
    
print "Initiating First Run command after YARN and HDFS"

cmd = cluster.first_run()

while cmd.active == True:
    sleep(5)
    print " ..."
    cmd = cmd.fetch()
    
if cmd.success != True:
    print "First run failed after YARN and HDFS: " + cmd.resultMessage
    exit(0)
print "First run successful after YARN and HDFS: " + cmd.resultMessage

print "Restarting cluster"
cluster.stop().wait()
cluster.start().wait()

print "Deploying Hive"

hive_service = cluster.create_service(hive_servicename, "HIVE")

i = 1
for hostref in hive_gateway_hostrefs:
    hive_service.create_role(hive_gateway_servicename + "-" + str(i),"GATEWAY",hostref)
    i = i+1

hive_service.create_role(hive_hivemetastore_servicename + "-1","HIVEMETASTORE",hive_metastore_hostref)
hive_service.create_role(hive_hiveserver2_servicename + "-1","HIVESERVER2",hive_hiveserver2_hostref)

    
print "configuraring HIVE"

hive_config = {
    'hive_metastore_database_host': cm_management_host,
    'hive_metastore_database_name': 'hive',
    'hive_metastore_database_password': 'suTOXS7HzV',
    'hive_metastore_database_port': 7432,
    'hive_metastore_database_type': 'postgresql',
    'mapreduce_yarn_service': yarn_servicename,
    'zookeeper_service': zookeeper_servicename
    }
hiveserver2_config = {
    'hiveserver2_java_heapsize':'52428800'
    }
hivemetastore_config = {
    'hive_metastore_java_heapsize':'52428800'
    }
    
hive_service.update_config(hive_config)

hiveserver2_groups = []
hivemetastore_groups = []
for group in hive_service.get_all_role_config_groups():
    if group.roleType == 'HIVESERVER2':
        hiveserver2_groups.append(group)    
    if group.roleType == 'HIVEMETASTORE':
        hivemetastore_groups.append(group)
        
for group in hiveserver2_groups:
    group.update_config(hiveserver2_config)
for group in hivemetastore_groups:
    group.update_config(hivemetastore_config)

print "Initiating First Run command after HIVE"

cmd = cluster.first_run()

while cmd.active == True:
    sleep(5)
    print " ..."
    cmd = cmd.fetch()
    
if cmd.success != True:
    print "First run failed after HIVE: " + cmd.resultMessage
    exit(0)
print "First run successful after HIVE: " + cmd.resultMessage

print "Restarting cluster"
cluster.stop().wait()
cluster.start().wait()

print "Cloudera CDH installation in done"
