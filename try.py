import ConfigParser
import socket
import sys
from time import sleep
from cm_api.api_client import ApiResource
from cm_api.endpoints.clusters import ApiCluster
from cm_api.endpoints.clusters import create_cluster
from cm_api.endpoints.parcels import ApiParcel
from cm_api.endpoints.parcels import get_parcel
from cm_api.endpoints.cms import ClouderaManager
from cm_api.endpoints.services import ApiService, ApiServiceSetupInfo
from cm_api.endpoints.services import create_service
from cm_api.endpoints.types import ApiCommand, ApiRoleConfigGroupRef
from cm_api.endpoints.role_config_groups import get_role_config_group
from cm_api.endpoints.role_config_groups import ApiRoleConfigGroup
from cm_api.endpoints.roles import ApiRole

config = ConfigParser.ConfigParser()
config.read("/root/python-script/clouderaconfig.ini")

cm_api_host = config.get("CM", "cm.api.host")
cm_api_version = config.get("CM", "cm.api.version")
cm_api_user = config.get("CM", "admin.api.name")
cm_api_pass = config.get("CM", "admin.api.password")

cm_management_host = config.get("MANAGEMENT", "mgmt.host")

host_username = config.get("HOST", "host.username")
private_key_file = config.get("HOST", "host.privatekey")

cluster_name = config.get("CDH", "cluster.name")
cluster_hosts = config.get("CDH", "cluster.hosts").split(',')
cdh_version = config.get("CDH", "cdh.version")
cdh_version_number = config.get("CDH", "parcel.version")

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

oozie_servicename = config.get("OOZIE", "oozie.servicename")
oozie_server_servicename = config.get("OOZIE", "oozie.server.servicename")
oozie_server = config.get("OOZIE", "oozie.server")
 
hue_servicename = config.get("HUE", "hue.servicename")
hue_server_servicename = config.get("HUE", "hue.server.servicename")
hue_server = config.get("HUE", "hue.server")

with open(private_key_file,'r') as f:
    private_key_contents = f.read()

api = ApiResource(cm_api_host, version=cm_api_version, username=cm_api_user, password=cm_api_pass)

manager = api.get_cloudera_manager()

cmd = manager.host_install(host_username,cluster_hosts, private_key=private_key_contents)
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
    if host.hostname == oozie_server:
        oozie_server_hostref = host.hostId
    if host.hostname == hue_server:
        hue_server_hostref = host.hostId
    hostrefs.append(host.hostId)

cluster.add_hosts(hostrefs)


parcels_list = []
# get and list all available parcels
print "Available parcels:"
for p in cluster.get_all_parcels():
    print '\t' + p.product + ' ' + p.version
    if p.version.startswith(cdh_version_number) and p.product == "CDH":
        parcels_list.append(p)
        
if len(parcels_list) == 0:
    print "No " + cdh_version + " parcel found!"
    exit(0)
    
cdh_parcel = parcels_list[0]
for p in parcels_list:
    if p.version > cdh_parcel.version:
        cdh_parcel = p

print cdh_parcel

cmd = cdh_parcel.start_download()

if cmd.success != True:
    print "Parcel download failed!"
    exit(0)

while cdh_parcel.stage != 'DOWNLOADED':
    sleep(5)
    cdh_parcel = get_parcel(api, cdh_parcel.product, cdh_parcel.version, cluster_name)
    
print cdh_parcel.product + ' ' + cdh_parcel.version + " downloaded"

# distribute the parcel
print "Starting parcel distribution. This might take a while."
cmd = cdh_parcel.start_distribution()
if cmd.success != True:
    print "Parcel distribution failed!"
    exit(0)

# make sure the distribution finishes
while cdh_parcel.stage != "DISTRIBUTED":
    sleep(5)
    cdh_parcel = get_parcel(api, cdh_parcel.product, cdh_parcel.version, cluster_name)
print cdh_parcel.product + ' ' + cdh_parcel.version + " distributed"

# activate the parcel
cmd = cdh_parcel.activate()
if cmd.success != True:
    print "Parcel activation failed!"
    exit(0)

# make sure the activation finishes
while cdh_parcel.stage != "ACTIVATED":
    cdh_parcel = get_parcel(api, cdh_parcel.product, cdh_parcel.version, cluster_name)
print cdh_parcel.product + ' ' + cdh_parcel.version + " activated"



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

print "Deploying OOZIE"

oozie_service = cluster.create_service(oozie_servicename, "OOZIE")
oozie_service.create_role(oozie_server_servicename,"OOZIE_SERVER",oozie_server_hostref)

print "configuring OOZIE"

oozie_config = {
    'hive_service':hive_servicename,
    'mapreduce_yarn_service':yarn_servicename,
    'zookeeper_service':zookeeper_servicename
    }
oozie_server_config = {
    'oozie_java_heapsize':'52428800',
    'oozie_data_dir':'/var/lib/oozie/data'
    }

oozieserver_groups = []
for group in oozie_service.get_all_role_config_groups():
    if group.roleType == 'OOZIE_SERVER':
        oozieserver_groups.append(group)

oozie_service.update_config(oozie_config)

for group in oozieserver_groups:
    group.update_config(oozie_server_config)

print "Initiating First Run command after OOZIE"

cmd = cluster.first_run()

while cmd.active == True:
    sleep(5)
    print " ..."
    cmd = cmd.fetch()
    
if cmd.success != True:
    print "First run failed after OOZIE: " + cmd.resultMessage
    exit(0)
print "First run successful after OOZIE: " + cmd.resultMessage

print "Restarting cluster"
cluster.stop().wait()
cluster.start().wait()   

print "Deploying HUE"

hue_service = cluster.create_service(hue_servicename, "HUE")
hue_service.create_role(hue_server_servicename,"HUE_SERVER",hue_server_hostref)

print "configuring HUE"

hue_config = {
    'hive_service':hive_servicename,
    'mapreduce_yarn_service':yarn_servicename,
    'zookeeper_service':zookeeper_servicename,
    'oozie_service':oozie_servicename,
    'hue_webhdfs':'namenode-1'
    }

hue_service.update_config(hue_config)

print "Initiating First Run command after HUE"

cmd = cluster.first_run()

while cmd.active == True:
    sleep(5)
    print " ..."
    cmd = cmd.fetch()
    
if cmd.success != True:
    print "First run failed after HUE: " + cmd.resultMessage
    exit(0)
print "First run successful after HUE: " + cmd.resultMessage

print "Restarting cluster"
cluster.stop().wait()
cluster.start().wait()
print "Cloudera CDH installation in done"
