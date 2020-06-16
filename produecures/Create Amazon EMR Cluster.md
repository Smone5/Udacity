# Create Amazon EMR with Amazon Key

The steps below are for Linux/Mac and Google Chrome

1. Create S3 Storage buckets for:
    + Bootstraps
    + EMR logs

2. Create EC2 Key Pair
	+ https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html#having-ec2-create-your-key-pair
	+ Use mkdir .ssh if not done already to create hidden directory for keys
	+ CD into .ssh directory
	+ Enter:
		+ aws ec2 create-key-pair --key-name spark-cluster2 --query 'KeyMaterial' --output text > spark-cluster2.pem 
	+ chmod 400 spark-cluster2.pem
	+ aws ec2 describe-key-pairs --key-name spark-cluster2
    + **Note:** Checks to see if key pair is found on AWS


3. Create the cluster by running the code below in terminal:

        aws emr create-cluster --name spark-cluster2 --release-label emr-5.9.0 \ 
        --applications Name=Spark Name=Zeppelin \ 
        --bootstrap-actions Path=s3://boostrap-bucketam521/bootstrap_emr.sh \ 
        --ec2-attributes KeyName=spark-cluster2 \ 
        --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m4.large InstanceGroupType=CORE,InstanceCount=2,InstanceType=m4.large \ 
        --log-uri s3://emrlogs-am521 

*Notes:
  + --bootstrap-actions Path: path to S3 bootstrap bucket and file created above
  + --log-uri: path to EMR logs bucket created above
  + --auto-terminate is removed at the end of CLI commands for now. You probably want that at end in the future to prevent the cluster from using $$$. 
  
 4. Open the cluster by clicking on the cluster name in "Cluster" section of Amazon EMR.
 5. In the "Security and access section", click on the link to "Security groups for Master".
 6. Add rule for inbound "SSH" access into the master node if not done so already. Make sure the source is set to your IP address and not open to the public.
 7. Jump back to the EMR main page
 8. Click again on the cluster name
 9. Wait till the Master and Core are at least created or in the bootstraping process to do the next steps. In the future you will not need to wait.
 10. These next steps, can be followed by using the AWS tutorials the section "Application user interfaces" and clicking on the link "Enable an SSH Connection". Open an SSH Tunnel to Amazon EMR Master Node:
 
 			ssh -i ~/.ssh/spark-cluster2.pem -ND 8157 hadoop@ec2-54-190-127-231.us-west-2.compute.amazonaws.com
			
*Notes: 
+ Make sure to put the .ssh folder in the path to the pem key created above.
+ Replace everything after "hadoop@" with your Master public DNS information found in the "Summary" tab of your Spark cluster. 
11. Install "Standard" version FoxyProxy on Chrome [foxyproxy link](http://foxyproxy.mozdev.org/downloads.html)
12. Restart Chrome after installing FoxyProxy
13. Save the following XML information to file called *foxyproxy-settings.xml*:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<foxyproxy>
    <proxies>
        <proxy name="emr-socks-proxy" id="2322596116" notes="" fromSubscription="false" enabled="true" mode="manual" selectedTabIndex="2" lastresort="false" animatedIcons="true" includeInCycle="true" color="#0055E5" proxyDNS="true" noInternalIPs="false" autoconfMode="pac" clearCacheBeforeUse="false" disableCache="false" clearCookiesBeforeUse="false" rejectCookies="false">
            <matches>
                <match enabled="true" name="*ec2*.amazonaws.com*" pattern="*ec2*.amazonaws.com*" isRegEx="false" isBlackList="false" isMultiLine="false" caseSensitive="false" fromSubscription="false" />
                <match enabled="true" name="*ec2*.compute*" pattern="*ec2*.compute*" isRegEx="false" isBlackList="false" isMultiLine="false" caseSensitive="false" fromSubscription="false" />
                <match enabled="true" name="10.*" pattern="http://10.*" isRegEx="false" isBlackList="false" isMultiLine="false" caseSensitive="false" fromSubscription="false" />
                <match enabled="true" name="*10*.amazonaws.com*" pattern="*10*.amazonaws.com*" isRegEx="false" isBlackList="false" isMultiLine="false" caseSensitive="false" fromSubscription="false" />
                <match enabled="true" name="*10*.compute*" pattern="*10*.compute*" isRegEx="false" isBlackList="false" isMultiLine="false" caseSensitive="false" fromSubscription="false" />
                <match enabled="true" name="*.compute.internal*" pattern="*.compute.internal*" isRegEx="false" isBlackList="false" isMultiLine="false" caseSensitive="false" fromSubscription="false" />
                <match enabled="true" name="*.ec2.internal*" pattern="*.ec2.internal*" isRegEx="false" isBlackList="false" isMultiLine="false" caseSensitive="false" fromSubscription="false" />
            </matches>
            <manualconf host="localhost" port="8157" socksversion="5" isSocks="true" username="" password="" domain="" />
        </proxy>
    </proxies>
</foxyproxy>
```
15. Click the FoxyProxy Icon in Chrome
16. Select Options
17. Click Import/Export
18. Choose file "foxyproxy-settings.xml" and click Open
19. Click Add
20. At top of the page for proxy mode, choose "Use proxies based on their pre-defined patterns and priorities"
21. Open up your cluster
22. In the summary tab, in the section "Application user interfaces" you should see links to Zeppelin, Spark History Server and Resource Manager if the process worked correctly.
23. The next time you should be able to skip the FoxyProxy steps and just SSH in. 

