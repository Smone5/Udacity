# Create Amazon EMR with Amazon Key

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
  + --bootstrap-actions Path=<path to S3 bootstrap bucked created above>
  + --log-uri  <path to EMR logs bucket created above>
  + --auto-terminate is removed at the end of CLI commands for now. You probably want that at end in the future to prevent the cluster from using $$$. 
