from aws_cdk import (
    core as cdk,
    aws_iam as _iam,
    aws_s3 as _s3,
    aws_kms as _kms
)


class ApplicationStack(cdk.Stack):
    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.job_flow_role = self._create_job_flow_role()
        self.service_role = self._create_service_role()
        self.data_encryption_key = self._create_data_encryption_key()
        self.data_bucket = self._create_encrypted_data_bucket()
        self.spark_app_bucket = self._create_spark_app_bucket()
        self.logs_bucket = self._create_logs_bucket()
        self._grant_read_write(self.job_flow_role)

    def _create_data_encryption_key(self):
        return _kms.Key(
            self, "DataEncryptionKey",
            alias="sid-emr-demo-key",
            description="KMS key for dataset data encryption",
            enable_key_rotation=False,
            removal_policy=cdk.RemovalPolicy.DESTROY
        )

    def _create_encrypted_data_bucket(self):
        data_bucket = _s3.Bucket(
            self, "DataBucket",
            bucket_name="sid-emr-test-data-bucket",
            encryption=_s3.BucketEncryption.KMS,
            encryption_key=self.data_encryption_key,
            block_public_access=_s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            object_ownership=_s3.ObjectOwnership.BUCKET_OWNER_PREFERRED,
            enforce_ssl=True,
            versioned=False
        )
        return data_bucket

    def _create_spark_app_bucket(self):
        spark_app_bucket = _s3.Bucket(
            self, "SparkApplicationBucket",
            bucket_name="sid-emr-spark-application",
            block_public_access=_s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            object_ownership=_s3.ObjectOwnership.BUCKET_OWNER_PREFERRED,
            enforce_ssl=True,
            versioned=False
        )
        return spark_app_bucket

    def _create_logs_bucket(self):
        logs_bucket = _s3.Bucket(
            self, "LogsBucket",
            bucket_name="sid-emr-logs",
            block_public_access=_s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            object_ownership=_s3.ObjectOwnership.BUCKET_OWNER_PREFERRED,
            enforce_ssl=True,
            versioned=False
        )
        return logs_bucket

    def _create_job_flow_role(self) -> _iam.Role:
        job_flow_role = _iam.Role(
            self, "JobFlowRole",
            role_name="sid-emr-job-flow-role",
            assumed_by=_iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[_iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonElasticMapReduceforEC2Role")]
        )
        return job_flow_role

    def _grant_read_write(self, principal: _iam.IPrincipal):
        self.data_bucket.add_to_resource_policy(
            _iam.PolicyStatement(
                effect=_iam.Effect.ALLOW,
                resources=[self.data_bucket.bucket_arn],
                principals=[principal],
                actions=["s3:List*"]
            )
        )
        self.data_bucket.add_to_resource_policy(
            _iam.PolicyStatement(
                effect=_iam.Effect.ALLOW,
                resources=[self.data_bucket.bucket_arn+'/*'],
                principals=[principal],
                actions=["s3:Get*", "s3:Put*", "s3:DeleteObject*", "s3:Abort*"]
            )
        )
        self.data_bucket.grant_read_write(self.job_flow_role)
        self.data_bucket.grant_put_acl(self.job_flow_role)

    def _create_service_role(self) -> _iam.Role:
        return _iam.Role(
            self, "ServiceRole",
            role_name="sid-emr-service-role",
            assumed_by=_iam.ServicePrincipal("elasticmapreduce.amazonaws.com"),
            managed_policies=[_iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonElasticMapReduceRole")]
        )
